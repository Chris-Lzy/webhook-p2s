import base64
import hashlib
import hmac
import json
import logging
import os
import shutil
import socket
import ssl
import subprocess
import threading
import time
import urllib.parse
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monitor")

PORT = int(os.environ.get("PORT", "8787"))
DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent / "data")))
CONFIG_PATH = DATA_DIR / "config.json"
MEDIA_DIR = DATA_DIR / "media"
MAX_PRINTERS = 20

STATE_TEXT = {
    "IDLE": "空闲",
    "PREPARE": "准备中",
    "SLICING": "切片中",
    "RUNNING": "打印中",
    "PAUSED": "已暂停",
    "FINISH": "已完成",
    "SUCCEEDED": "已完成",
    "FAILED": "失败",
}

EVENT_TAGS = {
    "开始打印": "start",
    "打印完成": "finish",
    "打印失败": "fail",
    "打印已暂停": "pause",
    "打印已恢复": "resume",
}

DEFAULT_CONFIG = {
    "printers": [
        {"name": f"P2S_{i}", "host": "", "sn": "", "access_code": "", "enabled": False}
        for i in range(1, MAX_PRINTERS + 1)
    ],
    "dingtalk": {"webhook": "", "secret": "", "keyword": ""},
    "notify": {
        "enabled": True,
        "on_start": True,
        "on_finish": True,
        "on_failed": True,
        "on_pause": False,
        "on_resume": False,
        "on_offline": True,
        "progress_step": 25,
    },
    "camera": {"enabled": False, "snapshot": True, "clip": True, "clip_seconds": 5},
}

lock = threading.RLock()
config = json.loads(json.dumps(DEFAULT_CONFIG))
events = deque(maxlen=50)


def blank_status():
    return {
        "connected": False,
        "configured": False,
        "last_msg_ts": 0,
        "gcode_state": "",
        "percent": None,
        "remaining_time": None,
        "layer_num": None,
        "total_layer_num": None,
        "nozzle_temper": None,
        "bed_temper": None,
        "gcode_file": "",
        "subtask_name": "",
        "error_code": "",
        "print_start_ts": None,
    }


statuses = [blank_status() for _ in range(MAX_PRINTERS)]


def deep_merge(dst, src):
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = v


def load_config():
    global config
    if not CONFIG_PATH.exists():
        return
    try:
        saved = json.loads(CONFIG_PATH.read_text("utf-8"))
    except Exception as e:
        log.warning("读取配置失败: %s", e)
        return
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    for sec in ("dingtalk", "notify", "camera"):
        if isinstance(saved.get(sec), dict):
            deep_merge(cfg[sec], saved[sec])
    items = saved.get("printers")
    legacy = isinstance(saved.get("printer"), dict)
    if not isinstance(items, list) and legacy:
        items = [saved["printer"]]
    for i, item in enumerate(items or []):
        if i >= MAX_PRINTERS or not isinstance(item, dict):
            break
        p = cfg["printers"][i]
        for k in ("name", "host", "sn", "access_code"):
            if item.get(k) is not None:
                p[k] = str(item[k]).strip()
        if not p["name"]:
            p["name"] = f"P2S_{i + 1}"
        p["enabled"] = bool(item.get("enabled", legacy)) and bool(p["host"] and p["sn"] and p["access_code"])
    config = cfg


def save_config():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(CONFIG_PATH)


def pcfg(index):
    with lock:
        return dict(config["printers"][index])


def add_event(etype, text):
    with lock:
        events.appendleft({"ts": time.time(), "type": etype, "text": text})


def dingtalk_send(title, text, force=False):
    with lock:
        d = dict(config["dingtalk"])
        enabled = config["notify"]["enabled"]
    if not force and not enabled:
        return False, "通知总开关已停用"
    if not d["webhook"].strip():
        return False, "未配置钉钉 webhook"
    url = d["webhook"].strip()
    if d["secret"].strip():
        ts = str(round(time.time() * 1000))
        sign = base64.b64encode(
            hmac.new(d["secret"].encode(), f"{ts}\n{d['secret']}".encode(), hashlib.sha256).digest()
        ).decode()
        url += "&timestamp=" + ts + "&sign=" + urllib.parse.quote_plus(sign)
    kw = d["keyword"].strip()
    full_title = title if (not kw or kw in title) else kw + title
    body = {
        "msgtype": "markdown",
        "markdown": {"title": full_title, "text": f"### {full_title}\n\n{text}"},
    }
    try:
        r = requests.post(url, json=body, timeout=10)
        data = r.json()
        if data.get("errcode") == 0:
            return True, "推送成功"
        return False, f"钉钉返回错误: {data.get('errmsg', data)}"
    except Exception as e:
        return False, f"发送失败: {e}"


def notify(pname, title, text, tag="print", with_media=False, printer_index=None, force=False):
    def run():
        with lock:
            enabled = config["notify"]["enabled"]
            cam_enabled = config["camera"]["enabled"]
        full_title = f"{pname} {title}"
        if with_media and cam_enabled and not force and printer_index is not None:
            text = attach_media(printer_index, tag, text)
        ok, msg = dingtalk_send(full_title, text, force=force)
        if force or enabled:
            add_event("notify" if ok else "error", f"【{full_title}】{msg}")
        else:
            add_event("info", f"【{full_title}】通知已停用，未推送")

    threading.Thread(target=run, daemon=True).start()


def fmt_minutes(m):
    try:
        m = int(m)
    except (TypeError, ValueError):
        return ""
    if m <= 0:
        return ""
    return f"{m // 60}小时{m % 60}分钟" if m >= 60 else f"{m}分钟"


def rtsp_url(p):
    code = urllib.parse.quote(p["access_code"], safe="")
    return f"rtsp://bblp:{code}@{p['host']}:322/stream"


def lan_base_url(host):
    if not host:
        return ""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        s.connect((host, 8883))
        ip = s.getsockname()[0]
        s.close()
        return f"http://{ip}:{PORT}"
    except Exception:
        return ""


def run_ffmpeg(src, out_args, out_path, timeout):
    if not shutil.which("ffmpeg"):
        return False
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-rtsp_transport", "tcp", "-i", src, *out_args, str(out_path),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
        return r.returncode == 0 and Path(out_path).exists() and Path(out_path).stat().st_size > 0
    except Exception:
        return False


def grab_snapshot(index, tag):
    p = pcfg(index)
    if not (p["host"] and p["access_code"]):
        return None
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    jpg = MEDIA_DIR / f"p{index + 1}_{tag}_{int(time.time())}.jpg"
    return jpg if run_ffmpeg(rtsp_url(p), ["-frames:v", "1", "-q:v", "3"], jpg, timeout=12) else None


def grab_clip(index, tag, seconds):
    p = pcfg(index)
    if not (p["host"] and p["access_code"]):
        return None
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    sec = max(2, min(15, int(seconds or 5)))
    mp4 = MEDIA_DIR / f"p{index + 1}_{tag}_{int(time.time())}.mp4"
    ok = run_ffmpeg(
        rtsp_url(p),
        ["-t", str(sec), "-vf", "scale=1280:-2", "-c:v", "libx264", "-preset", "veryfast",
         "-crf", "26", "-pix_fmt", "yuv420p", "-an", "-movflags", "+faststart"],
        mp4, timeout=sec * 6 + 20,
    )
    return mp4 if ok else None


def prune_media(keep=100):
    try:
        files = sorted(MEDIA_DIR.glob("*"), key=lambda f: f.stat().st_mtime, reverse=True)
        for f in files[keep:]:
            try:
                f.unlink()
            except OSError:
                pass
    except OSError:
        pass


def capture_media(index, tag):
    with lock:
        cam = dict(config["camera"])
    if not cam["enabled"]:
        return None, None
    jpg = grab_snapshot(index, tag) if cam["snapshot"] else None
    mp4 = grab_clip(index, tag, cam["clip_seconds"]) if cam["clip"] else None
    if jpg or mp4:
        prune_media()
    return jpg, mp4


def attach_media(index, tag, text):
    jpg, mp4 = capture_media(index, tag)
    if not jpg and not mp4:
        add_event("info", f"{pcfg(index)['name']} 摄像头抓取失败，仅推送文字")
        return text
    base = lan_base_url(pcfg(index)["host"])
    if not base:
        add_event("info", f"{pcfg(index)['name']} 无法确定 NAS 局域网地址，画面未附带")
        return text
    parts = [text]
    if jpg:
        parts.append(f"![监控画面]({base}/media/{jpg.name})")
    if mp4:
        parts.append(f"[查看视频]({base}/media/{mp4.name})")
    return "\n\n".join(parts)


class PrinterMonitor:
    def __init__(self, index):
        self.index = index
        self.client = None
        self.last_bucket = 0
        self.stopping = False

    def start(self):
        self.last_bucket = 0
        self.stopping = False
        p = pcfg(self.index)
        if not (p["host"] and p["sn"] and p["access_code"]):
            with lock:
                statuses[self.index]["configured"] = False
                statuses[self.index]["connected"] = False
            return
        statuses[self.index]["configured"] = True
        c = mqtt.Client()
        c.tls_set(cert_reqs=ssl.CERT_NONE)
        c.tls_insecure_set(True)
        c.username_pw_set("bblp", p["access_code"])
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        c.reconnect_delay_set(2, 30)
        self.client = c
        try:
            c.connect(p["host"], 8883, keepalive=30)
        except Exception as e:
            add_event("error", f"{p['name']} 连接失败: {e}")
            return
        c.loop_start()

    def stop(self):
        self.stopping = True
        if self.client:
            try:
                self.client.disconnect()
                self.client.loop_stop()
            except Exception:
                pass
            self.client = None
        with lock:
            statuses[self.index]["connected"] = False

    def restart(self):
        self.stop()
        time.sleep(0.3)
        self.start()

    def _on_connect(self, client, userdata, flags, rc):
        p = pcfg(self.index)
        with lock:
            statuses[self.index]["connected"] = True
        add_event("info", f"{p['name']} 已连接")
        client.subscribe(f"device/{p['sn']}/report")
        client.publish(
            f"device/{p['sn']}/request",
            json.dumps({"pushing": {"sequence_id": "1", "command": "pushall"}}),
        )

    def _on_disconnect(self, client, userdata, rc):
        if self.stopping or client is not self.client:
            with lock:
                statuses[self.index]["connected"] = False
            return
        p = pcfg(self.index)
        with lock:
            s = statuses[self.index]
            printing = s["gcode_state"] in ("RUNNING", "PAUSED", "PREPARE", "SLICING")
            do_notify = config["notify"]["enabled"] and config["notify"]["on_offline"]
            s["connected"] = False
        add_event("error", f"{p['name']} 连接断开，将自动重连")
        if printing and do_notify:
            notify(p["name"], "打印机离线", "MQTT 连接断开，打印可能中断，服务将自动重连。",
                   tag="offline", printer_index=self.index)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8", "ignore"))
        except Exception:
            return
        try:
            self._handle(data)
        except Exception as e:
            log.warning("%s 处理消息出错: %s", pcfg(self.index)["name"], e)

    def _handle(self, data):
        p = data.get("print") or {}
        if not isinstance(p, dict):
            return
        gs = p.get("gcode_state") or ""
        n = config["notify"]
        s = statuses[self.index]
        with lock:
            prev = s["gcode_state"]
            s["last_msg_ts"] = time.time()
            if gs:
                s["gcode_state"] = gs
            for key in ("percent", "remaining_time", "layer_num", "total_layer_num"):
                if p.get(key) is not None:
                    try:
                        s[key] = int(p[key])
                    except (TypeError, ValueError):
                        pass
            for key in ("nozzle_temper", "bed_temper"):
                if p.get(key) is not None:
                    try:
                        s[key] = round(float(p[key]), 1)
                    except (TypeError, ValueError):
                        pass
            if p.get("gcode_file"):
                s["gcode_file"] = p["gcode_file"]
            if p.get("subtask_name"):
                s["subtask_name"] = p["subtask_name"]
            s["error_code"] = str(p.get("print_error") or p.get("error_code") or "")
            pct = s["percent"] or 0
            rt = fmt_minutes(s["remaining_time"])
        if not gs or gs == prev:
            return

        name = pcfg(self.index)["name"]

        def task_name():
            with lock:
                return s["subtask_name"] or Path(s["gcode_file"]).name or "未知任务"

        if gs == "RUNNING" and prev in ("", "IDLE", "FINISH", "FAILED", "SUCCEEDED"):
            with lock:
                s["print_start_ts"] = time.time()
            step = int(n.get("progress_step") or 0)
            self.last_bucket = pct // step if step > 0 else 0
            if n.get("on_start"):
                notify(name, "开始打印", f"**任务**：{task_name()}\n\n**进度**：{pct}%",
                       tag="start", with_media=True, printer_index=self.index)

        if gs == "RUNNING" and prev == "PAUSED" and n.get("on_resume"):
            notify(name, "打印已恢复", f"**任务**：{task_name()}",
                   tag="resume", with_media=True, printer_index=self.index)

        if gs in ("FINISH", "SUCCEEDED"):
            with lock:
                start_ts = s["print_start_ts"]
            dur = ""
            if start_ts:
                mins = int((time.time() - start_ts) // 60)
                dur = f"\n\n**耗时**：{fmt_minutes(mins) or '0分钟'}"
            if n.get("on_finish"):
                notify(name, "打印完成", f"**任务**：{task_name()}{dur}",
                       tag="finish", with_media=True, printer_index=self.index)
        elif gs == "FAILED":
            with lock:
                err = s["error_code"]
            if n.get("on_failed"):
                notify(name, "打印失败", f"**任务**：{task_name()}\n\n**错误码**：{err or '未知'}",
                       tag="fail", with_media=True, printer_index=self.index)
        elif gs == "PAUSED" and prev != "PAUSED" and n.get("on_pause"):
            notify(name, "打印已暂停", f"**任务**：{task_name()}",
                   tag="pause", with_media=True, printer_index=self.index)

        if gs == "RUNNING":
            step = int(n.get("progress_step") or 0)
            if step > 0:
                b = pct // step
                if b > self.last_bucket:
                    self.last_bucket = b
                    notify(name, f"打印进度 {b * step}%",
                           f"**任务**：{task_name()}\n\n**剩余时间**：{rt or '未知'}",
                           tag="print", with_media=True, printer_index=self.index)


monitors = [PrinterMonitor(i) for i in range(MAX_PRINTERS)]


def start_enabled():
    with lock:
        enabled = [i for i, p in enumerate(config["printers"]) if p["enabled"]]
    for i in enabled:
        monitors[i].start()


@asynccontextmanager
async def lifespan(app):
    load_config()
    start_enabled()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/config")
def api_get_config():
    with lock:
        cfg = json.loads(json.dumps(config))
    printers = []
    for p in cfg["printers"]:
        printers.append({
            "name": p["name"],
            "host": p["host"],
            "sn": p["sn"],
            "enabled": p["enabled"],
            "access_code": "",
            "access_code_set": bool(p["access_code"].strip()),
        })
    d = cfg["dingtalk"]
    return {
        "printers": printers,
        "dingtalk": {
            **d,
            "webhook": "",
            "secret": "",
            "webhook_set": bool(d["webhook"].strip()),
            "secret_set": bool(d["secret"].strip()),
        },
        "notify": cfg["notify"],
        "camera": cfg["camera"],
    }


@app.post("/api/config")
async def api_post_config(req: Request):
    global config
    body = await req.json()
    with lock:
        before = [dict(p) for p in config["printers"]]
        cur = json.loads(json.dumps(config))
    for i, item in enumerate((body.get("printers") or [])[:MAX_PRINTERS]):
        if not isinstance(item, dict):
            continue
        p = cur["printers"][i]
        if "name" in item:
            p["name"] = str(item["name"]).strip() or f"P2S_{i + 1}"
        for k in ("host", "sn"):
            if k in item:
                p[k] = str(item[k]).strip()
        if str(item.get("access_code", "")).strip():
            p["access_code"] = str(item["access_code"]).strip()
    dt = body.get("dingtalk") or {}
    if "keyword" in dt:
        cur["dingtalk"]["keyword"] = str(dt["keyword"]).strip()
    if str(dt.get("webhook", "")).strip():
        cur["dingtalk"]["webhook"] = str(dt["webhook"]).strip()
    if str(dt.get("secret", "")).strip():
        cur["dingtalk"]["secret"] = str(dt["secret"]).strip()
    nt = body.get("notify") or {}
    for k, v in cur["notify"].items():
        if k not in nt:
            continue
        if isinstance(v, bool):
            cur["notify"][k] = bool(nt[k])
        elif isinstance(v, int):
            try:
                cur["notify"][k] = max(0, min(100, int(nt[k])))
            except (TypeError, ValueError):
                pass
    cm = body.get("camera") or {}
    for k in ("enabled", "snapshot", "clip"):
        if k in cm:
            cur["camera"][k] = bool(cm[k])
    if "clip_seconds" in cm:
        try:
            cur["camera"]["clip_seconds"] = max(2, min(15, int(cm["clip_seconds"])))
        except (TypeError, ValueError):
            pass
    with lock:
        config = cur
        save_config()
    add_event("info", "配置已保存")
    for i, p in enumerate(cur["printers"]):
        old = before[i]
        if (p["host"], p["sn"], p["access_code"], p["enabled"]) == (
            old["host"], old["sn"], old["access_code"], old["enabled"]
        ):
            continue
        if p["enabled"]:
            monitors[i].restart()
        else:
            monitors[i].stop()
    return {"ok": True, "msg": "已保存"}


@app.post("/api/printer/{index}/toggle")
async def api_toggle_printer(index: int, req: Request):
    if not (0 <= index < MAX_PRINTERS):
        raise HTTPException(status_code=400, detail="无效的设备序号")
    body = await req.json()
    enabled = bool(body.get("enabled"))
    with lock:
        p = config["printers"][index]
        if enabled and not (p["host"] and p["sn"] and p["access_code"]):
            return {"ok": False, "msg": "先填写该打印机的 IP / 序列号 / 访问码并保存"}
        p["enabled"] = enabled
        name = p["name"]
        save_config()
    if enabled:
        monitors[index].restart()
    else:
        monitors[index].stop()
    add_event("info", f"{name} 已{'启动' if enabled else '停用'}监控")
    return {"ok": True, "msg": f"{name} 已{'启动' if enabled else '停用'}"}


@app.get("/api/status")
def api_status():
    now = time.time()
    out = []
    with lock:
        for i, p in enumerate(config["printers"]):
            s = dict(statuses[i])
            out.append({
                "index": i,
                "name": p["name"],
                "enabled": p["enabled"],
                "configured": s["configured"] or bool(p["host"] and p["sn"] and p["access_code"]),
                "connected": s["connected"],
                "gcode_state": s["gcode_state"],
                "state_text": STATE_TEXT.get(s["gcode_state"], s["gcode_state"] or "未知"),
                "online": bool(s["connected"] and s["last_msg_ts"] and now - s["last_msg_ts"] < 120),
                "percent": s["percent"],
                "remaining_time": s["remaining_time"],
                "layer_num": s["layer_num"],
                "total_layer_num": s["total_layer_num"],
                "nozzle_temper": s["nozzle_temper"],
                "bed_temper": s["bed_temper"],
                "task": s["subtask_name"] or (Path(s["gcode_file"]).name if s["gcode_file"] else ""),
                "error_code": s["error_code"],
                "last_msg_ago": int(now - s["last_msg_ts"]) if s["last_msg_ts"] else None,
                "elapsed_mins": int((now - s["print_start_ts"]) // 60) if s["print_start_ts"] else None,
            })
        enabled_count = sum(1 for p in config["printers"] if p["enabled"])
    online_count = sum(1 for x in out if x["online"])
    return {"printers": out, "enabled_count": enabled_count, "online_count": online_count}


@app.get("/api/events")
def api_events():
    with lock:
        return {"events": list(events)}


@app.get("/media/{name}")
def api_media(name: str):
    f = MEDIA_DIR / Path(name).name
    if f.parent != MEDIA_DIR or not f.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(f)


@app.post("/api/test/printer")
async def api_test_printer(req: Request):
    body = await req.json()
    try:
        index = int(body.get("index", -1))
    except (TypeError, ValueError):
        index = -1
    if not (0 <= index < MAX_PRINTERS):
        raise HTTPException(status_code=400, detail="无效的设备序号")
    p = pcfg(index)
    if not (p["host"] and p["sn"] and p["access_code"]):
        return {"ok": False, "msg": f"{p['name']} 配置不完整"}
    res = {}

    def run():
        evt = threading.Event()
        c = mqtt.Client()
        c.tls_set(cert_reqs=ssl.CERT_NONE)
        c.tls_insecure_set(True)
        c.username_pw_set("bblp", p["access_code"])

        def on_connect(cl, u, f, rc):
            cl.subscribe(f"device/{p['sn']}/report")
            cl.publish(
                f"device/{p['sn']}/request",
                json.dumps({"pushing": {"sequence_id": "1", "command": "pushall"}}),
            )

        def on_message(cl, u, m):
            try:
                pr = json.loads(m.payload.decode("utf-8", "ignore")).get("print") or {}
                if isinstance(pr, dict) and pr.get("gcode_state"):
                    res["state"] = pr["gcode_state"]
            except Exception:
                pass
            evt.set()

        c.on_connect = on_connect
        c.on_message = on_message
        try:
            c.connect(p["host"], 8883, keepalive=30)
        except Exception as e:
            res["error"] = f"无法连接 {p['host']}:8883 — {e}"
            evt.set()
            return
        c.loop_start()
        evt.wait(8)
        try:
            c.disconnect()
            c.loop_stop()
        except Exception:
            pass

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=12)
    if "error" in res:
        return {"ok": False, "msg": res["error"]}
    if "state" not in res:
        return {"ok": False, "msg": f"{p['name']} 已连接但未收到数据，请检查序列号 / 访问码"}
    msg = f"{p['name']} MQTT 正常（{STATE_TEXT.get(res['state'], res['state'])}）"
    url = None
    with lock:
        cam = dict(config["camera"])
    if cam["enabled"]:
        jpg = grab_snapshot(index, "test")
        if jpg:
            base = lan_base_url(p["host"])
            url = f"{base}/media/{jpg.name}" if base else None
            msg += "；摄像头抓图成功"
        else:
            msg += "；摄像头抓图失败（检查 RTSP / 固件）"
    return {"ok": True, "msg": msg, "url": url}


@app.post("/api/test/dingtalk")
def api_test_dingtalk():
    ok, msg = dingtalk_send("测试消息", "这是 webhook-p2s 发出的测试消息。", force=True)
    return {"ok": ok, "msg": msg}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
