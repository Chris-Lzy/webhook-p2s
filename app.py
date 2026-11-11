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
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monitor")

PORT = int(os.environ.get("PORT", "8787"))
DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent / "data")))
CONFIG_PATH = DATA_DIR / "config.json"

DEFAULT_CONFIG = {
    "printer": {"host": "", "sn": "", "access_code": ""},
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

lock = threading.RLock()
config = json.loads(json.dumps(DEFAULT_CONFIG))
status = {
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
events = deque(maxlen=50)


def deep_merge(dst, src):
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = v


def load_config():
    global config
    if CONFIG_PATH.exists():
        try:
            saved = json.loads(CONFIG_PATH.read_text("utf-8"))
            merged = json.loads(json.dumps(DEFAULT_CONFIG))
            deep_merge(merged, saved)
            config = merged
        except Exception as e:
            log.warning("读取配置失败: %s", e)


def save_config():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(CONFIG_PATH)


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


def notify(title, text, force=False, with_media=False):
    def run():
        with lock:
            enabled = config["notify"]["enabled"]
            cam_enabled = config["camera"]["enabled"]
        if with_media and cam_enabled and not force:
            text = attach_media(title, text)
        ok, msg = dingtalk_send(title, text, force=force)
        if force or enabled:
            add_event("notify" if ok else "error", f"【{title}】{msg}")
        else:
            add_event("info", f"【{title}】通知已停用，未推送")

    threading.Thread(target=run, daemon=True).start()


def fmt_minutes(m):
    try:
        m = int(m)
    except (TypeError, ValueError):
        return ""
    if m <= 0:
        return ""
    return f"{m // 60}小时{m % 60}分钟" if m >= 60 else f"{m}分钟"


def task_name():
    with lock:
        return status["subtask_name"] or Path(status["gcode_file"]).name or "未知任务"


MEDIA_DIR = DATA_DIR / "media"
EVENT_TAGS = {
    "开始打印": "start",
    "打印完成": "finish",
    "打印失败": "fail",
    "打印已暂停": "pause",
    "打印已恢复": "resume",
}


def rtsp_url():
    with lock:
        p = dict(config["printer"])
    code = urllib.parse.quote(p["access_code"].strip(), safe="")
    return f"rtsp://bblp:{code}@{p['host'].strip()}:322/stream"


def lan_base_url():
    with lock:
        host = config["printer"]["host"].strip()
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


def run_ffmpeg(out_args, out_path, timeout):
    if not shutil.which("ffmpeg"):
        return False
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-rtsp_transport", "tcp", "-i", rtsp_url(), *out_args, str(out_path),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
        return r.returncode == 0 and Path(out_path).exists() and Path(out_path).stat().st_size > 0
    except Exception:
        return False


def grab_snapshot(tag):
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    jpg = MEDIA_DIR / f"{tag}_{int(time.time())}.jpg"
    return jpg if run_ffmpeg(["-frames:v", "1", "-q:v", "3"], jpg, timeout=12) else None


def grab_clip(tag, seconds):
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    sec = max(2, min(15, int(seconds or 5)))
    mp4 = MEDIA_DIR / f"{tag}_{int(time.time())}.mp4"
    ok = run_ffmpeg(
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


def capture_media(tag):
    with lock:
        cam = dict(config["camera"])
    if not cam["enabled"]:
        return None, None
    jpg = grab_snapshot(tag) if cam["snapshot"] else None
    mp4 = grab_clip(tag, cam["clip_seconds"]) if cam["clip"] else None
    if jpg or mp4:
        prune_media()
    return jpg, mp4


def attach_media(title, text):
    tag = EVENT_TAGS.get(title, "print")
    jpg, mp4 = capture_media(tag)
    if not jpg and not mp4:
        add_event("info", f"【{title}】摄像头抓取失败，仅推送文字")
        return text
    base = lan_base_url()
    if not base:
        add_event("info", f"【{title}】无法确定 NAS 局域网地址，画面未附带")
        return text
    parts = [text]
    if jpg:
        parts.append(f"![监控画面]({base}/media/{jpg.name})")
    if mp4:
        parts.append(f"[查看视频]({base}/media/{mp4.name})")
    return "\n\n".join(parts)


class PrinterMonitor:
    def __init__(self):
        self.client = None
        self.last_bucket = 0

    def start(self):
        self.last_bucket = 0
        with lock:
            p = dict(config["printer"])
        if not (p["host"].strip() and p["sn"].strip() and p["access_code"].strip()):
            with lock:
                status["configured"] = False
                status["connected"] = False
            add_event("info", "打印机配置不完整，监控未启动，请在网页上填写配置")
            return
        with lock:
            status["configured"] = True
        c = mqtt.Client()
        c.tls_set(cert_reqs=ssl.CERT_NONE)
        c.tls_insecure_set(True)
        c.username_pw_set("bblp", p["access_code"].strip())
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        c.reconnect_delay_set(2, 30)
        self.client = c
        try:
            c.connect(p["host"].strip(), 8883, keepalive=30)
        except Exception as e:
            add_event("error", f"打印机连接失败: {e}")
            return
        c.loop_start()

    def stop(self):
        if self.client:
            try:
                self.client.disconnect()
                self.client.loop_stop()
            except Exception:
                pass
            self.client = None
        with lock:
            status["connected"] = False

    def restart(self):
        self.stop()
        time.sleep(0.3)
        self.start()

    def _on_connect(self, client, userdata, flags, rc):
        with lock:
            p = dict(config["printer"])
            status["connected"] = True
        add_event("info", f"已连接打印机 {p['host']}")
        client.subscribe(f"device/{p['sn']}/report")
        client.publish(
            f"device/{p['sn']}/request",
            json.dumps({"pushing": {"sequence_id": "1", "command": "pushall"}}),
        )

    def _on_disconnect(self, client, userdata, rc):
        with lock:
            printing = status["gcode_state"] in ("RUNNING", "PAUSED", "PREPARE", "SLICING")
            do_notify = config["notify"]["enabled"] and config["notify"]["on_offline"]
            status["connected"] = False
        add_event("error", "与打印机的连接已断开，将自动重连")
        if printing and do_notify:
            notify("打印机离线", "MQTT 连接断开，打印可能中断，服务将自动重连。")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8", "ignore"))
        except Exception:
            return
        try:
            self._handle(data)
        except Exception as e:
            log.warning("处理消息出错: %s", e)

    def _handle(self, data):
        p = data.get("print") or {}
        if not isinstance(p, dict):
            return
        gs = p.get("gcode_state") or ""
        with lock:
            prev = status["gcode_state"]
            n = dict(config["notify"])
            status["last_msg_ts"] = time.time()
            if gs:
                status["gcode_state"] = gs
            for key in ("percent", "remaining_time", "layer_num", "total_layer_num"):
                if p.get(key) is not None:
                    try:
                        status[key] = int(p[key])
                    except (TypeError, ValueError):
                        pass
            for key in ("nozzle_temper", "bed_temper"):
                if p.get(key) is not None:
                    try:
                        status[key] = round(float(p[key]), 1)
                    except (TypeError, ValueError):
                        pass
            if p.get("gcode_file"):
                status["gcode_file"] = p["gcode_file"]
            if p.get("subtask_name"):
                status["subtask_name"] = p["subtask_name"]
            status["error_code"] = str(p.get("print_error") or p.get("error_code") or "")
            pct = status["percent"] or 0
            rt = fmt_minutes(status["remaining_time"])

        if not gs or gs == prev:
            return

        if gs == "RUNNING" and prev in ("", "IDLE", "FINISH", "FAILED", "SUCCEEDED"):
            with lock:
                status["print_start_ts"] = time.time()
            step = int(n.get("progress_step") or 0)
            self.last_bucket = pct // step if step > 0 else 0
            if n.get("on_start"):
                notify("开始打印", f"**任务**：{task_name()}\n\n**进度**：{pct}%", with_media=True)

        if gs == "RUNNING" and prev == "PAUSED" and n.get("on_resume"):
            notify("打印已恢复", f"**任务**：{task_name()}", with_media=True)

        if gs in ("FINISH", "SUCCEEDED"):
            with lock:
                start_ts = status["print_start_ts"]
            dur = ""
            if start_ts:
                mins = int((time.time() - start_ts) // 60)
                dur = f"\n\n**耗时**：{fmt_minutes(mins) or '0分钟'}"
            if n.get("on_finish"):
                notify("打印完成", f"**任务**：{task_name()}{dur}", with_media=True)
        elif gs == "FAILED":
            with lock:
                err = status["error_code"]
            if n.get("on_failed"):
                notify("打印失败", f"**任务**：{task_name()}\n\n**错误码**：{err or '未知'}", with_media=True)
        elif gs == "PAUSED" and prev != "PAUSED" and n.get("on_pause"):
            notify("打印已暂停", f"**任务**：{task_name()}", with_media=True)

        if gs == "RUNNING":
            step = int(n.get("progress_step") or 0)
            if step > 0:
                b = pct // step
                if b > self.last_bucket:
                    self.last_bucket = b
                    notify(f"打印进度 {b * step}%", f"**任务**：{task_name()}\n\n**剩余时间**：{rt or '未知'}", with_media=True)


monitor = PrinterMonitor()


@asynccontextmanager
async def lifespan(app):
    load_config()
    monitor.start()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/config")
def api_get_config():
    with lock:
        cfg = json.loads(json.dumps(config))
    p, d = cfg["printer"], cfg["dingtalk"]
    return {
        "printer": {**p, "access_code": "", "access_code_set": bool(p["access_code"].strip())},
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
        cur = json.loads(json.dumps(config))
    pr = body.get("printer") or {}
    for k in ("host", "sn"):
        if k in pr:
            cur["printer"][k] = str(pr[k]).strip()
    if str(pr.get("access_code", "")).strip():
        cur["printer"]["access_code"] = str(pr["access_code"]).strip()
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
    add_event("info", "配置已保存，监控已重启")
    monitor.restart()
    return {"ok": True, "msg": "已保存并重启监控"}


@app.get("/api/status")
def api_status():
    with lock:
        s = dict(status)
    now = time.time()
    s["state_text"] = STATE_TEXT.get(s["gcode_state"], s["gcode_state"] or "未知")
    s["online"] = bool(s["connected"] and s["last_msg_ts"] and now - s["last_msg_ts"] < 120)
    s["last_msg_ago"] = int(now - s["last_msg_ts"]) if s["last_msg_ts"] else None
    s["elapsed_mins"] = int((now - s["print_start_ts"]) // 60) if s["print_start_ts"] else None
    return s


@app.get("/api/events")
def api_events():
    with lock:
        return {"events": list(events)}


@app.post("/api/test/printer")
async def api_test_printer(req: Request):
    body = await req.json()
    with lock:
        cfgp = dict(config["printer"])
    host = str(body.get("host") or cfgp["host"]).strip()
    sn = str(body.get("sn") or cfgp["sn"]).strip()
    code = str(body.get("access_code") or cfgp["access_code"]).strip()
    if not (host and sn and code):
        return {"ok": False, "msg": "配置不完整，需要 IP / 序列号 / 访问码"}
    res = {}

    def run():
        evt = threading.Event()
        c = mqtt.Client()
        c.tls_set(cert_reqs=ssl.CERT_NONE)
        c.tls_insecure_set(True)
        c.username_pw_set("bblp", code)

        def on_connect(cl, u, f, rc):
            cl.subscribe(f"device/{sn}/report")
            cl.publish(
                f"device/{sn}/request",
                json.dumps({"pushing": {"sequence_id": "1", "command": "pushall"}}),
            )

        def on_message(cl, u, m):
            try:
                d = json.loads(m.payload.decode("utf-8", "ignore"))
                pr = d.get("print") or {}
                if isinstance(pr, dict) and pr:
                    res["state"] = pr.get("gcode_state", "未知")
            except Exception:
                pass
            evt.set()

        c.on_connect = on_connect
        c.on_message = on_message
        try:
            c.connect(host, 8883, keepalive=30)
        except Exception as e:
            res["error"] = f"无法连接 {host}:8883 — {e}"
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
        return {"ok": False, "msg": "已连接但未收到打印机数据，请检查序列号 / 访问码"}
    return {"ok": True, "msg": f"连接成功，打印机状态：{STATE_TEXT.get(res['state'], res['state'])}"}


@app.post("/api/test/dingtalk")
def api_test_dingtalk():
    ok, msg = dingtalk_send("测试消息", "这是拓竹打印机监控服务发出的测试消息。", force=True)
    return {"ok": ok, "msg": msg}


@app.get("/media/{name}")
def api_media(name: str):
    f = MEDIA_DIR / Path(name).name
    if f.parent != MEDIA_DIR or not f.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(f)


@app.post("/api/test/camera")
def api_test_camera():
    with lock:
        p = dict(config["printer"])
    if not (p["host"].strip() and p["access_code"].strip()):
        return {"ok": False, "msg": "请先保存打印机 IP 和访问码"}
    jpg = grab_snapshot("test")
    if not jpg:
        return {"ok": False, "msg": "抓图失败：打印机可能不支持 RTSP（X1 全系支持，P1/A1 需较新固件且开启局域网模式）"}
    base = lan_base_url()
    if not base:
        return {"ok": False, "msg": "抓图成功，但无法确定 NAS 局域网地址，消息中无法引用"}
    return {"ok": True, "msg": "抓图成功，这就是钉钉消息里内嵌的画面", "url": f"{base}/media/{jpg.name}"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
