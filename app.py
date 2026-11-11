import base64
import hashlib
import hmac
import json
import logging
import os
import ssl
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


def notify(title, text, force=False):
    with lock:
        enabled = config["notify"]["enabled"]
    ok, msg = dingtalk_send(title, text, force=force)
    if force or enabled:
        add_event("notify" if ok else "error", f"【{title}】{msg}")
    else:
        add_event("info", f"【{title}】通知已停用，未推送")
    return ok, msg


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
                notify("开始打印", f"**任务**：{task_name()}\n\n**进度**：{pct}%")

        if gs == "RUNNING" and prev == "PAUSED" and n.get("on_resume"):
            notify("打印已恢复", f"**任务**：{task_name()}")

        if gs in ("FINISH", "SUCCEEDED"):
            with lock:
                start_ts = status["print_start_ts"]
            dur = ""
            if start_ts:
                mins = int((time.time() - start_ts) // 60)
                dur = f"\n\n**耗时**：{fmt_minutes(mins) or '0分钟'}"
            if n.get("on_finish"):
                notify("打印完成", f"**任务**：{task_name()}{dur}")
        elif gs == "FAILED":
            with lock:
                err = status["error_code"]
            if n.get("on_failed"):
                notify("打印失败", f"**任务**：{task_name()}\n\n**错误码**：{err or '未知'}")
        elif gs == "PAUSED" and prev != "PAUSED" and n.get("on_pause"):
            notify("打印已暂停", f"**任务**：{task_name()}")

        if gs == "RUNNING":
            step = int(n.get("progress_step") or 0)
            if step > 0:
                b = pct // step
                if b > self.last_bucket:
                    self.last_bucket = b
                    notify(f"打印进度 {b * step}%", f"**任务**：{task_name()}\n\n**剩余时间**：{rt or '未知'}")


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
