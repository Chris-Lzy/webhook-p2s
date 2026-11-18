import base64
import concurrent.futures
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import shutil
import socket
import ssl
import subprocess
import threading
import time
import urllib.parse
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monitor")

__version__ = "2.0.0"

PORT = int(os.environ.get("PORT", "8787"))
DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent / "data")))
CONFIG_PATH = DATA_DIR / "config.json"
MEDIA_DIR = DATA_DIR / "media"
MAX_PRINTERS = 1000
MAX_WINDOWS = 8
WINDOW_EVENTS = ("start", "finish", "failed", "pause", "resume", "offline", "progress")

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

DEFAULT_CONFIG = {
    "printers": [
        {"id": "p_1", "name": "P2S_1", "host": "", "sn": "", "access_code": "", "enabled": False}
    ],
    "dingtalk": {"enabled": True, "webhook": "", "secret": "", "keyword": ""},
    "wecom": {"enabled": False, "webhook": ""},
    "feishu": {"enabled": False, "webhook": "", "secret": ""},
    "notify": {
        "enabled": True,
        "on_start": True,
        "on_finish": True,
        "on_failed": True,
        "on_pause": False,
        "on_resume": False,
        "on_offline": True,
        "progress_step": 25,
        "windows": [],
    },
    "camera": {"enabled": False, "snapshot": True, "clip": True, "clip_seconds": 5},
}

lock = threading.RLock()
config = json.loads(json.dumps(DEFAULT_CONFIG))
events = deque(maxlen=100)


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


statuses: dict[str, dict] = {}
monitors: dict[str, "PrinterMonitor"] = {}


def deep_merge(dst, src):
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = v


def server_tz_label():
    tz = os.environ.get("TZ", "").strip()
    if tz:
        return tz
    now = datetime.now().astimezone()
    key = getattr(now.tzinfo, "key", None)
    if key:
        return key
    offset = now.strftime("%z")
    if len(offset) == 5:
        sign, hh, mm = offset[0], offset[1:3], offset[3:]
        if hh == "00" and mm == "00":
            return "UTC"
        return f"UTC{sign}{hh}:{mm}"
    return "本地时区"


def canonicalize_hhmm(value):
    if not isinstance(value, str):
        return None
    parts = value.strip().replace("：", ":").split(":")
    if len(parts) not in (2, 3):
        return None
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return f"{hour:02d}:{minute:02d}"


def parse_hhmm(value):
    text = canonicalize_hhmm(value)
    if not text:
        return None
    hour, minute = text.split(":")
    return int(hour) * 60 + int(minute)


def minutes_in_window(now_min, start_min, end_min):
    if start_min == end_min:
        return True
    if start_min < end_min:
        return start_min <= now_min <= end_min
    return now_min >= start_min or now_min <= end_min


def window_weekdays(window):
    days = window.get("weekdays")
    if days is None:
        return set(range(7))
    if not isinstance(days, list):
        return set()
    out = set()
    for day in days:
        try:
            number = int(day)
        except (TypeError, ValueError):
            continue
        if 0 <= number <= 6:
            out.add(number)
    return out


def schedule_allows(windows, event_key, now=None):
    if not windows:
        return True
    if now is None:
        now = datetime.now().astimezone()
    elif now.tzinfo is not None:
        now = now.astimezone()
    now_min = now.hour * 60 + now.minute
    weekday = now.weekday()
    for window in windows:
        if not isinstance(window, dict) or not window.get("enabled", True):
            continue
        start = parse_hhmm(window.get("start", ""))
        end = parse_hhmm(window.get("end", ""))
        if start is None or end is None:
            continue
        if weekday not in window_weekdays(window):
            continue
        if not minutes_in_window(now_min, start, end):
            continue
        events = window.get("events")
        if isinstance(events, dict) and events.get(event_key):
            return True
    return False


def push_gate(enabled, windows, event_key, force=False, now=None):
    if force:
        return "send"
    if not enabled:
        return "disabled"
    if not schedule_allows(windows, event_key, now=now):
        return "window"
    return "send"


def normalize_windows(raw, strict=False):
    if raw is None:
        return [], None
    if not isinstance(raw, list):
        if strict:
            return [], "推送时间段格式不正确"
        return [], None
    if strict and len(raw) > MAX_WINDOWS:
        return [], f"最多添加 {MAX_WINDOWS} 个时间段"
    out = []
    for item in raw[:MAX_WINDOWS]:
        if not isinstance(item, dict):
            if strict:
                return [], "推送时间段格式不正确"
            continue
        start = canonicalize_hhmm(str(item.get("start", "")))
        end = canonicalize_hhmm(str(item.get("end", "")))
        if start is None or end is None:
            if strict:
                return [], "时间段的开始和结束须为 24 小时制 HH:MM"
            continue
        if "weekdays" not in item or item.get("weekdays") is None:
            days = list(range(7))
        else:
            days = sorted(window_weekdays({"weekdays": item.get("weekdays")}))
        if isinstance(item.get("events"), dict):
            events = {key: bool(item["events"].get(key, False)) for key in WINDOW_EVENTS}
        elif strict:
            return [], "时间段需要选择推送事件"
        else:
            events = {key: True for key in WINDOW_EVENTS}
        out.append({
            "enabled": bool(item.get("enabled", True)),
            "start": start,
            "end": end,
            "weekdays": days,
            "events": events,
        })
    return out, None


def generate_printer_id():
    return f"p_{int(time.time() * 1000) % 100000000}_{os.urandom(2).hex()}"


def ensure_printer_id(p, fallback_idx=1):
    pid = str(p.get("id") or "").strip()
    if not pid:
        pid = f"p_{fallback_idx}"
    p["id"] = pid
    return pid


def get_printer_status(pid):
    with lock:
        if pid not in statuses:
            statuses[pid] = blank_status()
        return statuses[pid]


def find_printer_index_and_data(identifier):
    with lock:
        printers = config["printers"]
        # Try finding by ID first
        for idx, p in enumerate(printers):
            if str(p.get("id")) == str(identifier):
                return idx, p
        # Try finding by numeric index
        try:
            num = int(identifier)
            if 0 <= num < len(printers):
                return num, printers[num]
        except (ValueError, TypeError):
            pass
        return None, None


def get_monitor_for_printer(pid):
    with lock:
        if pid not in monitors:
            monitors[pid] = PrinterMonitor(pid)
        return monitors[pid]


def load_config():
    global config
    if not CONFIG_PATH.exists():
        with lock:
            config = json.loads(json.dumps(DEFAULT_CONFIG))
            ensure_printer_id(config["printers"][0], 1)
            get_or_create_monitors()
        return
    try:
        saved = json.loads(CONFIG_PATH.read_text("utf-8"))
    except Exception as e:
        log.warning("读取配置失败: %s", e)
        return
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    for sec in ("dingtalk", "wecom", "feishu", "notify", "camera"):
        if isinstance(saved.get(sec), dict):
            deep_merge(cfg[sec], saved[sec])
    items = saved.get("printers")
    legacy = isinstance(saved.get("printer"), dict)
    if not isinstance(items, list) and legacy:
        items = [saved["printer"]]
    parsed_printers = []
    used_ids = set()
    for i, item in enumerate(items or []):
        if len(parsed_printers) >= MAX_PRINTERS or not isinstance(item, dict):
            break
        pid = str(item.get("id") or "").strip()
        if not pid or pid in used_ids:
            pid = f"p_{i + 1}"
        used_ids.add(pid)
        p = {
            "id": pid,
            "name": str(item.get("name") or f"P2S_{i + 1}").strip(),
            "host": str(item.get("host") or "").strip(),
            "sn": str(item.get("sn") or "").strip(),
            "access_code": str(item.get("access_code") or "").strip(),
            "enabled": False,
        }
        p["enabled"] = bool(item.get("enabled", legacy)) and bool(p["host"] and p["sn"] and p["access_code"])
        parsed_printers.append(p)
    if not parsed_printers:
        parsed_printers.append({
            "id": "p_1", "name": "P2S_1", "host": "", "sn": "", "access_code": "", "enabled": False
        })
    cfg["printers"] = parsed_printers
    saved_windows = cfg["notify"].get("windows") or []
    cleaned, _ = normalize_windows(saved_windows, strict=False)
    if isinstance(saved_windows, list) and len(cleaned) != len(saved_windows):
        log.warning("已忽略无效的推送时间段")
    cfg["notify"]["windows"] = cleaned
    with lock:
        config = cfg
        get_or_create_monitors()


def save_config():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(CONFIG_PATH)


def get_or_create_monitors():
    with lock:
        current_pids = set()
        for p in config["printers"]:
            pid = p["id"]
            current_pids.add(pid)
            if pid not in monitors:
                monitors[pid] = PrinterMonitor(pid)
            if pid not in statuses:
                statuses[pid] = blank_status()
        # Remove monitors of deleted printers
        for pid in list(monitors.keys()):
            if pid not in current_pids:
                try:
                    monitors[pid].stop()
                except Exception:
                    pass
                del monitors[pid]
                if pid in statuses:
                    del statuses[pid]


def pcfg_by_id(pid):
    with lock:
        for p in config["printers"]:
            if p["id"] == pid:
                return dict(p)
        return {"id": pid, "name": pid, "host": "", "sn": "", "access_code": "", "enabled": False}


def add_event(etype, text):
    with lock:
        events.appendleft({"ts": time.time(), "type": etype, "text": text})


# ------------------ 推送渠道支持（钉钉、企业微信、飞书） ------------------

def dingtalk_send(title, text, force=False):
    with lock:
        d = dict(config["dingtalk"])
        enabled = config["notify"]["enabled"]
    if not force and not enabled:
        return False, "通知总开关已停用"
    if not force and not d.get("enabled", True):
        return False, "钉钉推送已停用"
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
            return True, "钉钉推送成功"
        return False, f"钉钉返回错误: {data.get('errmsg', data)}"
    except Exception as e:
        return False, f"钉钉发送失败: {e}"


def wecom_send(title, text, force=False):
    with lock:
        w = dict(config.get("wecom", {}))
        enabled = config["notify"]["enabled"]
    if not force and not enabled:
        return False, "通知总开关已停用"
    if not force and not w.get("enabled", False):
        return False, "企业微信推送已停用"
    webhook = w.get("webhook", "").strip()
    if not webhook:
        return False, "未配置企业微信 webhook"
    body = {
        "msgtype": "markdown",
        "markdown": {"content": f"### {title}\n\n{text}"},
    }
    try:
        r = requests.post(webhook, json=body, timeout=10)
        data = r.json()
        if data.get("errcode") == 0:
            return True, "企业微信推送成功"
        return False, f"企业微信返回错误: {data.get('errmsg', data)}"
    except Exception as e:
        return False, f"企业微信发送失败: {e}"


def feishu_send(title, text, force=False):
    with lock:
        f = dict(config.get("feishu", {}))
        enabled = config["notify"]["enabled"]
    if not force and not enabled:
        return False, "通知总开关已停用"
    if not force and not f.get("enabled", False):
        return False, "飞书推送已停用"
    webhook = f.get("webhook", "").strip()
    if not webhook:
        return False, "未配置飞书 webhook"
    ts = str(int(time.time()))
    body = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [[{"tag": "text", "text": text}]],
                }
            }
        },
    }
    secret = f.get("secret", "").strip()
    if secret:
        string_to_sign = f"{ts}\n{secret}"
        sign = base64.b64encode(
            hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
        ).decode("utf-8")
        body["timestamp"] = ts
        body["sign"] = sign
    try:
        r = requests.post(webhook, json=body, timeout=10)
        data = r.json()
        if data.get("code") == 0 or data.get("StatusCode") == 0:
            return True, "飞书推送成功"
        return False, f"飞书返回错误: {data.get('msg', data)}"
    except Exception as e:
        return False, f"飞书发送失败: {e}"


def push_to_all_channels(title, text, force=False, channel=None):
    results = []
    if channel:
        if channel == "dingtalk":
            return [dingtalk_send(title, text, force=force)]
        elif channel == "wecom":
            return [wecom_send(title, text, force=force)]
        elif channel == "feishu":
            return [feishu_send(title, text, force=force)]
        return [(False, f"未知渠道: {channel}")]

    # 钉钉
    with lock:
        dt_en = bool(config["dingtalk"].get("enabled", True) and config["dingtalk"].get("webhook", "").strip())
        wc_en = bool(config.get("wecom", {}).get("enabled", False) and config.get("wecom", {}).get("webhook", "").strip())
        fs_en = bool(config.get("feishu", {}).get("enabled", False) and config.get("feishu", {}).get("webhook", "").strip())

    if dt_en or force:
        results.append(dingtalk_send(title, text, force=force))
    if wc_en:
        results.append(wecom_send(title, text, force=force))
    if fs_en:
        results.append(feishu_send(title, text, force=force))

    if not results:
        results.append((False, "未启用任何推送渠道或未配置 Webhook"))
    return results


def notify(pname, title, text, tag="print", event=None, with_media=False, printer_id=None, force=False):
    def run():
        with lock:
            enabled = config["notify"]["enabled"]
            windows = list(config["notify"].get("windows") or [])
            cam_enabled = config["camera"]["enabled"]
        full_title = f"{pname} {title}"
        gate = push_gate(enabled, windows, event, force=force)
        if gate == "disabled":
            add_event("info", f"【{full_title}】通知已停用，未推送")
            return
        if gate == "window":
            add_event("skip", f"【{full_title}】不在推送时间段内，已跳过")
            log.info("%s 不在推送时间段内，已跳过", full_title)
            return
        body = text
        if with_media and cam_enabled and not force and printer_id is not None:
            body = attach_media(printer_id, tag, body)
        res_list = push_to_all_channels(full_title, body, force=force)
        for ok, msg in res_list:
            add_event("notify" if ok else "error", f"【{full_title}】{msg}")

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


def grab_snapshot(pid, tag):
    p = pcfg_by_id(pid)
    if not (p["host"] and p["access_code"]):
        return None
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    jpg = MEDIA_DIR / f"{pid}_{tag}_{int(time.time())}.jpg"
    return jpg if run_ffmpeg(rtsp_url(p), ["-frames:v", "1", "-q:v", "3"], jpg, timeout=12) else None


def grab_clip(pid, tag, seconds):
    p = pcfg_by_id(pid)
    if not (p["host"] and p["access_code"]):
        return None
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    sec = max(2, min(15, int(seconds or 5)))
    mp4 = MEDIA_DIR / f"{pid}_{tag}_{int(time.time())}.mp4"
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


def capture_media(pid, tag):
    with lock:
        cam = dict(config["camera"])
    if not cam["enabled"]:
        return None, None
    jpg = grab_snapshot(pid, tag) if cam["snapshot"] else None
    mp4 = grab_clip(pid, tag, cam["clip_seconds"]) if cam["clip"] else None
    if jpg or mp4:
        prune_media()
    return jpg, mp4


def attach_media(pid, tag, text):
    jpg, mp4 = capture_media(pid, tag)
    p = pcfg_by_id(pid)
    if not jpg and not mp4:
        add_event("info", f"{p['name']} 摄像头抓取失败，仅推送文字")
        return text
    base = lan_base_url(p["host"])
    if not base:
        add_event("info", f"{p['name']} 无法确定 NAS 局域网地址，画面未附带")
        return text
    parts = [text]
    if jpg:
        parts.append(f"![监控画面]({base}/media/{jpg.name})")
    if mp4:
        parts.append(f"[查看视频]({base}/media/{mp4.name})")
    return "\n\n".join(parts)


class PrinterMonitor:
    def __init__(self, pid):
        self.pid = pid
        self.client = None
        self.last_bucket = 0
        self.stopping = False

    def start(self):
        self.last_bucket = 0
        self.stopping = False
        p = pcfg_by_id(self.pid)
        s = get_printer_status(self.pid)
        if not (p["host"] and p["sn"] and p["access_code"]):
            with lock:
                s["configured"] = False
                s["connected"] = False
            return
        with lock:
            s["configured"] = True
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
            if self.pid in statuses:
                statuses[self.pid]["connected"] = False

    def restart(self):
        self.stop()
        time.sleep(0.3)
        self.start()

    def _on_connect(self, client, userdata, flags, rc):
        p = pcfg_by_id(self.pid)
        with lock:
            if self.pid in statuses:
                statuses[self.pid]["connected"] = True
        add_event("info", f"{p['name']} 已连接")
        client.subscribe(f"device/{p['sn']}/report")
        client.publish(
            f"device/{p['sn']}/request",
            json.dumps({"pushing": {"sequence_id": "1", "command": "pushall"}}),
        )

    def _on_disconnect(self, client, userdata, rc):
        if self.stopping or client is not self.client:
            with lock:
                if self.pid in statuses:
                    statuses[self.pid]["connected"] = False
            return
        p = pcfg_by_id(self.pid)
        with lock:
            s = get_printer_status(self.pid)
            printing = s["gcode_state"] in ("RUNNING", "PAUSED", "PREPARE", "SLICING")
            do_notify = config["notify"]["enabled"] and config["notify"]["on_offline"]
            s["connected"] = False
        add_event("error", f"{p['name']} 连接断开，将自动重连")
        if printing and do_notify:
            notify(p["name"], "打印机离线", "MQTT 连接断开，打印可能中断，服务将自动重连。",
                   tag="offline", event="offline", printer_id=self.pid)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8", "ignore"))
        except Exception:
            return
        try:
            self._handle(data)
        except Exception as e:
            log.warning("%s 处理消息出错: %s", pcfg_by_id(self.pid)["name"], e)

    def _handle(self, data):
        p = data.get("print") or {}
        if not isinstance(p, dict):
            return
        gs = p.get("gcode_state") or ""
        n = config["notify"]
        s = get_printer_status(self.pid)
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

        name = pcfg_by_id(self.pid)["name"]

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
                       tag="start", event="start", with_media=True, printer_id=self.pid)

        if gs == "RUNNING" and prev == "PAUSED" and n.get("on_resume"):
            notify(name, "打印已恢复", f"**任务**：{task_name()}",
                       tag="resume", event="resume", with_media=True, printer_id=self.pid)

        if gs in ("FINISH", "SUCCEEDED"):
            with lock:
                start_ts = s["print_start_ts"]
            dur = ""
            if start_ts:
                mins = int((time.time() - start_ts) // 60)
                dur = f"\n\n**耗时**：{fmt_minutes(mins) or '0分钟'}"
            if n.get("on_finish"):
                notify(name, "打印完成", f"**任务**：{task_name()}{dur}",
                       tag="finish", event="finish", with_media=True, printer_id=self.pid)
        elif gs == "FAILED":
            with lock:
                err = s["error_code"]
            if n.get("on_failed"):
                notify(name, "打印失败", f"**任务**：{task_name()}\n\n**错误码**：{err or '未知'}",
                       tag="fail", event="failed", with_media=True, printer_id=self.pid)
        elif gs == "PAUSED" and prev != "PAUSED" and n.get("on_pause"):
            notify(name, "打印已暂停", f"**任务**：{task_name()}",
                   tag="pause", event="pause", with_media=True, printer_id=self.pid)

        if gs == "RUNNING":
            step = int(n.get("progress_step") or 0)
            if step > 0:
                b = pct // step
                if b > self.last_bucket:
                    self.last_bucket = b
                    notify(name, f"打印进度 {b * step}%",
                           f"**任务**：{task_name()}\n\n**剩余时间**：{rt or '未知'}",
                           tag="print", event="progress", with_media=True, printer_id=self.pid)


def start_enabled():
    with lock:
        get_or_create_monitors()
        enabled_pids = [p["id"] for p in config["printers"] if p["enabled"]]
    for pid in enabled_pids:
        monitors[pid].start()


# ------------------ 局域网扫描器（SSDP 组播发现 + 网段 8883 端口快速探测） ------------------

def detect_local_subnet():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        parts = local_ip.split(".")
        return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24", local_ip
    except Exception:
        return "192.168.1.0/24", "127.0.0.1"


def scan_bambu_ssdp(timeout=2.0):
    discovered = {}
    msearch_msg = (
        "M-SEARCH * HTTP/1.1\r\n"
        "HOST: 239.255.255.250:2021\r\n"
        'MAN: "ssdp:discover"\r\n'
        "MX: 2\r\n"
        "ST: urn:bambulab-com:device:3dprinter:1\r\n\r\n"
    ).encode("utf-8")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)

        # 尝试发送到组播地址和通用广播地址
        for target in [("239.255.255.250", 2021), ("255.255.255.255", 2021), ("239.255.255.250", 1990)]:
            try:
                sock.sendto(msearch_msg, target)
            except Exception:
                pass

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                data, addr = sock.recvfrom(4096)
                text = data.decode("utf-8", "ignore")
                headers = {}
                for line in text.split("\r\n"):
                    if ":" in line:
                        k, v = line.split(":", 1)
                        headers[k.strip().lower()] = v.strip()

                ip = addr[0]
                loc = headers.get("location", "")
                if loc and "://" in loc:
                    try:
                        parsed = urllib.parse.urlparse(loc)
                        if parsed.hostname:
                            ip = parsed.hostname
                    except Exception:
                        pass

                sn = headers.get("usn") or headers.get("devserial.bambu.com") or ""
                name = headers.get("devname.bambu.com") or headers.get("server") or f"Bambu_{ip}"
                model = headers.get("devmodel.bambu.com") or "拓竹打印机"

                discovered[ip] = {
                    "ip": ip,
                    "name": name,
                    "sn": sn,
                    "model": model,
                    "source": "ssdp",
                }
            except socket.timeout:
                break
            except Exception:
                pass
        sock.close()
    except Exception as e:
        log.warning("SSDP 扫描异常: %s", e)
    return discovered


def probe_port(ip, port=8883, timeout=0.35):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        res = s.connect_ex((ip, port))
        s.close()
        return res == 0
    except Exception:
        return False


def scan_subnet_ports(subnet_str, timeout=0.35):
    open_ips = []
    try:
        net = ipaddress.IPv4Network(subnet_str, strict=False)
        hosts = [str(h) for h in net.hosts()]
        if len(hosts) > 256:
            hosts = hosts[:256]
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as ex:
            future_to_ip = {ex.submit(probe_port, ip, 8883, timeout): ip for ip in hosts}
            for fut in concurrent.futures.as_completed(future_to_ip):
                ip = future_to_ip[fut]
                try:
                    if fut.result():
                        open_ips.append(ip)
                except Exception:
                    pass
    except Exception as e:
        log.warning("网段扫描异常: %s", e)
    return open_ips


def scan_lan_printers(subnet_str=None):
    auto_subnet, _ = detect_local_subnet()
    target_subnet = subnet_str.strip() if (subnet_str and subnet_str.strip()) else auto_subnet

    # 1. 组播/广播 SSDP 发现
    ssdp_res = scan_bambu_ssdp(timeout=1.8)

    # 2. 网段 8883 端口并发探测
    port_ips = scan_subnet_ports(target_subnet, timeout=0.4)

    # 合并结果
    merged = dict(ssdp_res)
    for ip in port_ips:
        if ip not in merged:
            merged[ip] = {
                "ip": ip,
                "name": f"Bambu_{ip}",
                "sn": "",
                "model": "拓竹设备 (8883端口)",
                "source": "port_8883",
            }

    # 标注是否已经在当前配置中
    with lock:
        configured_hosts = {p["host"].strip() for p in config["printers"] if p.get("host")}
        configured_sns = {p["sn"].strip() for p in config["printers"] if p.get("sn")}

    results = []
    for item in merged.values():
        ip = item["ip"]
        sn = item.get("sn", "")
        already_added = (ip in configured_hosts) or (sn and sn in configured_sns)
        results.append({
            **item,
            "already_added": already_added,
        })
    results.sort(key=lambda x: (x["already_added"], x["ip"]))
    return {
        "ok": True,
        "subnet": target_subnet,
        "devices": results,
        "total": len(results),
    }


# ------------------ FastAPI Lifespan 与路由 ------------------

@asynccontextmanager
async def lifespan(app):
    load_config()
    start_enabled()
    yield
    # Shutdown
    with lock:
        for m in monitors.values():
            try:
                m.stop()
            except Exception:
                pass


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index():
    page = (Path(__file__).parent / "static" / "index.html").read_text("utf-8")
    return HTMLResponse(page.replace("{{APP_VERSION}}", __version__))


@app.get("/api/config")
def api_get_config():
    with lock:
        cfg = json.loads(json.dumps(config))
    printers = []
    for p in cfg["printers"]:
        printers.append({
            "id": p["id"],
            "name": p["name"],
            "host": p["host"],
            "sn": p["sn"],
            "enabled": p["enabled"],
            "access_code": "",
            "access_code_set": bool(p["access_code"].strip()),
        })
    d = cfg["dingtalk"]
    wc = cfg.get("wecom", {"enabled": False, "webhook": ""})
    fs = cfg.get("feishu", {"enabled": False, "webhook": "", "secret": ""})
    auto_subnet, _ = detect_local_subnet()
    return {
        "printers": printers,
        "max_printers": MAX_PRINTERS,
        "printer_count": len(printers),
        "dingtalk": {
            **d,
            "webhook": "",
            "secret": "",
            "webhook_set": bool(d.get("webhook", "").strip()),
            "secret_set": bool(d.get("secret", "").strip()),
        },
        "wecom": {
            **wc,
            "webhook": "",
            "webhook_set": bool(wc.get("webhook", "").strip()),
        },
        "feishu": {
            **fs,
            "webhook": "",
            "secret": "",
            "webhook_set": bool(fs.get("webhook", "").strip()),
            "secret_set": bool(fs.get("secret", "").strip()),
        },
        "notify": cfg["notify"],
        "camera": cfg["camera"],
        "version": __version__,
        "timezone": server_tz_label(),
        "local_subnet": auto_subnet,
    }


@app.post("/api/config")
async def api_post_config(req: Request):
    global config
    body = await req.json()
    with lock:
        before_printers = {p["id"]: dict(p) for p in config["printers"]}
        cur = json.loads(json.dumps(config))

    # 更新打印机列表
    if "printers" in body and isinstance(body["printers"], list):
        new_list = []
        used_ids = set()
        for idx, item in enumerate(body["printers"][:MAX_PRINTERS]):
            if not isinstance(item, dict):
                continue
            pid = str(item.get("id") or "").strip()
            if not pid or pid in used_ids:
                pid = generate_printer_id()
            used_ids.add(pid)

            # 获取原有记录以保留留空的 access_code
            old_p = before_printers.get(pid, {})
            name = str(item.get("name") or "").strip() or f"P2S_{idx + 1}"
            host = str(item.get("host") or "").strip()
            sn = str(item.get("sn") or "").strip()
            code_in = str(item.get("access_code", "")).strip()
            code = code_in if code_in else old_p.get("access_code", "")

            enabled = bool(item.get("enabled", False))
            if enabled and not (host and sn and code):
                enabled = False

            new_list.append({
                "id": pid,
                "name": name,
                "host": host,
                "sn": sn,
                "access_code": code,
                "enabled": enabled,
            })
        if not new_list:
            new_list = [{
                "id": "p_1", "name": "P2S_1", "host": "", "sn": "", "access_code": "", "enabled": False
            }]
        cur["printers"] = new_list

    # 更新钉钉
    dt = body.get("dingtalk") or {}
    if "enabled" in dt:
        cur["dingtalk"]["enabled"] = bool(dt["enabled"])
    if "keyword" in dt:
        cur["dingtalk"]["keyword"] = str(dt["keyword"]).strip()
    if str(dt.get("webhook", "")).strip():
        cur["dingtalk"]["webhook"] = str(dt["webhook"]).strip()
    if str(dt.get("secret", "")).strip():
        cur["dingtalk"]["secret"] = str(dt["secret"]).strip()

    # 更新企业微信
    wc = body.get("wecom") or {}
    if "enabled" in wc:
        cur["wecom"]["enabled"] = bool(wc["enabled"])
    if str(wc.get("webhook", "")).strip():
        cur["wecom"]["webhook"] = str(wc.get("webhook")).strip()

    # 更新飞书
    fs = body.get("feishu") or {}
    if "enabled" in fs:
        cur["feishu"]["enabled"] = bool(fs["enabled"])
    if str(fs.get("webhook", "")).strip():
        cur["feishu"]["webhook"] = str(fs.get("webhook")).strip()
    if str(fs.get("secret", "")).strip():
        cur["feishu"]["secret"] = str(fs.get("secret")).strip()

    # 更新通知设置
    nt = body.get("notify") or {}
    for k, v in cur["notify"].items():
        if k not in nt or k == "windows":
            continue
        if isinstance(v, bool):
            cur["notify"][k] = bool(nt[k])
        elif isinstance(v, int):
            try:
                cur["notify"][k] = max(0, min(100, int(nt[k])))
            except (TypeError, ValueError):
                pass
    if "windows" in nt:
        windows, err = normalize_windows(nt.get("windows"), strict=True)
        if err:
            return {"ok": False, "msg": err}
        cur["notify"]["windows"] = windows

    # 更新摄像头设置
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
        get_or_create_monitors()

    add_event("info", "配置已保存")

    # 根据配置变化重启/停止监视器
    with lock:
        current_p_map = {p["id"]: p for p in cur["printers"]}

    for pid, p in current_p_map.items():
        old = before_printers.get(pid, {})
        if (p["host"], p["sn"], p["access_code"], p["enabled"]) == (
            old.get("host"), old.get("sn"), old.get("access_code"), old.get("enabled")
        ):
            continue
        m = get_monitor_for_printer(pid)
        if p["enabled"]:
            m.restart()
        else:
            m.stop()

    return {"ok": True, "msg": "已保存"}


@app.post("/api/printer/add")
async def api_add_printer(req: Request):
    body = await req.json()
    with lock:
        if len(config["printers"]) >= MAX_PRINTERS:
            return {"ok": False, "msg": f"已达到最大设备数限制（{MAX_PRINTERS}台）"}
        pid = generate_printer_id()
        idx = len(config["printers"]) + 1
        name = str(body.get("name") or f"P2S_{idx}").strip()
        host = str(body.get("host") or "").strip()
        sn = str(body.get("sn") or "").strip()
        access_code = str(body.get("access_code") or "").strip()
        enabled = bool(body.get("enabled", False)) and bool(host and sn and access_code)

        new_p = {
            "id": pid,
            "name": name,
            "host": host,
            "sn": sn,
            "access_code": access_code,
            "enabled": enabled,
        }
        config["printers"].append(new_p)
        save_config()
        get_or_create_monitors()

    if enabled:
        monitors[pid].start()
    add_event("info", f"新增设备: {name}")
    return {"ok": True, "msg": f"设备 {name} 已添加", "printer": {"id": pid, "name": name}}


@app.delete("/api/printer/{identifier}")
@app.post("/api/printer/{identifier}/delete")
def api_delete_printer(identifier: str):
    idx, p = find_printer_index_and_data(identifier)
    if p is None:
        raise HTTPException(status_code=404, detail="未找到该设备")
    pid = p["id"]
    name = p["name"]
    with lock:
        if pid in monitors:
            try:
                monitors[pid].stop()
            except Exception:
                pass
            del monitors[pid]
        if pid in statuses:
            del statuses[pid]
        config["printers"] = [item for item in config["printers"] if item["id"] != pid]
        if not config["printers"]:
            # 保持至少 1 个空行
            config["printers"] = [{
                "id": "p_1", "name": "P2S_1", "host": "", "sn": "", "access_code": "", "enabled": False
            }]
            get_or_create_monitors()
        save_config()

    add_event("info", f"设备 {name} 已删除")
    return {"ok": True, "msg": f"设备 {name} 已删除"}


@app.post("/api/printer/{identifier}/toggle")
async def api_toggle_printer(identifier: str, req: Request):
    idx, p = find_printer_index_and_data(identifier)
    if p is None:
        raise HTTPException(status_code=404, detail="未找到该设备")
    body = await req.json()
    enabled = bool(body.get("enabled"))
    pid = p["id"]
    name = p["name"]
    with lock:
        if enabled and not (p["host"] and p["sn"] and p["access_code"]):
            return {"ok": False, "msg": "先填写该打印机的 IP / 序列号 / 访问码并保存"}
        p["enabled"] = enabled
        save_config()

    m = get_monitor_for_printer(pid)
    if enabled:
        m.restart()
    else:
        m.stop()
    add_event("info", f"{name} 已{'启动' if enabled else '停用'}监控")
    return {"ok": True, "msg": f"{name} 已{'启动' if enabled else '停用'}"}


@app.get("/api/status")
def api_status():
    now = time.time()
    out = []
    with lock:
        for idx, p in enumerate(config["printers"]):
            pid = p["id"]
            s = dict(get_printer_status(pid))
            out.append({
                "index": idx,
                "id": pid,
                "name": p["name"],
                "host": p["host"],
                "sn": p["sn"],
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
    running_count = sum(1 for x in out if x["gcode_state"] == "RUNNING")
    return {
        "printers": out,
        "total_count": len(out),
        "enabled_count": enabled_count,
        "online_count": online_count,
        "running_count": running_count,
        "version": __version__,
        "timezone": server_tz_label(),
    }


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


@app.post("/api/scan")
async def api_scan(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    subnet = str(body.get("subnet") or "").strip()
    res = scan_lan_printers(subnet_str=subnet)
    return res


@app.post("/api/test/printer")
async def api_test_printer(req: Request):
    body = await req.json()
    identifier = body.get("id") if body.get("id") is not None else body.get("index")
    idx, p = find_printer_index_and_data(identifier)
    if p is None:
        raise HTTPException(status_code=400, detail="无效的设备标识")
    pid = p["id"]
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
        jpg = grab_snapshot(pid, "test")
        if jpg:
            base = lan_base_url(p["host"])
            url = f"{base}/media/{jpg.name}" if base else None
            msg += "；摄像头抓图成功"
        else:
            msg += "；摄像头抓图失败（检查 RTSP / 固件）"
    return {"ok": True, "msg": msg, "url": url}


@app.post("/api/test/channel")
async def api_test_channel(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    channel = body.get("channel", "dingtalk")
    results = push_to_all_channels("测试消息", "这是 webhook-p2s 发出的测试消息。", force=True, channel=channel)
    if results:
        ok, msg = results[0]
        return {"ok": ok, "msg": msg}
    return {"ok": False, "msg": "测试失败"}


@app.post("/api/test/dingtalk")
def api_test_dingtalk():
    ok, msg = dingtalk_send("测试消息", "这是 webhook-p2s 发出的测试消息。", force=True)
    return {"ok": ok, "msg": msg}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
