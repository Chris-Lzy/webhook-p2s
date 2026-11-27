# -*- coding: utf-8 -*-
"""拓竹云端接口（参考 Bambu Studio / Handy 任务接口）：读取打印任务克数、耗材明细和封面图。"""

import base64
import datetime as dt
import difflib
import json
import re

import requests

BASES = {"cn": "https://api.bambulab.cn", "global": "https://api.bambulab.com"}
UA = "bambu_network_agent/01.09.05.01"


class CloudError(Exception):
    pass


class CloudAuthError(CloudError):
    """令牌无效或已过期，需要重新登录。"""


def _headers(token=None):
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "X-BBL-Client-Name": "BambuStudio",
        "X-BBL-Client-Type": "slicer",
        "X-BBL-Client-Version": "01.09.05.51",
        "X-BBL-OS-Type": "windows",
        "X-BBL-Language": "zh-CN",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _request(method, region, path, token=None, params=None, body=None, timeout=15):
    base = BASES.get(str(region or "cn").lower(), BASES["cn"])
    url = base + path
    try:
        resp = requests.request(
            method, url, headers=_headers(token), params=params, json=body, timeout=timeout,
        )
    except requests.RequestException as exc:
        raise CloudError(f"网络错误：{type(exc).__name__}: {exc}") from exc
    text = resp.text or ""
    if resp.status_code in (401, 403):
        if "<html" in text[:300].lower():
            raise CloudError(f"请求被网站防护拦截（HTTP {resp.status_code}），请稍后再试或改用粘贴令牌")
        if token:
            raise CloudAuthError(f"令牌无效或已过期（HTTP {resp.status_code}），请重新登录")
    if resp.status_code >= 400:
        raise CloudError(f"HTTP {resp.status_code}：{text[:200]}")
    try:
        return resp.json()
    except ValueError as exc:
        raise CloudError("云端返回的不是 JSON：" + text[:120]) from exc


def login(region, account, password):
    """账号密码登录，返回 {"token": ...} 或 {"need_code": True}。"""
    data = _request(
        "POST", region, "/v1/user-service/user/login",
        body={"account": account, "password": password},
    )
    if isinstance(data, dict) and data.get("accessToken"):
        return {"token": data["accessToken"]}
    if isinstance(data, dict) and data.get("loginType") == "verifyCode":
        return {"need_code": True}
    msg = data.get("message") if isinstance(data, dict) else ""
    raise CloudError("登录失败：" + (msg or json.dumps(data, ensure_ascii=False)[:200]))


def send_code(region, account):
    """发送邮箱登录验证码。"""
    _request(
        "POST", region, "/v1/user-service/user/sendemail/code",
        body={"email": account, "type": "codeLogin"},
    )


def login_with_code(region, account, code):
    """使用邮箱验证码完成登录并返回令牌。"""
    data = _request(
        "POST", region, "/v1/user-service/user/login",
        body={"account": account, "code": code},
    )
    if isinstance(data, dict) and data.get("accessToken"):
        return data["accessToken"]
    msg = data.get("message") if isinstance(data, dict) else ""
    raise CloudError("验证码登录失败：" + (msg or json.dumps(data, ensure_ascii=False)[:200]))


def jwt_expiry(token):
    """解析 JWT 过期时间（epoch 秒），解析失败返回 None。"""
    try:
        payload = str(token or "").split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload.encode())).get("exp")
        return float(exp) if exp else None
    except Exception:
        return None


def _ts(value):
    if value in (None, "", 0):
        return None
    if isinstance(value, (int, float)):
        return float(value) / (1000.0 if value > 1e11 else 1.0)
    text = str(value).strip().replace("Z", "+00:00")
    text = re.sub(r"(\.\d{1,6})\d*", lambda m: m.group(1).ljust(7, "0"), text)
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _color(value):
    text = str(value or "").lstrip("#")
    return "#" + text[:6].upper() if re.fullmatch(r"[0-9A-Fa-f]{6,8}", text) else ""


def _float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_task(raw):
    """将云端任务条目规范化为统一结构。"""
    fils = []
    ams = raw.get("amsDetailMapping") or raw.get("filaments") or []
    if isinstance(ams, list):
        for item in ams:
            if not isinstance(item, dict):
                continue
            weight = _float(item.get("weight"))
            if weight <= 0:
                continue
            fils.append({
                "type": str(item.get("filamentType") or item.get("type") or "").strip(),
                "color": _color(item.get("sourceColor") or item.get("color")),
                "g": weight,
                "tray_info_idx": str(item.get("filamentId") or item.get("tray_info_idx") or "").strip(),
            })
    return {
        "id": str(raw.get("id", "")),
        "title": raw.get("title") or "",
        "design_title": raw.get("designTitle") or "",
        "device_id": raw.get("deviceId") or "",
        "status": raw.get("status"),
        "weight": _float(raw.get("weight")),
        "length": _float(raw.get("length")),
        "cost_time": int(_float(raw.get("costTime"))),
        "start_ts": _ts(raw.get("startTime")),
        "end_ts": _ts(raw.get("endTime")),
        "cover": raw.get("cover") or raw.get("thumbnail") or "",
        "filaments": fils,
    }


def parse_tasks(payload):
    """兼容 hits / tasks / 列表等几种云端返回结构。"""
    raw = None
    if isinstance(payload, list):
        raw = payload
    elif isinstance(payload, dict):
        for key in ("hits", "tasks"):
            val = payload.get(key)
            if isinstance(val, list):
                raw = val
                break
            if isinstance(val, dict) and isinstance(val.get("hits"), list):
                raw = val["hits"]
                break
    return [normalize_task(item) for item in (raw or []) if isinstance(item, dict)]


def list_tasks(region, token, device_id=None, limit=20):
    params = {"limit": int(limit)}
    if device_id:
        params["deviceId"] = device_id
    data = _request("GET", region, "/v1/user-service/my/tasks", token=token, params=params)
    return parse_tasks(data)


def _sim(a, b):
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def match_task(tasks, sn, start_ts, end_ts, task_name, window=1800):
    """按设备 SN、起止时间和任务名相似度匹配云端任务。"""
    best, best_score = None, None
    for task in tasks:
        if task["device_id"] and sn and task["device_id"].upper() != sn.upper():
            continue
        diffs = []
        if task["start_ts"] and start_ts:
            diffs.append(abs(task["start_ts"] - start_ts))
        if task["end_ts"] and end_ts:
            diffs.append(abs(task["end_ts"] - end_ts))
        sim = max(_sim(task["title"], task_name), _sim(task["design_title"], task_name))
        if diffs:
            diff = min(diffs)
            if diff > window:
                continue
        elif sim >= 0.6:
            diff = 0.0
        else:
            continue
        score = diff - sim * 600.0
        if best_score is None or score < best_score:
            best, best_score = task, score
    return best


def filaments_from_task(task):
    """将云端任务转为 webhook-p2s 耗材列表；总克数以云端 weight 为准。"""
    if not isinstance(task, dict):
        return []
    total_w = _float(task.get("weight"))
    raw_fils = task.get("filaments") or []
    if raw_fils and total_w > 0 and abs(sum(_float(f.get("g")) for f in raw_fils) - total_w) <= max(0.5, total_w * 0.02):
        return [
            {
                "material": str(item.get("type") or "").strip(),
                "color": str(item.get("color") or "").strip(),
                "used_g": round(_float(item.get("g")), 4),
                "used_m": None,
                "tray_info_idx": str(item.get("tray_info_idx") or "").strip(),
                "filament_id": str(idx + 1),
            }
            for idx, item in enumerate(raw_fils)
            if _float(item.get("g")) > 0
        ]
    if total_w > 0:
        first = raw_fils[0] if len(raw_fils) == 1 else {}
        return [{
            "material": str(first.get("type") or "").strip(),
            "color": str(first.get("color") or "").strip(),
            "used_g": round(total_w, 4),
            "used_m": round(_float(task.get("length")) / 1000.0, 4) if _float(task.get("length")) > 0 else None,
            "tray_info_idx": str(first.get("tray_info_idx") or "").strip(),
            "filament_id": "1",
        }]
    return []


def download_cover(url, timeout=15):
    """下载云端封面图（签名 URL 不带 Authorization），返回 (bytes, ext)。"""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.content
    ext = "jpg" if data[:3] == b"\xff\xd8\xff" else "png"
    return data, ext


def lookup_job_metadata(region, token, sn, start_ts, end_ts, task_name, window=1800, want_cover=True):
    """从拓竹云端查询任务耗材与封面，返回 (task, filaments, cover_bytes, cover_ext, reason)。"""
    tasks = list_tasks(region, token, device_id=sn or None, limit=30)
    task = match_task(tasks, sn, start_ts, end_ts, task_name, window=window)
    if not task:
        return None, [], None, "", f"cloud_not_found({len(tasks)})"
    filaments = filaments_from_task(task)
    cover, cover_ext = None, ""
    if want_cover and task.get("cover"):
        try:
            cover, cover_ext = download_cover(task["cover"])
        except Exception:
            cover, cover_ext = None, ""
    reason = "cloud_ok" if filaments else "cloud_no_weight"
    return task, filaments, cover, cover_ext, reason
