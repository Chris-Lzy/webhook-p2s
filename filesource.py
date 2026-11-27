"""从打印机读取切片用量和摄像头静帧。失败时返回原因，不向外抛。"""

from __future__ import annotations

import ftplib
import logging
import re
import socket
import ssl
import zipfile
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from xml.etree import ElementTree

log = logging.getLogger("monitor.files")

MAX_DOWNLOAD = 40 * 1024 * 1024
GCODE_HEAD = 64 * 1024

_PLATE = re.compile(r"plate_(\d+)", re.IGNORECASE)
# Bambu Studio 把旧的 `; filament used [g] =` 注释掉了，实际写出的是
# `; total filament weight [g] : 32.18,4.27` 和 `; total filament length [mm] : ...`。
# 长度单位是毫米。两条都认，避免只匹配到 Prusa 风格的那一行。
_USED_G = re.compile(
    r"(?:total\s+filament\s+weight|filament\s+used)\s*\[g\]\s*[:=]\s*([0-9eE+.,\s-]+)",
    re.IGNORECASE,
)
_USED_MM = re.compile(
    r"(?:total\s+filament\s+length|filament\s+used)\s*\[mm\]\s*[:=]\s*([0-9eE+.,\s-]+)",
    re.IGNORECASE,
)


@dataclass
class FileFetch:
    ok: bool = False
    reason: str = ""
    filaments: list = field(default_factory=list)
    preview: bytes = b""
    preview_ext: str = ""


def unverified_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """隐式 FTPS（端口 990），数据连接复用控制连接的 TLS 会话。

    Python 3.12 的 ftplib.FTP_TLS.ntransfercmd 包数据连接时没有传入 session。
    vsftpd 会因此返回 522 SSL connection failed: session reuse required。
    """

    def __init__(self, context=None, timeout=12):
        super().__init__(context=context or unverified_context(), timeout=timeout)

    def connect(self, host="", port=0, timeout=-999, source_address=None):
        if host:
            self.host = host
        if port:
            self.port = port
        if timeout != -999:
            self.timeout = timeout
        if self.timeout is not None and not self.timeout:
            raise ValueError("Non-blocking socket (timeout=0) is not supported")
        if source_address is not None:
            self.source_address = source_address
        raw = socket.create_connection((self.host, self.port), self.timeout, source_address=self.source_address)
        self.sock = self.context.wrap_socket(raw, server_hostname=self.host)
        self.af = self.sock.family
        self.file = self.sock.makefile("r", encoding=self.encoding)
        self.welcome = self.getresp()
        return self.welcome

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=getattr(self.sock, "session", None),
            )
        return conn, size


def safe_error(exc, secret=""):
    text = str(exc)
    if secret:
        text = text.replace(secret, "***")
    return text[:240]


def _meaningful(names):
    out = []
    for name in names or []:
        text = str(name).strip().replace("\\", "/").rstrip("/")
        leaf = text.split("/")[-1]
        if leaf in {"", ".", ".."}:
            continue
        out.append(str(name).strip())
    return out


def _plate_index(gcode_file):
    match = _PLATE.search(str(gcode_file or ""))
    return match.group(1) if match else ""


def collect_from_ftp(ftp, gcode_file="", task_name=""):
    """在已经登录并 PROT P 的连接上取文件。目录为空或 550 时直接降级。"""
    try:
        listed = ftp.nlst()
    except ftplib.error_perm as exc:
        if str(exc).startswith("550"):
            return _try_direct(ftp, gcode_file, "list_denied")
        return FileFetch(ok=False, reason="list_failed")
    except ftplib.all_errors:
        return FileFetch(ok=False, reason="list_failed")

    names = _meaningful(listed)
    if not names:
        return FileFetch(ok=False, reason="empty_chroot")

    plate = _plate_index(gcode_file)
    candidates = _rank_candidates(names, task_name, gcode_file, plate)
    parsed = _download_candidates(ftp, candidates, plate)
    if parsed.filaments or parsed.preview:
        parsed.ok = bool(parsed.filaments)
        if not parsed.reason:
            parsed.reason = "exact" if parsed.filaments else "preview_only"
        return parsed
    direct = _try_direct(ftp, gcode_file, "no_metadata")
    if direct.filaments or direct.preview:
        return direct
    direct.reason = parsed.reason or direct.reason or "no_metadata"
    return direct


def _rank_candidates(names, task_name, gcode_file, plate):
    task = str(task_name or "").strip().lower()
    gcode_leaf = Path(str(gcode_file or "")).name.lower()
    ranked = []
    for name in names:
        leaf = name.replace("\\", "/").split("/")[-1].lower()
        score = 0
        if leaf.endswith(".3mf"):
            score += 50
        elif leaf.endswith(".gcode"):
            score += 10
        else:
            continue
        if task and task in leaf:
            score += 30
        if gcode_leaf and gcode_leaf == leaf:
            score += 40
        if plate and f"plate_{plate}" in leaf:
            score += 15
        ranked.append((score, name))
    ranked.sort(key=lambda item: -item[0])
    return [name for _score, name in ranked[:4]]


def _download_candidates(ftp, names, plate):
    result = FileFetch()
    for name in names:
        blob = _retr(ftp, name)
        if not blob:
            continue
        parsed = parse_slice_bytes(blob, plate)
        if parsed.preview and not result.preview:
            result.preview = parsed.preview
            result.preview_ext = parsed.preview_ext
        if parsed.filaments and not result.filaments:
            result.filaments = parsed.filaments
        if result.filaments and result.preview:
            break
    return result


def _try_direct(ftp, gcode_file, fallback_reason):
    path = str(gcode_file or "").strip()
    if not path or path in {".", ".."}:
        return FileFetch(ok=False, reason=fallback_reason)
    blob = _retr(ftp, path)
    if not blob:
        return FileFetch(ok=False, reason=fallback_reason if fallback_reason == "list_denied" else "download_failed")
    parsed = parse_slice_bytes(blob, _plate_index(path))
    parsed.ok = bool(parsed.filaments)
    parsed.reason = "exact" if parsed.filaments else "no_metadata"
    return parsed


def _retr(ftp, name):
    chunks = []
    total = 0

    def collect(data):
        nonlocal total
        total += len(data)
        if total > MAX_DOWNLOAD:
            raise RuntimeError("too_large")
        chunks.append(data)

    try:
        ftp.retrbinary("RETR " + name, collect)
    except Exception:
        return b""
    return b"".join(chunks)


def parse_slice_bytes(blob, plate=""):
    if not blob:
        return FileFetch()
    if blob[:2] == b"PK":
        return _parse_3mf(blob, plate)
    head = blob[:GCODE_HEAD]
    if head[:1] == b";" or b"filament" in head.lower():
        filaments = parse_gcode_header(head)
        return FileFetch(ok=bool(filaments), reason="exact" if filaments else "no_metadata", filaments=filaments)
    return FileFetch(reason="no_metadata")


def _parse_3mf(blob, plate=""):
    result = FileFetch()
    try:
        archive = zipfile.ZipFile(BytesIO(blob))
    except zipfile.BadZipFile:
        return FileFetch(reason="bad_zip")
    with archive:
        names = archive.namelist()
        xml_name = _pick_name(names, ("slice_info.config",))
        if xml_name:
            try:
                result.filaments = parse_slice_info_xml(archive.read(xml_name), plate)
            except Exception:
                result.filaments = []
        png_name = _pick_plate_image(names, plate)
        if png_name:
            try:
                data = archive.read(png_name)
            except Exception:
                data = b""
            if data[:8] == b"\x89PNG\r\n\x1a\n" or data[:2] == b"\xff\xd8":
                result.preview = data
                result.preview_ext = "png" if data[:2] != b"\xff\xd8" else "jpg"
        if not result.filaments:
            gcode_name = _pick_name(names, (f"plate_{plate}.gcode",) if plate else ()) or _pick_gcode(names, plate)
            if gcode_name:
                try:
                    head = archive.read(gcode_name)[:GCODE_HEAD]
                except Exception:
                    head = b""
                result.filaments = parse_gcode_header(head)
    result.ok = bool(result.filaments)
    result.reason = "exact" if result.filaments else ("preview_only" if result.preview else "no_metadata")
    return result


def _pick_name(names, needles):
    lowered = [(name, name.lower()) for name in names]
    for needle in needles:
        target = needle.lower()
        for name, low in lowered:
            if low.endswith(target):
                return name
    return ""


def _pick_gcode(names, plate):
    if plate:
        found = _pick_name(names, (f"plate_{plate}.gcode",))
        if found:
            return found
    for name in names:
        if name.lower().endswith(".gcode"):
            return name
    return ""


def _pick_plate_image(names, plate):
    if plate:
        found = _pick_name(names, (f"plate_{plate}.png", f"plate_{plate}.jpg", f"plate_{plate}.jpeg"))
        if found:
            return found
    for name in names:
        low = name.lower()
        if "/plate_" in low.replace("\\", "/") or low.startswith("plate_"):
            if low.endswith((".png", ".jpg", ".jpeg")):
                return name
    return ""


def parse_slice_info_xml(blob, plate=""):
    try:
        root = ElementTree.fromstring(blob)
    except ElementTree.ParseError:
        return []
    plates = []
    for node in root.iter():
        if _tag(node) != "plate":
            continue
        index = ""
        plate_weight = None
        filaments = []
        for child in list(node):
            if _tag(child) == "metadata":
                key = str(child.attrib.get("key") or "").strip().lower()
                val = str(child.attrib.get("value") or "").strip()
                if key == "index":
                    index = val
                elif key in ("weight", "filament_used_g", "total_weight") and plate_weight is None:
                    plate_weight = _first_float({"v": val}, ("v",))
            if _tag(child) == "filament":
                filaments.append(_filament_from_attrib(child.attrib))
        plates.append((index, [item for item in filaments if item], plate_weight))

    def _finalize(filaments, plate_weight):
        if filaments:
            missing_g = [item for item in filaments if item.get("used_g") is None or item.get("used_g") == 0]
            if plate_weight and plate_weight > 0 and len(missing_g) == len(filaments):
                if len(filaments) == 1:
                    filaments[0]["used_g"] = plate_weight
                else:
                    total_m = sum(item.get("used_m") or 0.0 for item in filaments)
                    if total_m > 0:
                        for item in filaments:
                            item["used_g"] = round(plate_weight * ((item.get("used_m") or 0.0) / total_m), 4)
            return filaments
        if plate_weight and plate_weight > 0:
            return [{
                "material": "",
                "color": "",
                "used_g": plate_weight,
                "used_m": None,
                "tray_info_idx": "",
                "filament_id": "1",
            }]
        return []

    if plate:
        for index, filaments, plate_weight in plates:
            if index == str(plate):
                return _finalize(filaments, plate_weight)
    for _index, filaments, plate_weight in plates:
        kept = _finalize(filaments, plate_weight)
        if kept:
            return kept
    return []


def _tag(node):
    text = node.tag
    if "}" in text:
        text = text.split("}", 1)[1]
    return text.lower()


def _filament_from_attrib(attrib):
    used_g = _first_float(attrib, ("used_g", "used_g_weight", "weight"))
    used_m = _first_float(attrib, ("used_m", "used_meter"))
    material = str(attrib.get("type") or attrib.get("tray_type") or "").strip()
    color = str(attrib.get("color") or attrib.get("tray_color") or "").strip()
    info = str(attrib.get("tray_info_idx") or "").strip()
    if used_g is None and used_m is None and not material:
        return None
    return {
        "material": material,
        "color": color,
        "used_g": used_g,
        "used_m": used_m,
        "tray_info_idx": info,
        "filament_id": str(attrib.get("id") or "").strip(),
    }


def _first_float(attrib, keys):
    for key in keys:
        if key in attrib and str(attrib.get(key)).strip() != "":
            try:
                return float(str(attrib.get(key)).strip())
            except ValueError:
                continue
    return None


def _split_numbers(text):
    out = []
    for part in str(text or "").replace(";", " ").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except ValueError:
            continue
    return out


def parse_gcode_header(blob):
    if isinstance(blob, bytes):
        text = blob.decode("utf-8", "ignore")
    else:
        text = str(blob or "")
    head = text[:GCODE_HEAD]
    grams = []
    meters = []
    match_g = _USED_G.search(head)
    if match_g:
        grams = _split_numbers(match_g.group(1))
    match_m = _USED_MM.search(head)
    if match_m:
        meters = [value / 1000.0 for value in _split_numbers(match_m.group(1))]
    count = max(len(grams), len(meters))
    rows = []
    for index in range(count):
        used_g = grams[index] if index < len(grams) else None
        used_m = meters[index] if index < len(meters) else None
        if (used_g is None or used_g == 0) and (used_m is None or used_m == 0):
            continue
        rows.append({
            "material": "",
            "color": "",
            "used_g": used_g,
            "used_m": used_m,
            "tray_info_idx": "",
            "filament_id": str(index + 1),
        })
    return rows


def fetch_print_metadata(host, access_code, gcode_file="", task_name="", port=990, timeout=12):
    """登录打印机 FTPS。登录或目录失败时返回原因，调用方继续记账。"""
    secret = str(access_code or "")
    if not host or not secret:
        return FileFetch(ok=False, reason="skipped_no_auth")
    ftp = ImplicitFTP_TLS(timeout=timeout)
    try:
        ftp.connect(host, port or 990, timeout=timeout)
        ftp.login("bblp", secret)
        ftp.prot_p()
        return collect_from_ftp(ftp, gcode_file=gcode_file, task_name=task_name)
    except Exception as exc:
        reason = safe_error(exc, secret)
        log.info("FTPS 读取失败: %s", reason)
        return FileFetch(ok=False, reason="connect_failed")
    finally:
        try:
            ftp.quit()
        except Exception:
            try:
                ftp.close()
            except Exception:
                pass


def capture_camera_still(host, access_code, dest: Path, timeout=8, allow_rtsp=True, http_get=None, rtsp_runner=None):
    """尽力抓一张静帧。RTSP 322 和 HTTP 6000 都失败时返回 False，不抛异常。"""
    secret = str(access_code or "")
    if not host or not secret:
        return False, "skipped_no_auth"
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if allow_rtsp:
        runner = rtsp_runner or _ffmpeg_snapshot
        try:
            if runner(host, secret, dest, timeout):
                return True, "rtsp"
        except Exception:
            pass
    getter = http_get or _http_snapshot
    for url in (
        f"http://{host}:6000/snapshot",
        f"http://{host}:6000/?action=snapshot",
        f"http://{host}:6000/jpeg",
    ):
        try:
            blob = getter(url, min(timeout, 3))
        except Exception:
            blob = b""
        if _looks_like_image(blob):
            dest.write_bytes(blob)
            return True, "http_6000"
    if dest.exists() and dest.stat().st_size == 0:
        try:
            dest.unlink()
        except OSError:
            pass
    return False, "unavailable"


def _looks_like_image(blob):
    return bool(blob) and (blob[:2] == b"\xff\xd8" or blob[:8] == b"\x89PNG\r\n\x1a\n")


def _ffmpeg_snapshot(host, access_code, dest, timeout):
    import shutil
    import subprocess
    import urllib.parse

    if not shutil.which("ffmpeg"):
        return False
    code = urllib.parse.quote(str(access_code), safe="")
    url = f"rtsp://bblp:{code}@{host}:322/stream"
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-rtsp_transport", "tcp", "-i", url,
        "-frames:v", "1", "-q:v", "3", str(dest),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except Exception:
        return False
    return result.returncode == 0 and dest.exists() and dest.stat().st_size > 0


def _http_snapshot(url, timeout):
    import requests

    try:
        response = requests.get(url, timeout=timeout)
    except Exception:
        return b""
    content = response.content or b""
    if _looks_like_image(content):
        return content
    return b""
