"""耗材、装填、任务和图片的 SQLite 存储。配置 JSON 里不放这些会持续增长的数据。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from filament import (
    aggregate_by,
    aggregate_models,
    aggregate_periods,
    aggregate_spools,
    aggregate_usage_axis,
    effective_remain_pct,
    low_remain_decision,
    match_exact_filaments,
    normalize_color,
    official_identity,
    parse_float,
    select_estimated_usage,
    summarize_costs,
    trace_model,
    trace_spool,
    usage_source_of,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS spools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    tag_uid TEXT NOT NULL DEFAULT '',
    tray_uuid TEXT NOT NULL DEFAULT '',
    brand TEXT NOT NULL DEFAULT '',
    material TEXT NOT NULL DEFAULT '',
    color TEXT NOT NULL DEFAULT '',
    diameter REAL,
    nominal_weight_g REAL,
    unit_price_per_kg REAL,
    currency TEXT NOT NULL DEFAULT 'CNY',
    purchase_date TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    nozzle_temp_min REAL,
    nozzle_temp_max REAL,
    drying_temp REAL,
    drying_time REAL,
    remain_pct REAL,
    remain_g REAL,
    total_len_mm REAL,
    tray_id_name TEXT NOT NULL DEFAULT '',
    tray_info_idx TEXT NOT NULL DEFAULT '',
    sub_brands TEXT NOT NULL DEFAULT '',
    bound_printer_id TEXT NOT NULL DEFAULT '',
    bound_slot TEXT NOT NULL DEFAULT '',
    last_printer_id TEXT NOT NULL DEFAULT '',
    last_printer_name TEXT NOT NULL DEFAULT '',
    last_slot TEXT NOT NULL DEFAULT '',
    last_seen_ts REAL,
    low_remain_alerted INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_spool_identity
    ON spools(tag_uid, tray_uuid)
    WHERE tag_uid != '' AND tray_uuid != '';
CREATE TABLE IF NOT EXISTS spool_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spool_id INTEGER,
    event_type TEXT NOT NULL,
    ts REAL NOT NULL,
    printer_id TEXT NOT NULL DEFAULT '',
    printer_name TEXT NOT NULL DEFAULT '',
    slot TEXT NOT NULL DEFAULT '',
    job_id INTEGER,
    grams REAL,
    remain_pct REAL,
    note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_spool_events_spool ON spool_events(spool_id, ts);
CREATE TABLE IF NOT EXISTS loads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    printer_id TEXT NOT NULL,
    printer_name TEXT NOT NULL DEFAULT '',
    ams_id TEXT NOT NULL DEFAULT '',
    slot TEXT NOT NULL,
    spool_id INTEGER,
    tag_uid TEXT NOT NULL DEFAULT '',
    tray_uuid TEXT NOT NULL DEFAULT '',
    prev_tag_uid TEXT NOT NULL DEFAULT '',
    prev_tray_uuid TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    material TEXT NOT NULL DEFAULT '',
    color TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_loads_ts ON loads(ts);
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    printer_id TEXT NOT NULL,
    printer_name TEXT NOT NULL DEFAULT '',
    task_name TEXT NOT NULL DEFAULT '',
    gcode_file TEXT NOT NULL DEFAULT '',
    printer_job_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'running',
    start_ts REAL,
    end_ts REAL,
    start_incomplete INTEGER NOT NULL DEFAULT 0,
    layer_num INTEGER,
    total_layer_num INTEGER,
    percent INTEGER,
    duration_sec REAL,
    preview_path TEXT NOT NULL DEFAULT '',
    camera_path TEXT NOT NULL DEFAULT '',
    material_cost REAL,
    cost_complete INTEGER NOT NULL DEFAULT 0,
    partial_cost REAL,
    electricity_cost REAL,
    labor_cost REAL,
    electricity_rate REAL,
    labor_rate REAL,
    usage_source TEXT NOT NULL DEFAULT 'unknown',
    file_status TEXT NOT NULL DEFAULT '',
    camera_status TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    active_slot TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_printer ON jobs(printer_id, end_ts);
CREATE INDEX IF NOT EXISTS idx_jobs_task ON jobs(task_name);
CREATE TABLE IF NOT EXISTS job_baselines (
    job_id INTEGER NOT NULL,
    slot TEXT NOT NULL,
    spool_id INTEGER,
    tag_uid TEXT NOT NULL DEFAULT '',
    tray_uuid TEXT NOT NULL DEFAULT '',
    material TEXT NOT NULL DEFAULT '',
    color TEXT NOT NULL DEFAULT '',
    remain_pct REAL,
    remain_g REAL,
    remain_g_fresh INTEGER NOT NULL DEFAULT 0,
    tray_weight_g REAL,
    tray_info_idx TEXT NOT NULL DEFAULT '',
    occupied INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, slot)
);
CREATE TABLE IF NOT EXISTS job_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    spool_id INTEGER,
    tag_uid TEXT NOT NULL DEFAULT '',
    tray_uuid TEXT NOT NULL DEFAULT '',
    slot TEXT NOT NULL DEFAULT '',
    material TEXT NOT NULL DEFAULT '',
    color TEXT NOT NULL DEFAULT '',
    grams REAL,
    meters REAL,
    source TEXT NOT NULL,
    remain_before REAL,
    remain_after REAL,
    tray_weight_g REAL,
    note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_usage_job ON job_usage(job_id);
CREATE INDEX IF NOT EXISTS idx_usage_spool ON job_usage(spool_id);
"""


def _row(row):
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


class Store:
    def __init__(self, db_path, image_dir):
        self.path = Path(db_path)
        self.image_dir = Path(image_dir)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self._ensure_column("spools", "remain_g", "REAL")
        self._ensure_column("job_baselines", "remain_g", "REAL")
        self._ensure_column("job_baselines", "remain_g_fresh", "INTEGER NOT NULL DEFAULT 0")
        self.conn.commit()

    def _ensure_column(self, table, column, ddl):
        cols = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def close(self):
        with self._lock:
            self.conn.close()

    def create_manual(self, fields):
        material = str(fields.get("material") or "").strip()
        brand = str(fields.get("brand") or "").strip()
        weight = parse_float(fields.get("nominal_weight_g"))
        if not material and not brand:
            raise ValueError("请填写材料或品牌")
        if weight is None or weight <= 0:
            raise ValueError("请填写净重（克）")
        price = _optional_price(fields.get("unit_price_per_kg"))
        printer_id = str(fields.get("bound_printer_id") or "").strip()
        slot = str(fields.get("bound_slot") or "").strip()
        _check_bind(printer_id, slot)
        now = time.time()
        color = normalize_color(fields.get("color"))
        with self._lock:
            cur = self.conn.execute(
                """INSERT INTO spools (
                    source, brand, material, color, diameter, nominal_weight_g,
                    unit_price_per_kg, currency, purchase_date, notes,
                    bound_printer_id, bound_slot, created_at, updated_at
                ) VALUES ('manual', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    brand, material, color, parse_float(fields.get("diameter")) or 1.75,
                    weight, price, _currency(fields.get("currency")),
                    str(fields.get("purchase_date") or "").strip()[:10],
                    str(fields.get("notes") or "").strip(),
                    printer_id, slot,
                    now, now,
                ),
            )
            spool_id = cur.lastrowid
            self._event(spool_id, "created", note="手工录入")
            self._clear_competing_binds(spool_id)
            self.conn.commit()
            return self._spool(spool_id)

    def update_spool(self, spool_id, fields):
        with self._lock:
            current = self._spool(spool_id)
            if not current:
                raise ValueError("耗材不存在")
            sets = []
            args = []
            price_changed = False
            for key, column in (
                ("brand", "brand"),
                ("material", "material"),
                ("notes", "notes"),
                ("purchase_date", "purchase_date"),
                ("currency", "currency"),
            ):
                if key in fields:
                    value = str(fields.get(key) or "").strip()
                    if key == "currency":
                        value = _currency(value)
                    if key == "purchase_date":
                        value = value[:10]
                    sets.append(f"{column}=?")
                    args.append(value)
            if "color" in fields:
                sets.append("color=?")
                args.append(normalize_color(fields.get("color")))
            if "diameter" in fields:
                sets.append("diameter=?")
                args.append(parse_float(fields.get("diameter")))
            if "nominal_weight_g" in fields:
                weight = parse_float(fields.get("nominal_weight_g"))
                if weight is None or weight <= 0:
                    raise ValueError("净重（克）需要大于 0")
                sets.append("nominal_weight_g=?")
                args.append(weight)
            if "unit_price_per_kg" in fields:
                sets.append("unit_price_per_kg=?")
                args.append(_optional_price(fields.get("unit_price_per_kg")))
                price_changed = True
            if "bound_printer_id" in fields or "bound_slot" in fields:
                raw_printer = fields.get("bound_printer_id") if "bound_printer_id" in fields else current["bound_printer_id"]
                raw_slot = fields.get("bound_slot") if "bound_slot" in fields else current["bound_slot"]
                printer_id = str(raw_printer or "").strip()
                slot = str(raw_slot or "").strip()
                _check_bind(printer_id, slot)
                sets.append("bound_printer_id=?")
                args.append(printer_id)
                sets.append("bound_slot=?")
                args.append(slot)
            if not sets:
                return current
            sets.append("updated_at=?")
            args.append(time.time())
            args.append(spool_id)
            self.conn.execute(f"UPDATE spools SET {', '.join(sets)} WHERE id=?", args)
            if "bound_printer_id" in fields or "bound_slot" in fields:
                self._clear_competing_binds(spool_id)
            if price_changed:
                # 事件类型本身会显示成「更新单价」，备注再写一遍就会在历史上出现两次。
                self._event(spool_id, "price_set", note="")
                self._recompute_spool_jobs(spool_id)
            self.conn.commit()
            return self._spool(spool_id)

    def list_spools(self, query=""):
        with self._lock:
            rows = [dict(item) for item in self._spool_rows()]
        needle = str(query or "").strip().lower()
        if not needle:
            return rows
        kept = []
        for row in rows:
            blob = " ".join(str(row.get(key) or "") for key in (
                "brand", "material", "color", "tag_uid", "tray_uuid", "notes", "tray_id_name", "sub_brands",
            )).lower()
            if needle in blob:
                kept.append(row)
        return kept

    def get_spool(self, spool_id):
        with self._lock:
            spool = self._spool(spool_id)
            if not spool:
                return None
            events = [
                _row(row) for row in self.conn.execute(
                    "SELECT * FROM spool_events WHERE spool_id=? ORDER BY ts DESC, id DESC LIMIT 200",
                    (spool_id,),
                )
            ]
            loads = [
                _row(row) for row in self.conn.execute(
                    "SELECT * FROM loads WHERE spool_id=? ORDER BY ts DESC, id DESC LIMIT 100",
                    (spool_id,),
                )
            ]
        spool["events"] = events
        spool["loads"] = loads
        traced = trace_spool(spool_id, self._jobs_for_reports())
        spool["models"] = traced["models"]
        return spool

    def apply_observation(self, printer_id, printer_name, dirty_slots, changes, threshold, enabled):
        alerts = []
        with self._lock:
            for slot in dirty_slots or []:
                spool_id = self._link_slot(slot, printer_id, printer_name)
                slot["spool_id"] = spool_id
                if enabled and spool_id:
                    alert = self._consider_low_remain(spool_id, slot, printer_id, printer_name, threshold)
                    if alert:
                        alerts.append(alert)
            for change in changes or []:
                curr = change["curr"]
                if not curr.get("spool_id"):
                    curr["spool_id"] = self._link_slot(curr, printer_id, printer_name)
                self._insert_load(printer_id, printer_name, change)
            self.conn.commit()
        return {"alerts": alerts}

    def open_job(self, printer_id, printer_name, task_name, gcode_file, slots, active_slot,
                 layer_num, total_layer_num, percent, printer_job_id="", start_incomplete=False):
        with self._lock:
            existing = self._open_job_id(printer_id)
            if existing:
                self._touch(existing, task_name, gcode_file, layer_num, total_layer_num, percent, printer_job_id)
                self.conn.commit()
                return existing
            now = time.time()
            note = "服务上线时任务已在进行，开始时间不是装填或开打的真实时刻。" if start_incomplete else ""
            cur = self.conn.execute(
                """INSERT INTO jobs (
                    printer_id, printer_name, task_name, gcode_file, printer_job_id, state,
                    start_ts, start_incomplete, layer_num, total_layer_num, percent,
                    active_slot, note, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    printer_id, printer_name or "", str(task_name or "").strip(), str(gcode_file or ""),
                    str(printer_job_id or ""), now, 1 if start_incomplete else 0,
                    _int_or_none(layer_num), _int_or_none(total_layer_num), _int_or_none(percent),
                    active_slot or "", note, now,
                ),
            )
            job_id = cur.lastrowid
            self._write_baselines(job_id, slots)
            self.conn.commit()
            return job_id

    def touch_job(self, printer_id, task_name, gcode_file, layer_num, total_layer_num, percent, printer_job_id=""):
        with self._lock:
            job_id = self._open_job_id(printer_id)
            if not job_id:
                return None
            self._touch(job_id, task_name, gcode_file, layer_num, total_layer_num, percent, printer_job_id)
            self.conn.commit()
            return job_id

    def close_job(self, printer_id, state, after_slots, active_slot, electricity_per_hour, labor_per_hour):
        with self._lock:
            job_id = self._open_job_id(printer_id)
            if not job_id:
                return None
            job = _row(self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
            existing = self._usage_rows(job_id)
            mapped = {
                "finished": "finished",
                "failed": "failed",
                "cancelled": "cancelled",
            }.get(state, state or "finished")
            has_exact = any(row["source"] == "exact" for row in existing)
            if not has_exact:
                baselines = self._baseline_slots(job_id)
                estimated = select_estimated_usage(baselines, after_slots or [], active_slot or job.get("active_slot"))
                self._replace_usage(job_id, estimated)
            elif mapped in ("failed", "cancelled"):
                # 失败/取消任务不能按整盘切片重量记账：优先用实测余量差，否则按完成百分比折算。
                baselines = self._baseline_slots(job_id)
                estimated = select_estimated_usage(baselines, after_slots or [], active_slot or job.get("active_slot"))
                if any((row.get("grams") or 0) > 0 for row in estimated):
                    self._replace_usage(job_id, estimated)
                else:
                    pct = parse_float(job.get("percent"))
                    ratio = max(0.0, min(1.0, pct / 100.0)) if pct is not None and pct > 0 else 0.0
                    scaled = []
                    for row in existing:
                        g = parse_float(row.get("grams"))
                        m = parse_float(row.get("meters"))
                        scaled.append({
                            "slot": row.get("slot") or "",
                            "spool_id": row.get("spool_id"),
                            "tag_uid": row.get("tag_uid") or "",
                            "tray_uuid": row.get("tray_uuid") or "",
                            "material": row.get("material") or "",
                            "color": row.get("color") or "",
                            "grams": round(g * ratio, 4) if g is not None else None,
                            "meters": round(m * ratio, 4) if m is not None else None,
                            "source": "estimated" if g is not None else "unknown",
                            "remain_before": row.get("remain_before"),
                            "remain_after": row.get("remain_after"),
                            "tray_weight_g": row.get("tray_weight_g"),
                        })
                    self._replace_usage(job_id, scaled)
            end_ts = time.time()
            duration = None
            if job.get("start_ts"):
                duration = max(0.0, end_ts - float(job["start_ts"]))
            self.conn.execute(
                """UPDATE jobs SET state=?, end_ts=?, duration_sec=?, electricity_rate=?, labor_rate=?,
                   active_slot=COALESCE(NULLIF(?, ''), active_slot), updated_at=? WHERE id=?""",
                (mapped, end_ts, duration, parse_float(electricity_per_hour), parse_float(labor_per_hour),
                 active_slot or "", end_ts, job_id),
            )
            self._write_cost(job_id)
            self.conn.commit()
            return self.get_job(job_id)

    def apply_file_fetch(self, job_id, filaments, preview, preview_ext, reason, slots):
        with self._lock:
            job = _row(self.conn.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone())
            if not job:
                return None
            if filaments:
                bases = self._baseline_slots(job_id) or list(slots or [])
                self._replace_usage(job_id, match_exact_filaments(filaments, bases))
            if preview:
                ext = "jpg" if preview_ext == "jpg" else "png"
                name = f"job{job_id}_preview.{ext}"
                (self.image_dir / name).write_bytes(preview)
                self.conn.execute(
                    "UPDATE jobs SET preview_path=?, updated_at=? WHERE id=?",
                    (name, time.time(), job_id),
                )
            self.conn.execute(
                "UPDATE jobs SET file_status=?, updated_at=? WHERE id=?",
                (reason or "", time.time(), job_id),
            )
            self._write_cost(job_id)
            self.conn.commit()
            return self.get_job(job_id)

    def attach_camera(self, job_id, filename, status):
        with self._lock:
            self.conn.execute(
                "UPDATE jobs SET camera_path=?, camera_status=?, updated_at=? WHERE id=?",
                (Path(filename).name if filename else "", status or "", time.time(), job_id),
            )
            self.conn.commit()

    def mark_camera(self, job_id, status):
        with self._lock:
            self.conn.execute(
                "UPDATE jobs SET camera_status=?, updated_at=? WHERE id=?",
                (status or "", time.time(), job_id),
            )
            self.conn.commit()

    def mark_file(self, job_id, status):
        with self._lock:
            self.conn.execute(
                "UPDATE jobs SET file_status=?, updated_at=? WHERE id=?",
                (status or "", time.time(), job_id),
            )
            self.conn.commit()

    def list_jobs(self, limit=200):
        with self._lock:
            return self._jobs_for_reports(limit=limit)

    def get_job(self, job_id):
        with self._lock:
            jobs = self._jobs_for_reports()
        for job in jobs:
            if job["id"] == job_id:
                return job
        return None

    def list_loads(self, limit=200):
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM loads ORDER BY ts DESC, id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [_row(row) for row in rows]

    def reports(self, tz_name="Asia/Shanghai"):
        jobs = self._jobs_for_reports(limit=2000)
        with self._lock:
            spools = [dict(item) for item in self._spool_rows()]
        return {
            "models": aggregate_models(jobs),
            "spools": aggregate_spools(spools, jobs),
            "materials": aggregate_usage_axis(jobs, "material"),
            "colors": aggregate_usage_axis(jobs, "color"),
            "printers": aggregate_by(jobs, lambda job: job.get("printer_name") or job.get("printer_id") or "未知打印机"),
            "days": aggregate_periods(jobs, "day", tz_name),
            "months": aggregate_periods(jobs, "month", tz_name),
        }

    def trace(self, task_name="", spool_id=None, tz_name="Asia/Shanghai"):
        jobs = self._jobs_for_reports(limit=2000)
        return {
            "model": trace_model(task_name, jobs) if task_name else None,
            "spool": trace_spool(int(spool_id), jobs) if spool_id else None,
        }

    def recompute_rates(self, electricity_per_hour, labor_per_hour):
        with self._lock:
            rows = self.conn.execute("SELECT id FROM jobs").fetchall()
            for row in rows:
                self.conn.execute(
                    "UPDATE jobs SET electricity_rate=?, labor_rate=? WHERE id=?",
                    (parse_float(electricity_per_hour), parse_float(labor_per_hour), row["id"]),
                )
                self._write_cost(row["id"])
            self.conn.commit()

    def prune_images(self, max_age_days, max_count):
        removed = []
        now = time.time()
        files = [path for path in self.image_dir.glob("*") if path.is_file()]
        if max_age_days and float(max_age_days) > 0:
            cutoff = now - float(max_age_days) * 86400
            for path in files:
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink()
                        removed.append(path.name)
                except OSError:
                    continue
        files = [path for path in self.image_dir.glob("*") if path.is_file()]
        files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        if max_count and int(max_count) > 0:
            for path in files[int(max_count):]:
                try:
                    path.unlink()
                    removed.append(path.name)
                except OSError:
                    continue
        if removed:
            with self._lock:
                names = set(removed)
                for row in self.conn.execute("SELECT id, preview_path, camera_path FROM jobs"):
                    preview = "" if row["preview_path"] in names else row["preview_path"]
                    camera = "" if row["camera_path"] in names else row["camera_path"]
                    if preview != row["preview_path"] or camera != row["camera_path"]:
                        self.conn.execute(
                            "UPDATE jobs SET preview_path=?, camera_path=? WHERE id=?",
                            (preview, camera, row["id"]),
                        )
                self.conn.commit()
        return removed

    def _spool_rows(self):
        return self.conn.execute("SELECT * FROM spools ORDER BY updated_at DESC, id DESC").fetchall()

    def _spool(self, spool_id):
        return _row(self.conn.execute("SELECT * FROM spools WHERE id=?", (spool_id,)).fetchone())

    def _event(self, spool_id, event_type, printer_id="", printer_name="", slot="", job_id=None,
               grams=None, remain_pct=None, note=""):
        self.conn.execute(
            """INSERT INTO spool_events (
                spool_id, event_type, ts, printer_id, printer_name, slot, job_id, grams, remain_pct, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (spool_id, event_type, time.time(), printer_id or "", printer_name or "", slot or "",
             job_id, grams, remain_pct, note or ""),
        )

    def _clear_competing_binds(self, spool_id):
        row = self._spool(spool_id)
        if not row or not row["bound_printer_id"] or not row["bound_slot"]:
            return
        self.conn.execute(
            """UPDATE spools SET bound_printer_id='', bound_slot='', updated_at=?
               WHERE bound_printer_id=? AND bound_slot=? AND id!=?""",
            (time.time(), row["bound_printer_id"], row["bound_slot"], spool_id),
        )

    def _link_slot(self, slot, printer_id, printer_name):
        identity = official_identity(slot.get("tag_uid"), slot.get("tray_uuid"))
        now = time.time()
        if identity and slot.get("occupied"):
            tag, uuid = identity
            found = self.conn.execute(
                "SELECT * FROM spools WHERE tag_uid=? AND tray_uuid=?",
                (tag, uuid),
            ).fetchone()
            if found:
                spool_id = found["id"]
                self._refresh_official(found, slot, printer_id, printer_name, now)
            else:
                spool_id = self._insert_official(slot, printer_id, printer_name, now)
            return spool_id
        if slot.get("occupied"):
            bound = self.conn.execute(
                """SELECT id FROM spools
                   WHERE source='manual' AND bound_printer_id=? AND bound_slot=?
                   ORDER BY updated_at DESC LIMIT 1""",
                (printer_id, slot["slot"]),
            ).fetchone()
            if bound:
                self.conn.execute(
                    """UPDATE spools SET last_printer_id=?, last_printer_name=?, last_slot=?, last_seen_ts=?,
                       material=CASE WHEN material='' THEN ? ELSE material END,
                       color=CASE WHEN color='' THEN ? ELSE color END,
                       updated_at=? WHERE id=?""",
                    (printer_id, printer_name, slot["slot"], now, slot.get("material") or "",
                     slot.get("color") or "", now, bound["id"]),
                )
                return bound["id"]
        return None

    def _insert_official(self, slot, printer_id, printer_name, now):
        cur = self.conn.execute(
            """INSERT INTO spools (
                source, tag_uid, tray_uuid, brand, material, color, diameter, nominal_weight_g,
                currency, nozzle_temp_min, nozzle_temp_max, drying_temp, drying_time, remain_pct, remain_g,
                total_len_mm, tray_id_name, tray_info_idx, sub_brands,
                last_printer_id, last_printer_name, last_slot, last_seen_ts, created_at, updated_at
            ) VALUES (
                'rfid', ?, ?, ?, ?, ?, ?, ?, 'CNY', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )""",
            (
                slot.get("tag_uid") or "", slot.get("tray_uuid") or "",
                slot.get("sub_brands") or "", slot.get("material") or "", slot.get("color") or "",
                slot.get("diameter"), slot.get("nominal_weight_g"),
                slot.get("nozzle_temp_min"), slot.get("nozzle_temp_max"),
                slot.get("drying_temp"), slot.get("drying_time"), slot.get("remain_pct"), slot.get("remain_g"),
                slot.get("total_len_mm"), slot.get("tray_id_name") or "", slot.get("tray_info_idx") or "",
                slot.get("sub_brands") or "", printer_id, printer_name, slot["slot"], now, now, now,
            ),
        )
        spool_id = cur.lastrowid
        self._event(spool_id, "created", printer_id, printer_name, slot["slot"], note="AMS 上报官方标签，自动建档")
        return spool_id

    def _refresh_official(self, found, slot, printer_id, printer_name, now):
        brand = found["brand"] or slot.get("sub_brands") or ""
        old_remain = found["remain_pct"]
        new_remain = slot.get("remain_pct")
        new_grams = slot.get("remain_g")
        old_grams = found["remain_g"] if "remain_g" in found.keys() else None
        self.conn.execute(
            """UPDATE spools SET brand=?, material=?, color=?, diameter=?, nominal_weight_g=?,
               nozzle_temp_min=?, nozzle_temp_max=?, drying_temp=?, drying_time=?, remain_pct=?, remain_g=?,
               total_len_mm=?, tray_id_name=?, tray_info_idx=?, sub_brands=?,
               last_printer_id=?, last_printer_name=?, last_slot=?, last_seen_ts=?, updated_at=?
               WHERE id=?""",
            (
                brand, slot.get("material") or found["material"], slot.get("color") or found["color"],
                slot.get("diameter") if slot.get("diameter") is not None else found["diameter"],
                slot.get("nominal_weight_g") if slot.get("nominal_weight_g") is not None else found["nominal_weight_g"],
                slot.get("nozzle_temp_min"), slot.get("nozzle_temp_max"),
                slot.get("drying_temp"), slot.get("drying_time"),
                new_remain if new_remain is not None else old_remain,
                new_grams if new_grams is not None else old_grams,
                slot.get("total_len_mm") if slot.get("total_len_mm") is not None else found["total_len_mm"],
                slot.get("tray_id_name") or found["tray_id_name"],
                slot.get("tray_info_idx") or found["tray_info_idx"],
                slot.get("sub_brands") or found["sub_brands"],
                printer_id, printer_name, slot["slot"], now, now, found["id"],
            ),
        )
        if new_remain is not None and new_remain >= 0 and (old_remain is None or int(old_remain) != int(new_remain)):
            self._event(found["id"], "remain", printer_id, printer_name, slot["slot"], remain_pct=new_remain)

    def _consider_low_remain(self, spool_id, slot, printer_id, printer_name, threshold):
        spool = self._spool(spool_id)
        if not spool:
            return None
        action, flagged = low_remain_decision(effective_remain_pct(slot), threshold, spool["low_remain_alerted"])
        if action == "skip":
            return None
        if int(bool(flagged)) != int(spool["low_remain_alerted"]):
            self.conn.execute(
                "UPDATE spools SET low_remain_alerted=? WHERE id=?",
                (1 if flagged else 0, spool_id),
            )
        if action != "alert":
            return None
        shown = effective_remain_pct(slot)
        self._event(spool_id, "low_remain", printer_id, printer_name, slot["slot"], remain_pct=shown)
        return {
            "spool_id": spool_id,
            "material": spool["material"] or slot.get("material") or "",
            "color": spool["color"] or slot.get("color") or "",
            "remain_pct": shown,
            "slot": slot["slot"],
            "tag_uid": spool["tag_uid"],
            "tray_uuid": spool["tray_uuid"],
            "printer_name": printer_name,
            "threshold": threshold,
        }

    def open_job_id(self, printer_id):
        with self._lock:
            return self._open_job_id(printer_id)

    def recompute_job(self, job_id, electricity_per_hour, labor_per_hour):
        with self._lock:
            found = self.conn.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not found:
                return None
            self.conn.execute(
                "UPDATE jobs SET electricity_rate=?, labor_rate=? WHERE id=?",
                (parse_float(electricity_per_hour), parse_float(labor_per_hour), job_id),
            )
            self._write_cost(job_id)
            self.conn.commit()
        return self.get_job(job_id)

    def _insert_load(self, printer_id, printer_name, change):
        curr = change["curr"]
        prev = change.get("prev") or {}
        now = time.time()
        self.conn.execute(
            """INSERT INTO loads (
                ts, printer_id, printer_name, ams_id, slot, spool_id, tag_uid, tray_uuid,
                prev_tag_uid, prev_tray_uuid, reason, material, color
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                now, printer_id, printer_name, curr.get("ams_id") or "", curr["slot"], curr.get("spool_id"),
                curr.get("tag_uid") or "", curr.get("tray_uuid") or "",
                prev.get("tag_uid") or "", prev.get("tray_uuid") or "", change["reason"],
                curr.get("material") or "", curr.get("color") or "",
            ),
        )
        if curr.get("spool_id"):
            self._event(
                curr["spool_id"], "loaded", printer_id, printer_name, curr["slot"],
                note="空槽变为有料" if change["reason"] == "empty_to_occupied" else "标签或 UUID 变化",
            )

    def _open_job_id(self, printer_id):
        row = self.conn.execute(
            "SELECT id FROM jobs WHERE printer_id=? AND end_ts IS NULL ORDER BY id DESC LIMIT 1",
            (printer_id,),
        ).fetchone()
        return row["id"] if row else None

    def _touch(self, job_id, task_name, gcode_file, layer_num, total_layer_num, percent, printer_job_id):
        job = self.conn.execute("SELECT task_name, gcode_file, printer_job_id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            return
        task = str(task_name or "").strip()
        if job["task_name"] and task in {"", "未知任务"}:
            task = job["task_name"]
        gcode = str(gcode_file or "").strip() or job["gcode_file"]
        remote_id = str(printer_job_id or "").strip() or job["printer_job_id"]
        self.conn.execute(
            """UPDATE jobs SET task_name=?, gcode_file=?, printer_job_id=?, layer_num=?, total_layer_num=?,
               percent=?, updated_at=? WHERE id=?""",
            (task, gcode, remote_id, _int_or_none(layer_num), _int_or_none(total_layer_num),
             _int_or_none(percent), time.time(), job_id),
        )

    def _write_baselines(self, job_id, slots):
        for slot in slots or []:
            if not slot.get("occupied"):
                continue
            self.conn.execute(
                """INSERT OR REPLACE INTO job_baselines (
                    job_id, slot, spool_id, tag_uid, tray_uuid, material, color, remain_pct, remain_g,
                    remain_g_fresh, tray_weight_g, tray_info_idx, occupied
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    job_id, slot["slot"], slot.get("spool_id"), slot.get("tag_uid") or "",
                    slot.get("tray_uuid") or "", slot.get("material") or "", slot.get("color") or "",
                    slot.get("remain_pct"), slot.get("remain_g"), 1 if slot.get("remain_g_fresh") else 0,
                    slot.get("nominal_weight_g"), slot.get("tray_info_idx") or "",
                ),
            )

    def seed_baselines_if_empty(self, job_id, slots):
        with self._lock:
            count = self.conn.execute(
                "SELECT COUNT(*) AS n FROM job_baselines WHERE job_id=?",
                (job_id,),
            ).fetchone()["n"]
            if count:
                return False
            self._write_baselines(job_id, slots)
            self.conn.commit()
            return True

    def _baseline_slots(self, job_id):
        rows = self.conn.execute("SELECT * FROM job_baselines WHERE job_id=?", (job_id,)).fetchall()
        out = []
        for row in rows:
            out.append({
                "slot": row["slot"],
                "occupied": bool(row["occupied"]),
                "spool_id": row["spool_id"],
                "tag_uid": row["tag_uid"],
                "tray_uuid": row["tray_uuid"],
                "material": row["material"],
                "color": row["color"],
                "remain_pct": row["remain_pct"],
                "remain_g": row["remain_g"] if "remain_g" in row.keys() else None,
                "remain_g_fresh": bool(row["remain_g_fresh"]) if "remain_g_fresh" in row.keys() else False,
                "nominal_weight_g": row["tray_weight_g"],
                "tray_info_idx": row["tray_info_idx"],
            })
        return out

    def _replace_usage(self, job_id, rows):
        self.conn.execute("DELETE FROM job_usage WHERE job_id=?", (job_id,))
        self.conn.execute(
            "DELETE FROM spool_events WHERE job_id=? AND event_type='used'",
            (job_id,),
        )
        for row in rows or []:
            self.conn.execute(
                """INSERT INTO job_usage (
                    job_id, spool_id, tag_uid, tray_uuid, slot, material, color, grams, meters,
                    source, remain_before, remain_after, tray_weight_g, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    job_id, row.get("spool_id"), row.get("tag_uid") or "", row.get("tray_uuid") or "",
                    row.get("slot") or "", row.get("material") or "", row.get("color") or "",
                    parse_float(row.get("grams")), parse_float(row.get("meters")),
                    row.get("source") or "unknown", parse_float(row.get("remain_before")),
                    parse_float(row.get("remain_after")), parse_float(row.get("tray_weight_g")),
                    row.get("note") or "",
                ),
            )
            if row.get("spool_id"):
                grams = parse_float(row.get("grams"))
                self._event(
                    row["spool_id"], "used", slot=row.get("slot") or "", job_id=job_id, grams=grams,
                    note="文件用量" if row.get("source") == "exact" else (
                        "按余量估算" if row.get("source") == "estimated" else "用量未知"
                    ),
                )
        source = usage_source_of(rows or [])
        self.conn.execute("UPDATE jobs SET usage_source=? WHERE id=?", (source, job_id))

    def _usage_rows(self, job_id):
        return [
            _row(row) for row in self.conn.execute(
                "SELECT * FROM job_usage WHERE job_id=? ORDER BY id",
                (job_id,),
            )
        ]

    def _write_cost(self, job_id):
        job = _row(self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
        if not job:
            return
        lines = []
        for row in self._usage_rows(job_id):
            price = None
            if row.get("spool_id"):
                spool = self._spool(row["spool_id"])
                if spool:
                    price = spool.get("unit_price_per_kg")
            lines.append({"grams": row.get("grams"), "price_per_kg": price})
        duration = job.get("duration_sec")
        summary = summarize_costs(lines, duration, job.get("electricity_rate"), job.get("labor_rate"))
        self.conn.execute(
            """UPDATE jobs SET material_cost=?, cost_complete=?, partial_cost=?, electricity_cost=?,
               labor_cost=?, updated_at=? WHERE id=?""",
            (
                summary["material_cost"], 1 if summary["cost_complete"] else 0, summary["partial_cost"],
                summary["electricity_cost"], summary["labor_cost"], time.time(), job_id,
            ),
        )

    def _recompute_spool_jobs(self, spool_id):
        rows = self.conn.execute(
            "SELECT DISTINCT job_id FROM job_usage WHERE spool_id=?",
            (spool_id,),
        ).fetchall()
        for row in rows:
            self._write_cost(row["job_id"])

    def _jobs_for_reports(self, limit=2000):
        with self._lock:
            jobs = [
                _row(row) for row in self.conn.execute(
                    "SELECT * FROM jobs ORDER BY COALESCE(end_ts, start_ts) DESC, id DESC LIMIT ?",
                    (int(limit),),
                )
            ]
            if not jobs:
                return []
            ids = [job["id"] for job in jobs]
            marks = ",".join("?" for _ in ids)
            usage = [
                _row(row) for row in self.conn.execute(
                    f"SELECT * FROM job_usage WHERE job_id IN ({marks}) ORDER BY id",
                    ids,
                )
            ]
            prices = {
                row["id"]: row["unit_price_per_kg"]
                for row in self.conn.execute("SELECT id, unit_price_per_kg FROM spools")
            }
        by_job = {job["id"]: job for job in jobs}
        for job in jobs:
            job["cost_complete"] = bool(job.get("cost_complete"))
            job["usages"] = []
        for row in usage:
            row["price_per_kg"] = prices.get(row.get("spool_id"))
            parent = by_job.get(row["job_id"])
            if parent is not None:
                parent["usages"].append(row)
        return jobs


def _optional_price(value):
    if value is None or value == "":
        return None
    price = parse_float(value)
    if price is None or price < 0:
        raise ValueError("单价需要是大于等于 0 的数字，单位为每千克")
    return price


def _check_bind(printer_id, slot):
    if bool(printer_id) != bool(slot):
        raise ValueError("绑定需要同时选择打印机和槽位")


def _currency(value):
    text = str(value or "CNY").strip().upper()
    return text[:8] or "CNY"


def _int_or_none(value):
    number = parse_float(value)
    if number is None:
        return None
    return int(number)

