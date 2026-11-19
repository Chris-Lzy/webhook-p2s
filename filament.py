"""耗材身份、用量估算、成本与报表的纯函数。不访问网络，也不写数据库。"""

from __future__ import annotations

import math
from datetime import datetime
from zoneinfo import ZoneInfo

# 空槽。实机 P2S + AMS 2 Pro：空槽只有 id + state，state 为 9。
EMPTY_STATES = {"9"}
EXTERNAL_TRAY_IDS = {"254", "255"}


def parse_float(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
    if not math.isfinite(number):
        return None
    return number


def parse_remain(value):
    """余量百分比。-1 表示打印机不知道剩余，必须原样保留，不能当成 0。"""
    return parse_float(value)


def remain_known(value):
    number = parse_remain(value)
    return number is not None and number >= 0


def normalize_color(value):
    text = str(value or "").strip().upper()
    if text.startswith("#"):
        text = text[1:]
    if len(text) == 6:
        text += "FF"
    if len(text) != 8:
        return ""
    try:
        int(text, 16)
    except ValueError:
        return ""
    return text


def color_rgb(value):
    text = normalize_color(value)
    return text[:6] if text else ""


def slot_occupied(raw):
    if not isinstance(raw, dict):
        return False
    state = str(raw.get("state") if raw.get("state") is not None else "").strip()
    if state in EMPTY_STATES:
        return False
    for key in ("tag_uid", "tray_uuid", "tray_type", "tray_id_name", "tray_info_idx"):
        if str(raw.get(key) or "").strip():
            return True
    # 已装填但字段不全时，仍视为占用，避免把未知状态当成空槽。
    if state not in {"", "0"}:
        return True
    return False


def _text(raw, key):
    return str(raw.get(key) or "").strip()


def normalize_slot(raw, slot, ams_id=""):
    """把一槽 MQTT 字段收成稳定结构。缺字段保持空，不补假数据。"""
    raw = raw if isinstance(raw, dict) else {}
    remain = parse_remain(raw.get("remain"))
    return {
        "slot": slot,
        "ams_id": str(ams_id or ""),
        "tray_id": _text(raw, "id"),
        "occupied": slot_occupied(raw),
        "state": str(raw.get("state") if raw.get("state") is not None else "").strip(),
        "tag_uid": _text(raw, "tag_uid"),
        "tray_uuid": _text(raw, "tray_uuid"),
        "material": _text(raw, "tray_type"),
        "color": normalize_color(raw.get("tray_color")),
        "diameter": parse_float(raw.get("tray_diameter")),
        "nominal_weight_g": parse_float(raw.get("tray_weight")),
        "remain_pct": remain,
        "sub_brands": _text(raw, "tray_sub_brands"),
        "tray_id_name": _text(raw, "tray_id_name"),
        "tray_info_idx": _text(raw, "tray_info_idx"),
        "nozzle_temp_min": parse_float(raw.get("nozzle_temp_min")),
        "nozzle_temp_max": parse_float(raw.get("nozzle_temp_max")),
        "drying_temp": parse_float(raw.get("drying_temp")),
        "drying_time": parse_float(raw.get("drying_time")),
        "total_len_mm": parse_float(raw.get("total_len")),
        "cols": _text(raw, "cols"),
        "ctype": _text(raw, "ctype"),
    }


def _tray_list(unit):
    trays = unit.get("tray")
    if isinstance(trays, list):
        return [item for item in trays if isinstance(item, dict)]
    return []


def parse_slots(print_obj):
    """从 print 对象抽出 AMS 槽位和外挂料盘。没有 ams 时返回空列表。"""
    if not isinstance(print_obj, dict):
        return []
    slots = []
    ams = print_obj.get("ams")
    units = []
    if isinstance(ams, dict):
        raw_units = ams.get("ams")
        if isinstance(raw_units, list):
            units = [item for item in raw_units if isinstance(item, dict)]
    elif isinstance(ams, list):
        units = [item for item in ams if isinstance(item, dict)]
    for unit in units:
        ams_id = str(unit.get("id") if unit.get("id") is not None else "0").strip() or "0"
        for tray in _tray_list(unit):
            tray_id = str(tray.get("id") if tray.get("id") is not None else "").strip()
            if not tray_id:
                continue
            slots.append(normalize_slot(tray, f"ams:{ams_id}:tray:{tray_id}", ams_id))
    vt = None
    if isinstance(ams, dict) and isinstance(ams.get("vt_tray"), dict):
        vt = ams.get("vt_tray")
    if isinstance(print_obj.get("vt_tray"), dict):
        vt = print_obj.get("vt_tray")
    if isinstance(vt, dict):
        slots.append(normalize_slot(vt, "vt_tray", "vt"))
    return slots


def active_slot_key(print_obj, slots):
    """当前挤出槽。对不上时返回 None，调用方不要猜。"""
    if not isinstance(print_obj, dict):
        return None
    ams = print_obj.get("ams") if isinstance(print_obj.get("ams"), dict) else {}
    raw = ams.get("tray_now")
    if raw is None:
        raw = print_obj.get("tray_now")
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text in EXTERNAL_TRAY_IDS:
        return "vt_tray" if any(item["slot"] == "vt_tray" for item in slots) else None
    ams_id = str(ams.get("ams_id") or ams.get("tray_now_ams_id") or "").strip()
    matches = [item for item in slots if item.get("tray_id") == text]
    if ams_id:
        scoped = [item for item in matches if item.get("ams_id") == ams_id]
        if len(scoped) == 1:
            return scoped[0]["slot"]
    if len(matches) == 1:
        return matches[0]["slot"]
    return None


def detect_load_change(prev, curr):
    """对比同一槽的两次观察。prev 为空表示这是本进程第一次看到该槽，不算装填。"""
    if not prev or not curr:
        return None
    if not prev.get("occupied") and curr.get("occupied"):
        return "empty_to_occupied"
    if prev.get("occupied") and curr.get("occupied"):
        prev_id = (prev.get("tag_uid") or "", prev.get("tray_uuid") or "")
        curr_id = (curr.get("tag_uid") or "", curr.get("tray_uuid") or "")
        if prev_id != curr_id and any(prev_id + curr_id):
            return "identity_change"
    return None


class SlotTracker:
    """进程内槽位基线。服务没看到过的槽位变化不记装填时间。"""

    def __init__(self):
        self.prev = {}
        self.active_slot = None

    def feed(self, slots, active_slot=None):
        changes = []
        dirty = []
        for slot in slots:
            key = slot["slot"]
            prev = self.prev.get(key)
            reason = detect_load_change(prev, slot)
            if reason:
                changes.append({"reason": reason, "prev": prev, "curr": slot})
            if prev is None or _slot_sig(prev) != _slot_sig(slot):
                dirty.append(slot)
            elif prev.get("spool_id") and "spool_id" not in slot:
                # 槽位没变化时不会重新关联耗材。沿用上一次的 spool_id，
                # 否则任务基线会丢掉耗材关联，补上单价后成本仍是「待补全」。
                slot["spool_id"] = prev["spool_id"]
            self.prev[key] = slot
        if active_slot is not None:
            self.active_slot = active_slot
        return {"changes": changes, "dirty": dirty, "slots": list(self.prev.values())}


def _slot_sig(slot):
    remain = slot.get("remain_pct")
    if remain is None:
        remain_key = None
    elif remain < 0:
        remain_key = -1
    else:
        remain_key = int(remain)
    return (
        bool(slot.get("occupied")),
        slot.get("tag_uid") or "",
        slot.get("tray_uuid") or "",
        slot.get("material") or "",
        slot.get("color") or "",
        remain_key,
        slot.get("nominal_weight_g"),
        slot.get("tray_info_idx") or "",
    )


def official_identity(tag_uid, tray_uuid):
    tag = str(tag_uid or "").strip()
    uuid = str(tray_uuid or "").strip()
    if not tag or not uuid:
        return None
    return tag, uuid


def estimate_grams(tray_weight_g, remain_before, remain_after):
    """用余量百分比差估算克数。任一端未知（含 -1）则返回 unknown，不编造数字。"""
    if not remain_known(remain_before) or not remain_known(remain_after):
        return None, "unknown"
    weight = parse_float(tray_weight_g)
    if weight is None or weight <= 0:
        return None, "unknown"
    delta = float(remain_before) - float(remain_after)
    if delta < 0:
        return None, "unknown"
    return weight * delta / 100.0, "estimated"


def select_estimated_usage(baselines, after_slots, active_slot=None):
    """只给真正可能用到的槽记账。旁路槽 remain=-1 不把整单成本打成未知。"""
    after = {item["slot"]: item for item in after_slots}
    occupied = [item for item in baselines if item.get("occupied")]
    chosen = set()
    for item in occupied:
        before = item.get("remain_pct")
        after_item = after.get(item["slot"]) or {}
        after_remain = after_item.get("remain_pct", before)
        grams, _source = estimate_grams(item.get("nominal_weight_g"), before, after_remain)
        if grams is not None and grams > 0:
            chosen.add(item["slot"])
    if active_slot and any(item["slot"] == active_slot for item in baselines):
        chosen.add(active_slot)
    elif len(occupied) == 1 and not chosen:
        chosen.add(occupied[0]["slot"])
    rows = []
    for item in baselines:
        if item["slot"] not in chosen:
            continue
        after_item = after.get(item["slot"]) or {}
        weight = item.get("nominal_weight_g")
        if weight is None:
            weight = after_item.get("nominal_weight_g")
        grams, source = estimate_grams(weight, item.get("remain_pct"), after_item.get("remain_pct"))
        rows.append({
            "slot": item["slot"],
            "spool_id": item.get("spool_id"),
            "tag_uid": item.get("tag_uid") or "",
            "tray_uuid": item.get("tray_uuid") or "",
            "material": item.get("material") or after_item.get("material") or "",
            "color": item.get("color") or after_item.get("color") or "",
            "grams": grams,
            "meters": None,
            "source": source,
            "remain_before": item.get("remain_pct"),
            "remain_after": after_item.get("remain_pct"),
            "tray_weight_g": weight,
        })
    return rows


def line_cost(grams, unit_price_per_kg):
    """单价是每千克。克数或单价缺失时返回 None，不能当成 0。"""
    grams_n = parse_float(grams)
    price_n = parse_float(unit_price_per_kg)
    if grams_n is None or price_n is None:
        return None
    if grams_n < 0 or price_n < 0:
        return None
    return grams_n / 1000.0 * price_n


def _priced(grams, price):
    grams_n = parse_float(grams)
    if grams_n is None:
        return False
    if grams_n == 0:
        return True
    return parse_float(price) is not None


def summarize_costs(lines, duration_sec=None, electricity_per_hour=None, labor_per_hour=None):
    """lines 需要 grams 与 price_per_kg。有一行说不清，整单材料成本就是不完整。"""
    partial = 0.0
    complete = bool(lines)
    for line in lines or []:
        grams = line.get("grams")
        price = line.get("price_per_kg")
        if not _priced(grams, price):
            complete = False
            continue
        if parse_float(grams) == 0:
            continue
        cost = line_cost(grams, price)
        if cost is None:
            complete = False
        else:
            partial += cost
    material = round(partial, 4) if complete else None
    electricity = _time_cost(duration_sec, electricity_per_hour)
    labor = _time_cost(duration_sec, labor_per_hour)
    total = None
    if material is not None:
        total = material
        if electricity is not None:
            total += electricity
        if labor is not None:
            total += labor
        total = round(total, 4)
    return {
        "material_cost": material,
        "partial_cost": round(partial, 4),
        "cost_complete": complete,
        "electricity_cost": None if electricity is None else round(electricity, 4),
        "labor_cost": None if labor is None else round(labor, 4),
        "total_cost": total,
    }


def _time_cost(duration_sec, rate_per_hour):
    duration = parse_float(duration_sec)
    rate = parse_float(rate_per_hour)
    if duration is None or rate is None or duration < 0 or rate < 0:
        return None
    return rate * duration / 3600.0


def match_exact_filaments(filaments, slots):
    """把切片文件里的用料行对上槽位。对不上也保留克数，只是没有耗材身份。"""
    pool = [dict(item) for item in slots if item.get("occupied")]
    used = set()
    rows = []
    for filament in filaments or []:
        grams = parse_float(filament.get("used_g"))
        meters = parse_float(filament.get("used_m"))
        if (grams is None or grams == 0) and (meters is None or meters == 0):
            continue
        match = _match_slot(filament, pool, used)
        if match is not None:
            used.add(match["slot"])
        source = "exact" if grams is not None else "unknown"
        rows.append({
            "slot": match["slot"] if match else "",
            "spool_id": match.get("spool_id") if match else None,
            "tag_uid": (match or {}).get("tag_uid") or "",
            "tray_uuid": (match or {}).get("tray_uuid") or "",
            "material": (match or {}).get("material") or str(filament.get("material") or ""),
            "color": (match or {}).get("color") or normalize_color(filament.get("color")),
            "grams": grams,
            "meters": meters,
            "source": source,
            "remain_before": None,
            "remain_after": None,
            "tray_weight_g": (match or {}).get("nominal_weight_g"),
            "tray_info_idx": str(filament.get("tray_info_idx") or ""),
        })
    return rows


def _match_slot(filament, pool, used):
    info = str(filament.get("tray_info_idx") or "").strip()
    if info:
        same = [
            item for item in pool
            if item["slot"] not in used and item.get("tray_info_idx") == info
        ]
        if len(same) > 1:
            # 同一 AMS 里几卷同款料（白/红/黑 PETG Basic 都是 GFG00）时，再按颜色区分。
            # 同色有多卷时仍取第一卷该色，避免退回成另一种颜色的槽。
            color = color_rgb(filament.get("color"))
            by_color = [item for item in same if color and color_rgb(item.get("color")) == color]
            if by_color:
                return by_color[0]
        if same:
            return same[0]
    material = str(filament.get("material") or "").strip().upper()
    color = color_rgb(filament.get("color"))
    if material and color:
        hits = [
            item for item in pool
            if item["slot"] not in used
            and str(item.get("material") or "").upper() == material
            and color_rgb(item.get("color")) == color
        ]
        if len(hits) == 1:
            return hits[0]
    if material:
        hits = [
            item for item in pool
            if item["slot"] not in used and str(item.get("material") or "").upper() == material
        ]
        if len(hits) == 1:
            return hits[0]
    fresh = [item for item in pool if item["slot"] not in used]
    if len(pool) == 1 and fresh:
        return fresh[0]
    return None


def usage_source_of(rows):
    sources = {row.get("source") or "unknown" for row in rows}
    if not sources:
        return "unknown"
    if sources == {"exact"}:
        return "exact"
    if sources == {"estimated"}:
        return "estimated"
    if "exact" in sources and "estimated" in sources:
        return "mixed"
    if sources <= {"exact", "estimated"}:
        return "mixed" if len(sources) > 1 else next(iter(sources))
    return "unknown" if sources == {"unknown"} else "mixed"


def low_remain_decision(remain, threshold, alerted, hysteresis=5):
    """返回 alert / clear / skip / none，以及新的 alerted 标记。remain<0 不告警。"""
    number = parse_remain(remain)
    limit = parse_float(threshold)
    if number is None or number < 0 or limit is None:
        return "skip", bool(alerted)
    flagged = bool(alerted)
    if number <= limit and not flagged:
        return "alert", True
    if number > limit + max(0, float(hysteresis)) and flagged:
        return "clear", False
    return "none", flagged


def job_total_cost(job):
    if not job.get("cost_complete"):
        return None
    material = parse_float(job.get("material_cost"))
    if material is None:
        return None
    total = material
    for key in ("electricity_cost", "labor_cost"):
        extra = parse_float(job.get(key))
        if extra is not None:
            total += extra
    return round(total, 4)


def _sum_grams(rows):
    total = 0.0
    known = False
    for row in rows:
        grams = parse_float(row.get("grams"))
        if grams is None:
            continue
        total += grams
        known = True
    return round(total, 4) if known else None


def _job_sort_ts(job):
    return parse_float(job.get("end_ts")) or parse_float(job.get("start_ts")) or 0


def aggregate_models(jobs):
    grouped = {}
    for job in jobs or []:
        name = str(job.get("task_name") or "").strip() or "未命名任务"
        grouped.setdefault(name, []).append(job)
    reports = []
    for name, items in grouped.items():
        reports.append(_summarize_job_group(name, items))
    reports.sort(key=lambda item: (-(item["print_count"]), item["task_name"]))
    return reports


def _summarize_job_group(key, items):
    grams = []
    costs = []
    unknown_usage = 0
    incomplete = 0
    last = max(items, key=_job_sort_ts)
    for job in items:
        usages = job.get("usages") or []
        if any(parse_float(row.get("grams")) is None for row in usages) or not usages:
            unknown_usage += 1
        total_g = _sum_grams(usages)
        if total_g is not None:
            grams.append(total_g)
        cost = job_total_cost(job)
        if cost is None:
            incomplete += 1
        else:
            costs.append(cost)
    last_spools = []
    for row in last.get("usages") or []:
        last_spools.append({
            "spool_id": row.get("spool_id"),
            "tag_uid": row.get("tag_uid") or "",
            "tray_uuid": row.get("tray_uuid") or "",
            "material": row.get("material") or "",
            "color": row.get("color") or "",
            "grams": parse_float(row.get("grams")),
            "source": row.get("source") or "unknown",
            "slot": row.get("slot") or "",
        })
    return {
        "key": key,
        "task_name": key,
        "print_count": len(items),
        "total_grams": round(sum(grams), 4) if grams else None,
        "total_cost": round(sum(costs), 4) if costs and incomplete == 0 else None,
        "partial_cost": round(sum(costs), 4) if costs else None,
        "cost_complete": incomplete == 0 and bool(items),
        "complete_jobs": len(costs),
        "incomplete_jobs": incomplete,
        "avg_cost": round(sum(costs) / len(costs), 4) if costs else None,
        "min_cost": round(min(costs), 4) if costs else None,
        "max_cost": round(max(costs), 4) if costs else None,
        "unknown_usage_jobs": unknown_usage,
        "last_ts": _job_sort_ts(last) or None,
        "last_spools": last_spools,
    }


def aggregate_spools(spools, jobs):
    usage_by_spool = {}
    for job in jobs or []:
        for row in job.get("usages") or []:
            spool_id = row.get("spool_id")
            if not spool_id:
                continue
            bucket = usage_by_spool.setdefault(int(spool_id), {"grams": [], "cost": [], "models": {}})
            grams = parse_float(row.get("grams"))
            if grams is not None:
                bucket["grams"].append(grams)
                if grams == 0:
                    bucket["cost"].append(0.0)
                else:
                    cost = line_cost(grams, row.get("price_per_kg"))
                    if cost is not None:
                        bucket["cost"].append(cost)
            name = str(job.get("task_name") or "").strip() or "未命名任务"
            model = bucket["models"].setdefault(name, {"task_name": name, "grams": 0.0, "grams_known": False, "prints": 0})
            model["prints"] += 1
            if grams is not None:
                model["grams"] += grams
                model["grams_known"] = True
    reports = []
    for spool in spools or []:
        bucket = usage_by_spool.get(int(spool["id"]), {"grams": [], "cost": [], "models": {}})
        used = round(sum(bucket["grams"]), 4) if bucket["grams"] else None
        nominal = parse_float(spool.get("nominal_weight_g"))
        remain = parse_remain(spool.get("remain_pct"))
        if remain_known(remain) and nominal is not None:
            remaining = round(nominal * float(remain) / 100.0, 4)
            remaining_source = "printer"
        elif nominal is not None and used is not None:
            remaining = round(max(0.0, nominal - used), 4)
            remaining_source = "ledger"
        else:
            remaining = None
            remaining_source = "unknown"
        models = []
        for model in bucket["models"].values():
            models.append({
                "task_name": model["task_name"],
                "prints": model["prints"],
                "grams": round(model["grams"], 4) if model["grams_known"] else None,
            })
        models.sort(key=lambda item: item["task_name"])
        reports.append({
            "spool_id": spool["id"],
            "source": spool.get("source") or "",
            "tag_uid": spool.get("tag_uid") or "",
            "tray_uuid": spool.get("tray_uuid") or "",
            "brand": spool.get("brand") or "",
            "material": spool.get("material") or "",
            "color": spool.get("color") or "",
            "nominal_weight_g": nominal,
            "used_grams": used,
            "remain_pct": remain,
            "remaining_grams": remaining,
            "remaining_source": remaining_source,
            "partial_cost": round(sum(bucket["cost"]), 4) if bucket["cost"] else None,
            "cost": round(sum(bucket["cost"]), 4) if bucket["grams"] and len(bucket["cost"]) == len(bucket["grams"]) else None,
            "cost_complete": bool(bucket["grams"]) and len(bucket["cost"]) == len(bucket["grams"]),
            "unit_price_per_kg": parse_float(spool.get("unit_price_per_kg")),
            "currency": spool.get("currency") or "CNY",
            "models": models,
        })
    reports.sort(key=lambda item: (item["material"], item["spool_id"] or 0))
    return reports


def aggregate_by(jobs, key_fn):
    grouped = {}
    for job in jobs or []:
        # 用量行维度（材料、颜色）在外面拆；这里按任务整单归类。
        key = key_fn(job)
        grouped.setdefault(key, []).append(job)
    rows = []
    for key, items in grouped.items():
        summary = _summarize_job_group(key, items)
        rows.append(summary)
    rows.sort(key=lambda item: item["key"])
    return rows


def aggregate_usage_axis(jobs, axis):
    """按材料或颜色汇总用量。成本只累加能算清的行，缺单价则标记不完整。"""
    buckets = {}
    for job in jobs or []:
        for row in job.get("usages") or []:
            if axis == "color":
                key = row.get("color") or "未知颜色"
            else:
                key = row.get("material") or "未知材料"
            bucket = buckets.setdefault(key, {"grams": [], "cost": [], "missing": 0, "prints": set()})
            grams = parse_float(row.get("grams"))
            if grams is None:
                bucket["missing"] += 1
            else:
                bucket["grams"].append(grams)
                if grams == 0:
                    bucket["cost"].append(0.0)
                else:
                    cost = line_cost(grams, row.get("price_per_kg"))
                    if cost is None:
                        bucket["missing"] += 1
                    else:
                        bucket["cost"].append(cost)
            bucket["prints"].add(job.get("id"))
    rows = []
    for key, bucket in buckets.items():
        rows.append({
            "key": key,
            "total_grams": round(sum(bucket["grams"]), 4) if bucket["grams"] else None,
            "total_cost": round(sum(bucket["cost"]), 4) if bucket["cost"] and bucket["missing"] == 0 else None,
            "partial_cost": round(sum(bucket["cost"]), 4) if bucket["cost"] else None,
            "cost_complete": bucket["missing"] == 0 and bool(bucket["grams"]),
            "print_count": len([item for item in bucket["prints"] if item is not None]),
        })
    rows.sort(key=lambda item: item["key"])
    return rows


def period_key(ts, granularity="day", tz_name="Asia/Shanghai"):
    number = parse_float(ts)
    if number is None:
        return ""
    try:
        tz = ZoneInfo(tz_name or "Asia/Shanghai")
    except Exception:
        tz = ZoneInfo("Asia/Shanghai")
    moment = datetime.fromtimestamp(number, tz)
    if granularity == "month":
        return moment.strftime("%Y-%m")
    return moment.strftime("%Y-%m-%d")


def aggregate_periods(jobs, granularity="day", tz_name="Asia/Shanghai"):
    grouped = {}
    for job in jobs or []:
        key = period_key(job.get("end_ts") or job.get("start_ts"), granularity, tz_name)
        if not key:
            continue
        grouped.setdefault(key, []).append(job)
    rows = []
    for key in sorted(grouped):
        summary = _summarize_job_group(key, grouped[key])
        rows.append(summary)
    return rows


def trace_model(task_name, jobs):
    name = str(task_name or "").strip()
    matched = [job for job in jobs or [] if (str(job.get("task_name") or "").strip() or "未命名任务") == name]
    spools = {}
    for job in matched:
        for row in job.get("usages") or []:
            identity = (
                row.get("spool_id"),
                row.get("tag_uid") or "",
                row.get("tray_uuid") or "",
                row.get("material") or "",
                row.get("color") or "",
            )
            bucket = spools.setdefault(identity, {
                "spool_id": row.get("spool_id"),
                "tag_uid": row.get("tag_uid") or "",
                "tray_uuid": row.get("tray_uuid") or "",
                "material": row.get("material") or "",
                "color": row.get("color") or "",
                "grams": [],
                "prints": 0,
            })
            bucket["prints"] += 1
            grams = parse_float(row.get("grams"))
            if grams is not None:
                bucket["grams"].append(grams)
    spool_rows = []
    for bucket in spools.values():
        spool_rows.append({
            **{k: bucket[k] for k in ("spool_id", "tag_uid", "tray_uuid", "material", "color", "prints")},
            "grams": round(sum(bucket["grams"]), 4) if bucket["grams"] else None,
        })
    spool_rows.sort(key=lambda item: (item["material"], str(item["tag_uid"])))
    summary = _summarize_job_group(name or "未命名任务", matched) if matched else None
    return {"task_name": name, "jobs": len(matched), "summary": summary, "spools": spool_rows}


def trace_spool(spool_id, jobs):
    models = []
    seen = {}
    for job in jobs or []:
        for row in job.get("usages") or []:
            if row.get("spool_id") != spool_id:
                continue
            name = str(job.get("task_name") or "").strip() or "未命名任务"
            bucket = seen.setdefault(name, {"task_name": name, "prints": 0, "grams": []})
            bucket["prints"] += 1
            grams = parse_float(row.get("grams"))
            if grams is not None:
                bucket["grams"].append(grams)
    for bucket in seen.values():
        models.append({
            "task_name": bucket["task_name"],
            "prints": bucket["prints"],
            "grams": round(sum(bucket["grams"]), 4) if bucket["grams"] else None,
        })
    models.sort(key=lambda item: item["task_name"])
    return {"spool_id": spool_id, "models": models}
