import ftplib
import socket
import tempfile
import unittest
import zipfile
from unittest import mock
from io import BytesIO
from pathlib import Path

from filament import (
    SlotTracker,
    aggregate_models,
    aggregate_spools,
    estimate_grams,
    line_cost,
    low_remain_decision,
    match_exact_filaments,
    normalize_slot,
    parse_slots,
    select_estimated_usage,
    summarize_costs,
    trace_model,
    trace_spool,
)
from filesource import (
    ImplicitFTP_TLS,
    capture_camera_still,
    collect_from_ftp,
    parse_gcode_header,
    parse_slice_bytes,
)
from store import Store


def loaded_tray(**overrides):
    raw = {
        "id": "0",
        "state": 11,
        "tray_type": "PETG",
        "tray_sub_brands": "Bambu",
        "tray_color": "D6001CFF",
        "tray_diameter": "1.75",
        "tray_weight": "1000",
        "remain": 80,
        "tag_uid": "TAGPETG",
        "tray_uuid": "UUIDPETG",
        "tray_id_name": "G00-W00",
        "tray_info_idx": "GFG00",
        "nozzle_temp_min": "240",
        "nozzle_temp_max": "270",
        "drying_temp": "65",
        "drying_time": "12",
        "total_len": "330000",
    }
    raw.update(overrides)
    return raw


class CostAndRemainTests(unittest.TestCase):
    def test_price_times_grams_is_per_kilogram(self):
        self.assertEqual(line_cost(100, 80), 8)
        self.assertEqual(line_cost(250, 100), 25)
        self.assertIsNone(line_cost(100, None))
        self.assertIsNone(line_cost(None, 80))
        summary = summarize_costs([
            {"grams": 100, "price_per_kg": 80},
            {"grams": 50, "price_per_kg": None},
        ])
        self.assertFalse(summary["cost_complete"])
        self.assertIsNone(summary["material_cost"])
        self.assertNotEqual(summary["material_cost"], 0)
        self.assertEqual(summary["partial_cost"], 8)
        complete = summarize_costs([{"grams": 100, "price_per_kg": 80}], duration_sec=3600, electricity_per_hour=1.5)
        self.assertTrue(complete["cost_complete"])
        self.assertEqual(complete["material_cost"], 8)
        self.assertEqual(complete["electricity_cost"], 1.5)
        self.assertEqual(complete["total_cost"], 9.5)

    def test_remain_percent_estimate_and_unknown_minus_one(self):
        grams, source = estimate_grams(1000, 100, 90)
        self.assertEqual(grams, 100)
        self.assertEqual(source, "estimated")
        for before, after in ((-1, 80), (80, -1), (None, 70), (70, None)):
            grams, source = estimate_grams(1000, before, after)
            self.assertIsNone(grams)
            self.assertEqual(source, "unknown")
        self.assertEqual(low_remain_decision(-1, 10, False), ("skip", False))
        self.assertEqual(low_remain_decision(None, 10, False), ("skip", False))
        action, flagged = low_remain_decision(8, 10, False)
        self.assertEqual((action, flagged), ("alert", True))
        self.assertEqual(low_remain_decision(8, 10, True)[0], "none")

    def test_spare_unknown_spool_does_not_block_a_measured_slot(self):
        active = {
            "slot": "ams:0:tray:0", "occupied": True, "spool_id": 1,
            "tag_uid": "A", "tray_uuid": "U1", "material": "PETG", "color": "D6001CFF",
            "remain_pct": 100, "nominal_weight_g": 1000, "tray_info_idx": "GFG00",
        }
        spare = {
            "slot": "ams:0:tray:1", "occupied": True, "spool_id": 2,
            "tag_uid": "B", "tray_uuid": "U2", "material": "PLA", "color": "FFFFFFFF",
            "remain_pct": -1, "nominal_weight_g": 1000, "tray_info_idx": "GFA00",
        }
        after = [
            {**active, "remain_pct": 90},
            spare,
        ]
        rows = select_estimated_usage([active, spare], after, active_slot="ams:0:tray:0")
        self.assertEqual([row["slot"] for row in rows], ["ams:0:tray:0"])
        self.assertEqual(rows[0]["grams"], 100)
        self.assertEqual(rows[0]["source"], "estimated")


class LoadDetectionTests(unittest.TestCase):
    def test_first_sight_is_baseline_and_uuid_change_is_a_load(self):
        tracker = SlotTracker()
        first = normalize_slot(loaded_tray(), "ams:0:tray:0", "0")
        fed = tracker.feed([first])
        self.assertEqual(fed["changes"], [])
        changed = normalize_slot(loaded_tray(tray_uuid="UUID-NEXT", tag_uid="TAGPETG"), "ams:0:tray:0", "0")
        fed = tracker.feed([changed])
        self.assertEqual(fed["changes"][0]["reason"], "identity_change")
        self.assertEqual(fed["changes"][0]["prev"]["tray_uuid"], "UUIDPETG")
        self.assertEqual(fed["changes"][0]["curr"]["tray_uuid"], "UUID-NEXT")

    def test_empty_to_occupied_and_empty_slot_shape(self):
        tracker = SlotTracker()
        empty = normalize_slot({"id": "1", "state": 9}, "ams:0:tray:1", "0")
        self.assertFalse(empty["occupied"])
        fed = tracker.feed([empty])
        self.assertEqual(fed["changes"], [])
        loaded = normalize_slot(loaded_tray(id="1"), "ams:0:tray:1", "0")
        fed = tracker.feed([loaded])
        self.assertEqual(fed["changes"][0]["reason"], "empty_to_occupied")

    def test_sample_p2s_payload_keeps_rfid_fields(self):
        payload = {
            "ams": {
                "ams": [{
                    "id": "0",
                    "humidity": "4",
                    "temp": "28.0",
                    "tray": [loaded_tray(remain=-1), {"id": "1", "state": 9}],
                }],
                "tray_exist_bits": "1",
                "tray_is_bbl_bits": "1",
                "tray_now": "0",
                "vt_tray": {"id": "254", "state": 9},
            }
        }
        slots = parse_slots({"ams": payload["ams"]})
        by_slot = {item["slot"]: item for item in slots}
        official = by_slot["ams:0:tray:0"]
        self.assertTrue(official["occupied"])
        self.assertEqual(official["material"], "PETG")
        self.assertEqual(official["color"], "D6001CFF")
        self.assertEqual(official["nominal_weight_g"], 1000)
        self.assertEqual(official["remain_pct"], -1)
        self.assertEqual(official["tray_info_idx"], "GFG00")
        self.assertEqual(official["drying_temp"], 65)
        self.assertEqual(official["total_len_mm"], 330000)
        self.assertFalse(by_slot["ams:0:tray:1"]["occupied"])
        self.assertEqual(by_slot["vt_tray"]["slot"], "vt_tray")

    def test_partial_report_keeps_weight_and_remain_g_is_preferred(self):
        tracker = SlotTracker()
        full = normalize_slot(loaded_tray(remain=86, remain_g=867), "ams:0:tray:0", "0")
        tracker.feed([full])
        partial = normalize_slot({"id": "0", "state": 27}, "ams:0:tray:0", "0")
        fed = tracker.feed([partial])
        live = list(tracker.prev.values())[0]
        self.assertEqual(fed["dirty"], [])
        self.assertEqual(live["nominal_weight_g"], 1000)
        self.assertEqual(live["remain_pct"], 86)
        self.assertEqual(live["remain_g"], 867)
        self.assertFalse(live["remain_g_fresh"])
        self.assertEqual(live["material"], "PETG")
        self.assertTrue(live["occupied"])
        self.assertEqual(live["state"], "27")

        cleared = normalize_slot({"id": "0", "state": 9}, "ams:0:tray:0", "0")
        tracker.feed([cleared])
        empty = list(tracker.prev.values())[0]
        self.assertFalse(empty["occupied"])
        self.assertIsNone(empty["nominal_weight_g"])

    def test_zero_tray_weight_is_unknown_and_vir_slot_is_read(self):
        external = normalize_slot(
            {"id": "254", "state": 11, "tray_type": "PLA", "tray_weight": "0", "remain": 0},
            "vt_tray", "vt",
        )
        self.assertIsNone(external["nominal_weight_g"])
        self.assertIsNone(external["remain_pct"])
        self.assertTrue(external["occupied"])

        slots = parse_slots({
            "ams": {
                "ams": [{"id": "0", "tray": [loaded_tray(remain=40, remain_g=410)]}],
                "vt_tray": {"id": "254", "state": 9},
                "vir_slot": [{
                    "id": "254",
                    "state": 11,
                    "tray_type": "PETG",
                    "tray_color": "D6001CFF",
                    "tray_weight": "1000",
                    "remain": 40,
                    "remain_g": 410,
                    "tag_uid": "TAGPETG",
                    "tray_uuid": "UUIDPETG",
                }],
            }
        })
        by_slot = {item["slot"]: item for item in slots}
        self.assertEqual(by_slot["vt_tray"]["remain_g"], 410)
        self.assertEqual(by_slot["vt_tray"]["nominal_weight_g"], 1000)
        self.assertTrue(by_slot["vt_tray"]["occupied"])
        self.assertNotEqual(by_slot["vt_tray"]["state"], "9")
        report = aggregate_spools([{
            "id": 1, "source": "rfid", "tag_uid": "TAGPETG", "tray_uuid": "UUIDPETG",
            "brand": "", "material": "PETG", "color": "D6001CFF",
            "nominal_weight_g": 1000, "remain_pct": 40, "remain_g": 410,
            "unit_price_per_kg": None, "currency": "CNY",
        }], [])
        self.assertEqual(report[0]["remaining_grams"], 410)
        self.assertEqual(report[0]["remaining_source"], "printer")


class ReportTests(unittest.TestCase):
    def test_model_and_spool_trace_each_other(self):
        jobs = [
            {
                "id": 1, "task_name": "挂钩", "printer_name": "P2S_1", "start_ts": 1, "end_ts": 100,
                "material_cost": 8, "electricity_cost": None, "labor_cost": None, "cost_complete": True,
                "usages": [{
                    "spool_id": 7, "tag_uid": "TAGPETG", "tray_uuid": "UUIDPETG",
                    "material": "PETG", "color": "D6001CFF", "grams": 100, "source": "exact",
                    "price_per_kg": 80, "slot": "ams:0:tray:0",
                }],
            },
            {
                "id": 2, "task_name": "挂钩", "printer_name": "P2S_1", "start_ts": 200, "end_ts": 300,
                "material_cost": 4, "electricity_cost": None, "labor_cost": None, "cost_complete": True,
                "usages": [{
                    "spool_id": 7, "tag_uid": "TAGPETG", "tray_uuid": "UUIDPETG",
                    "material": "PETG", "color": "D6001CFF", "grams": 50, "source": "estimated",
                    "price_per_kg": 80, "slot": "ams:0:tray:0",
                }],
            },
            {
                "id": 3, "task_name": "挂钩", "printer_name": "P2S_1", "start_ts": 400, "end_ts": 500,
                "material_cost": None, "cost_complete": False,
                "usages": [{
                    "spool_id": 7, "tag_uid": "TAGPETG", "tray_uuid": "UUIDPETG",
                    "material": "PETG", "color": "D6001CFF", "grams": None, "source": "unknown",
                    "price_per_kg": 80, "slot": "ams:0:tray:0",
                }],
            },
        ]
        report = aggregate_models(jobs)[0]
        self.assertEqual(report["print_count"], 3)
        self.assertEqual(report["total_grams"], 150)
        self.assertIsNone(report["total_cost"])
        self.assertEqual(report["avg_cost"], 6)
        self.assertEqual(report["min_cost"], 4)
        self.assertEqual(report["max_cost"], 8)
        self.assertEqual(report["last_spools"][0]["tag_uid"], "TAGPETG")
        spools = aggregate_spools([{
            "id": 7, "source": "rfid", "tag_uid": "TAGPETG", "tray_uuid": "UUIDPETG",
            "brand": "Bambu", "material": "PETG", "color": "D6001CFF",
            "nominal_weight_g": 1000, "remain_pct": -1, "unit_price_per_kg": 80, "currency": "CNY",
        }], jobs)
        self.assertEqual(spools[0]["used_grams"], 150)
        self.assertEqual(spools[0]["remaining_source"], "ledger")
        self.assertEqual(spools[0]["models"][0]["task_name"], "挂钩")
        self.assertEqual(trace_spool(7, jobs)["models"][0]["grams"], 150)
        traced = trace_model("挂钩", jobs)
        self.assertEqual(traced["spools"][0]["tag_uid"], "TAGPETG")
        self.assertEqual(traced["spools"][0]["grams"], 150)


class FileSourceTests(unittest.TestCase):
    def test_data_connection_reuses_tls_session(self):
        ftp = ImplicitFTP_TLS()
        ftp.host = "printer.local"
        ftp._prot_p = True
        ftp.sock = type("Sock", (), {"session": "session-token"})()
        seen = {}

        def wrap(conn, server_hostname=None, session=None):
            seen["session"] = session
            seen["host"] = server_hostname
            return "wrapped"

        ftp.context.wrap_socket = wrap
        with mock.patch("filesource.ftplib.FTP.ntransfercmd", return_value=("raw", 12)):
            conn, size = ftp.ntransfercmd("RETR plate.gcode")
        self.assertEqual(conn, "wrapped")
        self.assertEqual(size, 12)
        self.assertEqual(seen["session"], "session-token")
        self.assertEqual(seen["host"], "printer.local")

    def test_implicit_connect_wraps_socket_before_greeting(self):
        ftp = ImplicitFTP_TLS()
        order = []
        wrapped = type("Wrapped", (), {
            "makefile": lambda self, *args, **kwargs: None,
            "family": socket.AF_INET,
        })()

        def wrap(sock, server_hostname=None):
            order.append("wrap")
            return wrapped

        ftp.context.wrap_socket = wrap
        ftp.getresp = lambda: order.append("greeting") or "220 ready"
        with mock.patch("filesource.socket.create_connection", return_value=object()):
            welcome = ftp.connect("printer.local", 990, timeout=1)
        self.assertEqual(order, ["wrap", "greeting"])
        self.assertEqual(welcome, "220 ready")

    def test_empty_chroot_does_not_download(self):
        class Fake:
            def nlst(self):
                return [".", ".."]

            def retrbinary(self, *args, **kwargs):
                raise AssertionError("空目录不应再下载")

        result = collect_from_ftp(Fake(), gcode_file="/data/Metadata/plate_1.gcode", task_name="demo")
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "empty_chroot")
        self.assertEqual(result.filaments, [])

    def test_denied_listing_falls_back_without_raising(self):
        class Fake:
            def nlst(self):
                raise ftplib.error_perm("550 Permission denied")

            def retrbinary(self, cmd, callback):
                raise ftplib.error_perm("550 Failed to open file.")

        result = collect_from_ftp(Fake(), gcode_file="/data/Metadata/plate_1.gcode")
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "list_denied")
        self.assertEqual(result.filaments, [])

    def test_3mf_metadata_and_preview(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <config><plate>
          <metadata key="index" value="1"/>
          <filament id="1" tray_info_idx="GFG00" type="PETG" color="#D6001C" used_m="12.5" used_g="36.2"/>
          <filament id="2" tray_info_idx="" type="PLA" color="#FFFFFF" used_m="0" used_g="0"/>
        </plate></config>""".encode()
        blob = BytesIO()
        with zipfile.ZipFile(blob, "w") as archive:
            archive.writestr("Metadata/slice_info.config", xml)
            archive.writestr("Metadata/plate_1.png", b"\x89PNG\r\n\x1a\npreview")
        parsed = parse_slice_bytes(blob.getvalue(), "1")
        self.assertEqual(parsed.filaments[0]["used_g"], 36.2)
        self.assertEqual(parsed.filaments[0]["tray_info_idx"], "GFG00")
        self.assertTrue(parsed.preview.startswith(b"\x89PNG"))
        rows = match_exact_filaments(parsed.filaments, [{
            "slot": "ams:0:tray:0", "occupied": True, "spool_id": 3,
            "tag_uid": "TAGPETG", "tray_uuid": "UUIDPETG", "material": "PETG",
            "color": "D6001CFF", "tray_info_idx": "GFG00", "nominal_weight_g": 1000,
        }])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["grams"], 36.2)
        self.assertEqual(rows[0]["source"], "exact")
        self.assertEqual(rows[0]["spool_id"], 3)

    def test_gcode_header_grams(self):
        rows = parse_gcode_header(b"; filament used [mm] = 3500, 0\n; filament used [g] = 10.5, 0\n")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["used_g"], 10.5)
        self.assertAlmostEqual(rows[0]["used_m"], 3.5)

    def test_bambu_studio_total_filament_weight(self):
        # Bambu Studio 实际写出的是这一组，旧的 "filament used [g] =" 被注释掉了。
        # 长度是毫米；体积行标成 cm^3 但数值是 mm^3，不能拿来换算克数。
        blob = (
            b"; HEADER_BLOCK_START\n"
            b"; total filament length [mm] : 10618.13,0.00\n"
            b"; total filament volume [cm^3] : 25539.60,0.00\n"
            b"; total filament weight [g] : 32.18,0.00\n"
        )
        rows = parse_gcode_header(blob)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["used_g"], 32.18)
        self.assertAlmostEqual(rows[0]["used_m"], 10.61813)
        parsed = parse_slice_bytes(blob, "1")
        self.assertEqual(parsed.filaments[0]["used_g"], 32.18)

    def test_camera_failure_is_not_fatal(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "still.jpg"

            def explode(url, timeout):
                raise TimeoutError("reset")

            ok, how = capture_camera_still(
                "10.1.2.3", "fake-access-code", dest, allow_rtsp=False, http_get=explode,
            )
            self.assertFalse(ok)
            self.assertEqual(how, "unavailable")
            self.assertFalse(dest.exists())

            def jpeg(url, timeout):
                return b"\xff\xd8\xff\xd9"

            ok, how = capture_camera_still(
                "10.1.2.3", "fake-access-code", dest, allow_rtsp=False, http_get=jpeg,
            )
            self.assertTrue(ok)
            self.assertEqual(how, "http_6000")
            self.assertNotIn(b"fake-access-code", dest.read_bytes())


class StoreRoundTripTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.store = Store(root / "inventory.db", root / "archive")

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_uuid_change_price_and_report_roundtrip(self):
        tracker = SlotTracker()
        slot = normalize_slot(loaded_tray(remain=100), "ams:0:tray:0", "0")
        first = tracker.feed([slot])
        result = self.store.apply_observation("p1", "P2S_1", first["dirty"], first["changes"], 10, True)
        self.assertEqual(result["alerts"], [])
        self.assertEqual(self.store.list_loads(), [])
        current = list(tracker.prev.values())
        self.assertTrue(current[0]["spool_id"])
        changed = normalize_slot(loaded_tray(remain=100, tray_uuid="UUID-2"), "ams:0:tray:0", "0")
        second = tracker.feed([changed])
        self.store.apply_observation("p1", "P2S_1", second["dirty"], second["changes"], 10, True)
        loads = self.store.list_loads()
        self.assertEqual(loads[0]["reason"], "identity_change")
        self.assertEqual(len(self.store.list_spools()), 2)

        priced = normalize_slot(loaded_tray(remain=100, tag_uid="TAG2", tray_uuid="UUID2"), "ams:0:tray:1", "0")
        tracker.feed([priced])
        # 重新观察带价格的那一卷：先建档再开打。
        tracker2 = SlotTracker()
        fed = tracker2.feed([priced])
        self.store.apply_observation("p1", "P2S_1", fed["dirty"], fed["changes"], 10, True)
        live = list(tracker2.prev.values())[0]
        self.store.open_job("p1", "P2S_1", "挂钩", "/data/Metadata/plate_1.gcode", [live], live["slot"], 3, 100, 10)
        after = dict(live)
        after["remain_pct"] = 90
        job = self.store.close_job("p1", "finished", [after], live["slot"], None, None)
        self.assertEqual(job["usages"][0]["grams"], 100)
        self.assertEqual(job["usages"][0]["source"], "estimated")
        self.assertFalse(job["cost_complete"])
        self.assertIsNone(job["material_cost"])
        updated = self.store.update_spool(live["spool_id"], {"unit_price_per_kg": 80})
        self.assertEqual(updated["unit_price_per_kg"], 80)
        job = self.store.get_job(job["id"])
        self.assertTrue(job["cost_complete"])
        self.assertEqual(job["material_cost"], 8)
        models = self.store.reports()["models"]
        hook = next(item for item in models if item["task_name"] == "挂钩")
        self.assertEqual(hook["print_count"], 1)
        self.assertEqual(hook["total_grams"], 100)
        self.assertEqual(hook["total_cost"], 8)
        traced = self.store.trace(task_name="挂钩", spool_id=live["spool_id"])
        self.assertEqual(traced["model"]["spools"][0]["spool_id"], live["spool_id"])
        self.assertEqual(traced["spool"]["models"][0]["task_name"], "挂钩")

    def test_remain_g_delta_is_used_when_percent_is_unknown(self):
        tracker = SlotTracker()
        start = normalize_slot(loaded_tray(remain=-1, remain_g=800), "ams:0:tray:0", "0")
        fed = tracker.feed([start])
        self.store.apply_observation("p1", "P2S_1", fed["dirty"], fed["changes"], 10, True)
        live = list(tracker.prev.values())[0]
        self.assertEqual(self.store.get_spool(live["spool_id"])["remain_g"], 800)
        self.store.open_job("p1", "P2S_1", "克数", "/data/Metadata/plate_1.gcode", [live], live["slot"], 1, 10, 1)
        end = normalize_slot(loaded_tray(remain=-1, remain_g=760), "ams:0:tray:0", "0")
        tracker.feed([end])
        after = list(tracker.prev.values())[0]
        job = self.store.close_job("p1", "finished", [after], live["slot"], None, None)
        self.assertEqual(job["usages"][0]["grams"], 40)
        self.assertEqual(job["usages"][0]["source"], "estimated")

    def test_remain_g_alerts_when_percent_is_unknown(self):
        tracker = SlotTracker()
        slot = normalize_slot(loaded_tray(remain=-1, remain_g=50), "ams:0:tray:0", "0")
        fed = tracker.feed([slot])
        result = self.store.apply_observation("p1", "P2S_1", fed["dirty"], fed["changes"], 10, True)
        self.assertEqual(len(result["alerts"]), 1)
        self.assertEqual(result["alerts"][0]["remain_pct"], 5)

    def test_remain_minus_one_is_unknown_and_does_not_alert(self):
        tracker = SlotTracker()
        slot = normalize_slot(loaded_tray(remain=-1), "ams:0:tray:0", "0")
        fed = tracker.feed([slot])
        result = self.store.apply_observation("p9", "P2S_9", fed["dirty"], fed["changes"], 10, True)
        self.assertEqual(result["alerts"], [])
        live = list(tracker.prev.values())[0]
        self.store.open_job("p9", "P2S_9", "未知余量", "", [live], live["slot"], 1, 10, 1)
        job = self.store.close_job("p9", "finished", [dict(live)], live["slot"], None, None)
        self.assertEqual(job["usages"][0]["source"], "unknown")
        self.assertIsNone(job["usages"][0]["grams"])
        self.assertIsNone(job["material_cost"])
        self.assertNotEqual(job["material_cost"], 0)

    def test_manual_spool_binds_to_external_slot(self):
        spool = self.store.create_manual({
            "brand": "某牌",
            "material": "PLA",
            "color": "#FFFFFF",
            "nominal_weight_g": 1000,
            "unit_price_per_kg": 60,
            "bound_printer_id": "p1",
            "bound_slot": "vt_tray",
        })
        tracker = SlotTracker()
        empty = normalize_slot({"id": "254", "state": 9}, "vt_tray", "vt")
        tracker.feed([empty])
        loaded = normalize_slot({"id": "254", "state": 11, "tray_type": "PLA", "tray_color": "FFFFFFFF"}, "vt_tray", "vt")
        fed = tracker.feed([loaded])
        self.store.apply_observation("p1", "P2S_1", fed["dirty"], fed["changes"], 10, True)
        self.assertEqual(fed["changes"][0]["curr"]["spool_id"], spool["id"])
        loads = self.store.list_loads()
        self.assertEqual(loads[0]["reason"], "empty_to_occupied")
        self.assertEqual(loads[0]["slot"], "vt_tray")
        with self.assertRaises(ValueError):
            self.store.create_manual({
                "material": "PLA",
                "nominal_weight_g": 500,
                "bound_slot": "vt_tray",
            })

    def test_unchanged_report_keeps_spool_so_price_recomputes(self):
        """槽位原样再报一次后开打，补上单价仍应重算，不能一直停在待补全。"""
        tracker = SlotTracker()
        slot = normalize_slot(loaded_tray(remain=100), "ams:0:tray:0", "0")
        first = tracker.feed([slot])
        self.store.apply_observation("p1", "P2S_1", first["dirty"], first["changes"], 10, True)
        spool_id = list(tracker.prev.values())[0]["spool_id"]
        self.assertTrue(spool_id)
        again = tracker.feed([normalize_slot(loaded_tray(remain=100), "ams:0:tray:0", "0")])
        self.assertEqual(again["dirty"], [])
        current = list(tracker.prev.values())[0]
        self.assertEqual(current.get("spool_id"), spool_id)
        self.store.open_job(
            "p1", "P2S_1", "挂钩", "/data/Metadata/plate_1.gcode",
            [current], current["slot"], 3, 100, 10,
        )
        after = dict(current)
        after["remain_pct"] = 90
        job = self.store.close_job("p1", "finished", [after], current["slot"], None, None)
        self.assertEqual(job["usages"][0]["spool_id"], spool_id)
        self.assertFalse(job["cost_complete"])
        self.assertIsNone(job["material_cost"])
        self.store.update_spool(spool_id, {"unit_price_per_kg": 80})
        job = self.store.get_job(job["id"])
        self.assertTrue(job["cost_complete"])
        self.assertEqual(job["material_cost"], 8)

    def test_price_history_note_is_not_the_visible_label(self):
        spool = self.store.create_manual({
            "brand": "Bambu",
            "material": "PETG",
            "nominal_weight_g": 1000,
        })
        self.store.update_spool(spool["id"], {"unit_price_per_kg": 80})
        detail = self.store.get_spool(spool["id"])
        price_events = [event for event in detail["events"] if event["event_type"] == "price_set"]
        self.assertEqual(len(price_events), 1)
        self.assertNotEqual(price_events[0]["note"], "更新单价")


class RegressionSpoolLink(unittest.TestCase):
    def test_spool_id_survives_identical_report(self):
        payload = {"ams": {"ams": [{"id": "0", "tray": [{
            "id": "0", "state": 11, "tray_type": "PETG",
            "tag_uid": "AAAA000011112222", "tray_uuid": "B" * 32,
            "remain": 80, "tray_weight": "1000",
        }]}]}}
        tracker = SlotTracker()
        fed = tracker.feed(parse_slots(payload))
        fed["dirty"][0]["spool_id"] = 7
        tracker.feed(parse_slots(payload))
        self.assertEqual(tracker.prev["ams:0:tray:0"].get("spool_id"), 7)

    def test_same_tray_info_idx_matched_by_color(self):
        # 实机：官方 PETG Basic（GFG00）白、红、黑同时在 AMS 里。
        slots = [
            {"slot": "ams:0:tray:0", "occupied": True, "spool_id": 1, "tray_info_idx": "GFG00", "material": "PETG", "color": "FFFFFFFF"},
            {"slot": "ams:0:tray:1", "occupied": True, "spool_id": 2, "tray_info_idx": "GFG00", "material": "PETG", "color": "C12E1FFF"},
            {"slot": "ams:0:tray:2", "occupied": True, "spool_id": 3, "tray_info_idx": "GFG00", "material": "PETG", "color": "000000FF"},
        ]
        red = match_exact_filaments(
            [{"tray_info_idx": "GFG00", "material": "PETG", "color": "#C12E1F", "used_g": 12.5}],
            slots,
        )
        black = match_exact_filaments(
            [{"tray_info_idx": "GFG00", "material": "PETG", "color": "#000000", "used_g": 4}],
            slots,
        )
        white = match_exact_filaments(
            [{"tray_info_idx": "GFG00", "material": "PETG", "color": "#FFFFFF", "used_g": 1}],
            slots,
        )
        self.assertEqual(red[0]["spool_id"], 2)
        self.assertEqual(red[0]["grams"], 12.5)
        self.assertEqual(black[0]["spool_id"], 3)
        self.assertEqual(white[0]["spool_id"], 1)


if __name__ == "__main__":
    unittest.main()
