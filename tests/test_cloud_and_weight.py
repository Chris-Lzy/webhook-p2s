import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as monitor_app  # noqa: E402
import bambu_cloud  # noqa: E402
from filament import estimate_grams_from_meters, format_job_notify_lines, match_exact_filaments  # noqa: E402
from filesource import parse_slice_info_xml  # noqa: E402
from store import Store  # noqa: E402


class BambuCloudTests(unittest.TestCase):
    def test_parse_and_match_task_and_filaments(self):
        payload = {
            "hits": [
                {
                    "id": "101",
                    "title": "BenchMark_PLA",
                    "designTitle": "BenchMark",
                    "deviceId": "01P00A000000001",
                    "weight": 23.63,
                    "length": 7900,
                    "costTime": 1800,
                    "startTime": "2026-11-27T02:00:00Z",
                    "endTime": "2026-11-27T02:30:00Z",
                    "amsDetailMapping": [
                        {"filamentType": "PLA", "sourceColor": "FF0000FF", "weight": 15.63, "filamentId": "GFA00"},
                        {"filamentType": "PETG", "sourceColor": "#FFFFFF", "weight": 8.0, "filamentId": "GFG00"},
                    ],
                }
            ]
        }
        tasks = bambu_cloud.parse_tasks(payload)
        self.assertEqual(len(tasks), 1)
        matched = bambu_cloud.match_task(
            tasks, "01p00a000000001", tasks[0]["start_ts"] + 30, None, "BenchMark_PLA"
        )
        self.assertIsNotNone(matched)
        fils = bambu_cloud.filaments_from_task(matched)
        self.assertEqual(len(fils), 2)
        self.assertAlmostEqual(sum(f["used_g"] for f in fils), 23.63, places=2)
        self.assertEqual(fils[0]["color"], "#FF0000")

    def test_filaments_fallback_when_ams_detail_mismatch(self):
        task = {
            "weight": 30.0,
            "length": 10000,
            "filaments": [{"type": "PLA", "color": "#112233", "g": 10.0}],
        }
        fils = bambu_cloud.filaments_from_task(task)
        self.assertEqual(len(fils), 1)
        self.assertAlmostEqual(fils[0]["used_g"], 30.0, places=2)
        self.assertAlmostEqual(fils[0]["used_m"], 10.0, places=2)

    def test_jwt_expiry(self):
        payload = base64.urlsafe_b64encode(json.dumps({"exp": 1800000000}).encode()).decode().rstrip("=")
        token = f"hdr.{payload}.sig"
        self.assertEqual(bambu_cloud.jwt_expiry(token), 1800000000.0)


class WeightEnhancementTests(unittest.TestCase):
    def test_slice_info_plate_weight_fallback(self):
        xml = b"""
        <config>
          <plate>
            <metadata key="index" value="1"/>
            <metadata key="weight" value="42.5"/>
            <filament id="1" type="PLA" color="#00FF00" used_m="5.0"/>
            <filament id="2" type="PLA" color="#0000FF" used_m="15.0"/>
          </plate>
        </config>
        """
        rows = parse_slice_info_xml(xml, plate="1")
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(rows[0]["used_g"] + rows[1]["used_g"], 42.5, places=2)
        self.assertAlmostEqual(rows[0]["used_g"], 10.625, places=3)

    def test_match_exact_filaments_estimates_from_meters(self):
        est = estimate_grams_from_meters(10.0, "PLA")
        self.assertIsNotNone(est)
        self.assertGreater(est, 25.0)
        rows = match_exact_filaments(
            [{"material": "PLA", "color": "#FFFFFF", "used_g": None, "used_m": 10.0, "tray_info_idx": ""}],
            [],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "estimated")
        self.assertAlmostEqual(rows[0]["grams"], est, places=3)

    def test_format_job_notify_lines(self):
        job = {
            "usage_source": "exact",
            "material_cost": 1.42,
            "electricity_cost": 0.4,
            "labor_cost": None,
            "usages": [
                {"material": "PLA", "color": "#111111", "grams": 23.63, "source": "exact"},
            ],
        }
        txt = format_job_notify_lines(job)
        self.assertIn("23.63 g（精确）", txt)
        self.assertIn("PLA #111111 23.63g", txt)
        self.assertIn("¥1.82", txt)

    def test_failed_job_scales_planned_exact_weight_by_percent(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "test.db", Path(tmp) / "img")
            try:
                slots = [{
                    "slot": "A1", "occupied": True, "material": "PLA", "color": "#FFFFFF",
                    "tag_uid": "T1", "tray_uuid": "U1", "tray_info_idx": "GFA00",
                    "remain_pct": 80, "remain_g": None, "remain_g_fresh": False, "nominal_weight_g": 1000.0,
                }]
                job_id = store.open_job("p_1", "P2S_1", "FailTest", "fail.3mf", slots, "A1", 10, 100, 25)
                store.apply_file_fetch(
                    job_id,
                    [{"material": "PLA", "color": "#FFFFFF", "used_g": 100.0, "used_m": 30.0, "tray_info_idx": "GFA00"}],
                    None, "", "cloud_ok", slots,
                )
                closed = store.close_job("p_1", "failed", slots, "A1", None, None)
                self.assertEqual(closed["usage_source"], "estimated")
                self.assertAlmostEqual(closed["usages"][0]["grams"], 25.0, places=2)
            finally:
                store.conn.close()

    def test_finish_job_archive_uses_cloud_and_formats_notify(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            test_store = Store(tmp_path / "test.db", tmp_path / "img")
            with monitor_app.lock:
                old_cfg = json.loads(json.dumps(monitor_app.config))
                monitor_app.config["printers"] = [
                    {"id": "p_1", "name": "P2S_1", "host": "1.2.3.4", "sn": "SN123", "access_code": "12345678", "enabled": True}
                ]
                monitor_app.config["cloud"] = {"enabled": True, "region": "cn", "token": "tok"}
                monitor_app.config["notify"]["on_finish"] = True
            try:
                fake_fils = [{"material": "PLA", "color": "#000000", "used_g": 18.5, "used_m": None, "tray_info_idx": "", "filament_id": "1"}]
                with patch.object(monitor_app, "get_store", return_value=test_store), \
                     patch.object(bambu_cloud, "lookup_job_metadata", return_value=({"id": "1"}, fake_fils, None, "", "cloud_ok")), \
                     patch.object(monitor_app, "_enrich_camera"):
                    monitor_app.begin_job_archive("p_1")
                    job = monitor_app.finish_job_archive("p_1", "finished")
                    self.assertIsNotNone(job)
                    self.assertEqual(job["usage_source"], "exact")
                    lines = format_job_notify_lines(job)
                    self.assertIn("18.50 g（精确）", lines)
            finally:
                test_store.conn.close()
                with monitor_app.lock:
                    monitor_app.config = old_cfg


if __name__ == "__main__":
    unittest.main()
