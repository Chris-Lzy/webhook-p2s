import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="webhook-p2s-duration-"))

import app


def _elapsed_label(text):
    match = re.search(r"\*\*耗时\*\*：([^\n]+)", text or "")
    return match.group(1).strip() if match else ""


class PrintDurationTests(unittest.TestCase):
    """2026-11-23 钉钉完成通知耗时 61 小时与 64 小时，两次相差正好 3 小时 49 分。

    那是同一条 print_start_ts 在空闲之后没有重置。1% 进度推送原先不带耗时，
    只显示打印机报的剩余时间，所以看起来是正常的。
    """

    def setUp(self):
        self.pid = "p_duration"
        self.original_time = app.time.time
        self.original_config = app.json.loads(app.json.dumps(app.config))
        self.original_notify = app.notify
        self.original_enrich = app._enrich_job
        self.original_camera = app._enrich_camera
        self.clock = {"t": 1_700_000_000}
        self.notes = []
        app.time.time = lambda: self.clock["t"]
        app._enrich_job = lambda *args, **kwargs: None
        app._enrich_camera = lambda *args, **kwargs: None
        app.notify = self._capture_notify
        app.config["printers"] = [{
            "id": self.pid,
            "name": "P2SBambu_192.168.1.139",
            "host": "192.168.1.139",
            "sn": "TESTSN",
            "access_code": "test-code",
            "enabled": True,
        }]
        app.config["notify"]["enabled"] = True
        app.config["notify"]["on_start"] = True
        app.config["notify"]["on_finish"] = True
        app.config["notify"]["on_pause"] = False
        app.config["notify"]["on_resume"] = False
        app.config["notify"]["progress_step"] = 1
        app.config["camera"]["enabled"] = False
        app.statuses[self.pid] = app.blank_status()
        self.monitor = app.PrinterMonitor(self.pid)

    def tearDown(self):
        app.time.time = self.original_time
        app.config = self.original_config
        app.notify = self.original_notify
        app._enrich_job = self.original_enrich
        app._enrich_camera = self.original_camera
        app.statuses.pop(self.pid, None)
        app.monitors.pop(self.pid, None)

    def _capture_notify(self, pname, title, text, tag="print", event=None, with_media=False, printer_id=None, force=False):
        self.notes.append({"title": title, "text": text, "event": event, "tag": tag})

    def _push(self, **fields):
        self.monitor._handle({"print": fields})

    def _titles(self, event):
        return [note for note in self.notes if note["event"] == event]

    def test_new_job_after_prepare_does_not_keep_previous_start(self):
        base = self.clock["t"]
        task_a = "Vision_502-黑色POM-上护套板-SMD视觉"
        task_b = "3D打印-黑色-晒松-83G-10T-测试护套"
        stale = str(int(base))

        self._push(gcode_state="RUNNING", subtask_name=task_a, percent=0, gcode_start_time=stale)
        self.clock["t"] = base + 30 * 60
        self._push(gcode_state="RUNNING", percent=100, mc_percent=100)
        self._push(gcode_state="FINISH", percent=100, gcode_start_time=stale)
        self._push(gcode_state="IDLE")

        # 上一单结束后空闲约 61 小时，新任务先 PREPARE 再 RUNNING。
        # 报文里的 gcode_start_time 仍是上一单的时间戳。
        start_b = base + 61 * 3600 + 2 * 60
        self.clock["t"] = start_b
        self._push(
            gcode_state="PREPARE",
            subtask_name=task_b,
            gcode_file="/data/Metadata/plate_1.gcode",
            gcode_start_time=stale,
            percent=0,
        )
        self.clock["t"] = start_b + 2 * 60
        self._push(
            gcode_state="RUNNING",
            subtask_name=task_b,
            mc_percent=1,
            mc_remaining_time=38,
            gcode_start_time=stale,
        )
        self.clock["t"] = start_b + 20 * 60
        self._push(
            gcode_state="RUNNING",
            subtask_name=task_b,
            mc_percent=50,
            mc_remaining_time=20,
            gcode_start_time=stale,
        )
        self.clock["t"] = start_b + 40 * 60
        self._push(
            gcode_state="FINISH",
            subtask_name=task_b,
            mc_percent=100,
            gcode_start_time=stale,
        )

        progress = [note for note in self._titles("progress") if task_b in note["text"]]
        self.assertGreaterEqual(len(progress), 2, self.notes)
        one_pct = next(note for note in progress if note["title"] == "打印进度 1%")
        mid = next(note for note in progress if note["title"] == "打印进度 50%")
        finish = [note for note in self._titles("finish") if task_b in note["text"]]
        self.assertEqual(len(finish), 1, self.notes)
        self.assertEqual(_elapsed_label(one_pct["text"]), "2分钟")
        self.assertEqual(_elapsed_label(mid["text"]), "20分钟")
        self.assertEqual(_elapsed_label(finish[0]["text"]), "40分钟")
        for note in (one_pct, mid, finish[0]):
            self.assertNotIn("小时", note["text"])
            self.assertNotIn("61", _elapsed_label(note["text"]))

        status = next(item for item in app.api_status()["printers"] if item["id"] == self.pid)
        self.assertEqual(status["elapsed_mins"], 40)
        self.assertEqual(status["gcode_state"], "FINISH")
        self.clock["t"] = start_b + 40 * 60 + 10 * 3600
        status_later = next(item for item in app.api_status()["printers"] if item["id"] == self.pid)
        self.assertEqual(status_later["elapsed_mins"], 40)

        jobs = [job for job in app.get_store().list_jobs() if job.get("printer_id") == self.pid and job.get("task_name") == task_b]
        self.assertTrue(jobs, app.get_store().list_jobs())
        self.assertAlmostEqual(jobs[0]["duration_sec"], 40 * 60, delta=1)

    def test_same_task_name_reprint_and_running_switch(self):
        base = self.clock["t"]
        name = "测试护套"
        self._push(gcode_state="RUNNING", subtask_name=name, percent=0, gcode_start_time=str(base))
        self.clock["t"] = base + 10 * 60
        self._push(gcode_state="FINISH", subtask_name=name, gcode_start_time=str(base))
        start = base + 5 * 3600
        self.clock["t"] = start
        self._push(gcode_state="PREPARE", subtask_name=name, gcode_start_time=str(base), percent=0)
        self.clock["t"] = start + 12 * 60
        self._push(gcode_state="FINISH", subtask_name=name, gcode_start_time=str(base), percent=100)
        finish = [note for note in self._titles("finish") if "12分钟" in note["text"]]
        self.assertTrue(finish, self.notes)
        self.assertNotIn("小时", finish[-1]["text"])

        switched = app.blank_status()
        now = start + 3600
        app.apply_job_clock(switched, {
            "gcode_state": "RUNNING",
            "subtask_name": "第一单",
            "gcode_start_time": str(now),
        }, "", now)
        later = now + 10 * 60
        new_job = app.apply_job_clock(switched, {
            "gcode_state": "RUNNING",
            "subtask_name": "第二单",
            "gcode_start_time": str(now),
        }, "RUNNING", later)
        self.assertTrue(new_job)
        self.assertEqual(app.elapsed_minutes(switched["print_start_ts"], later), 0)
        done = later + 8 * 60
        app.apply_job_clock(switched, {
            "gcode_state": "RUNNING",
            "subtask_name": "第二单",
            "gcode_start_time": str(now),
        }, "RUNNING", done)
        self.assertEqual(app.elapsed_minutes(switched["print_start_ts"], done), 8)

    def test_pause_does_not_reset_and_milliseconds_are_seconds(self):
        base = self.clock["t"]
        self._push(gcode_state="RUNNING", subtask_name="暂停件", percent=0, gcode_start_time=str(base))
        self.clock["t"] = base + 10 * 60
        self._push(gcode_state="PAUSE", subtask_name="暂停件", percent=10)
        self.clock["t"] = base + 40 * 60
        self._push(gcode_state="RUNNING", subtask_name="暂停件", percent=10)
        self.clock["t"] = base + 45 * 60
        self._push(gcode_state="RUNNING", subtask_name="暂停件", mc_percent=20, mc_remaining_time=30)
        progress = [note for note in self._titles("progress") if "暂停件" in note["text"]]
        twenty = next(note for note in progress if note["title"] == "打印进度 20%")
        self.assertEqual(_elapsed_label(twenty["text"]), "45分钟")
        starts = [note for note in self.notes if note["title"] == "开始打印"]
        self.assertEqual(len(starts), 1)
        status = next(item for item in app.api_status()["printers"] if item["id"] == self.pid)
        self.assertEqual(status["elapsed_mins"], 45)
        self.assertEqual(status["gcode_state"], "RUNNING")

        joined = app.blank_status()
        now = base + 100 * 3600
        started = now - 40 * 60
        app.apply_job_clock(joined, {
            "gcode_state": "RUNNING",
            "subtask_name": "中途接入",
            "gcode_start_time": str(started * 1000),
            "mc_percent": 1,
        }, "", now)
        self.assertEqual(app.elapsed_minutes(joined["print_start_ts"], now), 40)
        self.assertIsNone(app.parse_gcode_start_time("0", now))
        self.assertIsNone(app.parse_gcode_start_time("120", now))

    def test_late_task_name_does_not_reset_clock(self):
        status = app.blank_status()
        now = 1_700_000_000
        app.apply_job_clock(status, {"gcode_state": "RUNNING", "percent": 0}, "", now)
        filled = app.apply_job_clock(status, {
            "gcode_state": "RUNNING",
            "subtask_name": "后补上的任务名",
            "mc_percent": 1,
        }, "RUNNING", now + 5 * 60)
        self.assertFalse(filled)
        self.assertEqual(app.elapsed_minutes(status["print_start_ts"], now + 5 * 60), 5)

    def test_status_hides_elapsed_while_idle(self):
        base = self.clock["t"]
        self._push(gcode_state="RUNNING", subtask_name="短任务", percent=1, gcode_start_time=str(base))
        self.clock["t"] = base + 6 * 60
        self._push(gcode_state="FINISH", subtask_name="短任务")
        self._push(gcode_state="IDLE")
        self.clock["t"] = base + 30 * 3600
        status = next(item for item in app.api_status()["printers"] if item["id"] == self.pid)
        self.assertIsNone(status["elapsed_mins"])
        self.assertEqual(status["gcode_state"], "IDLE")


if __name__ == "__main__":
    unittest.main()
