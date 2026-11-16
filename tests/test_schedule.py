import asyncio
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="webhook-p2s-test-"))

import app
from starlette.requests import Request


def window(start, end, events, weekdays=None, enabled=True):
    if weekdays is None:
        weekdays = [0, 1, 2, 3, 4, 5, 6]
    flags = {key: False for key in app.WINDOW_EVENTS}
    flags.update(events)
    return {
        "enabled": enabled,
        "start": start,
        "end": end,
        "weekdays": weekdays,
        "events": flags,
    }


def request(body):
    payload = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/api/config",
        "headers": [(b"content-type", b"application/json")],
    }
    return Request(scope, receive)


class ScheduleTests(unittest.TestCase):
    def test_version_is_single_semver_source(self):
        self.assertRegex(app.__version__, r"^\d+\.\d+\.\d+$")
        page = app.index().body.decode("utf-8")
        self.assertIn("v" + app.__version__, page)
        self.assertNotIn("{{APP_VERSION}}", page)
        status = app.api_status()
        self.assertEqual(status["version"], app.__version__)
        self.assertTrue(status["timezone"])

    def test_empty_windows_keep_previous_behavior(self):
        now = datetime(2026, 11, 16, 3, 15)
        for event in app.WINDOW_EVENTS:
            self.assertTrue(app.schedule_allows([], event, now))
            self.assertTrue(app.schedule_allows(None, event, now))
        self.assertEqual(app.push_gate(True, [], "start", now=now), "send")
        self.assertEqual(app.push_gate(False, [], "start", now=now), "disabled")

    def test_midnight_wrap_includes_both_ends(self):
        rules = [window("22:00", "07:00", {"start": True})]
        monday_night = datetime(2026, 11, 16, 22, 0)
        late = datetime(2026, 11, 16, 23, 59)
        midnight = datetime(2026, 11, 17, 0, 0)
        early = datetime(2026, 11, 17, 6, 30)
        end_minute = datetime(2026, 11, 17, 7, 0)
        after = datetime(2026, 11, 17, 7, 1)
        before = datetime(2026, 11, 16, 21, 59)
        for moment in (monday_night, late, midnight, early, end_minute):
            self.assertTrue(app.schedule_allows(rules, "start", moment))
        self.assertFalse(app.schedule_allows(rules, "start", after))
        self.assertFalse(app.schedule_allows(rules, "start", before))
        self.assertFalse(app.schedule_allows(rules, "finish", late))

    def test_wrap_weekday_follows_the_current_minute(self):
        rules = [window("22:00", "07:00", {"offline": True}, weekdays=[0])]
        self.assertTrue(app.schedule_allows(rules, "offline", datetime(2026, 11, 16, 23, 0)))
        self.assertTrue(app.schedule_allows(rules, "offline", datetime(2026, 11, 16, 6, 0)))
        self.assertFalse(app.schedule_allows(rules, "offline", datetime(2026, 11, 17, 6, 0)))

    def test_overlap_is_union_without_a_second_decision(self):
        both = [
            window("09:00", "12:00", {"start": True}),
            window("11:00", "18:00", {"finish": True}),
        ]
        at_overlap = datetime(2026, 11, 16, 11, 30)
        self.assertTrue(app.schedule_allows(both, "start", at_overlap))
        self.assertTrue(app.schedule_allows(both, "finish", at_overlap))
        self.assertFalse(app.schedule_allows(both, "failed", at_overlap))
        self.assertEqual(app.push_gate(True, both, "start", now=at_overlap), "send")
        self.assertFalse(app.schedule_allows(both, "start", datetime(2026, 11, 16, 13, 0)))
        self.assertTrue(app.schedule_allows(both, "finish", datetime(2026, 11, 16, 13, 0)))
        # 交界的这一分钟两边都算命中，结果仍是一次「允许」，不会变成两次推送。
        boundary = [
            window("09:00", "13:00", {"start": True}),
            window("13:00", "18:00", {"finish": True}),
        ]
        at_edge = datetime(2026, 11, 16, 13, 0)
        self.assertTrue(app.schedule_allows(boundary, "start", at_edge))
        self.assertTrue(app.schedule_allows(boundary, "finish", at_edge))
        self.assertEqual(app.push_gate(True, boundary, "start", now=at_edge), "send")

    def test_disabled_window_and_empty_weekdays_do_not_match(self):
        noon = datetime(2026, 11, 16, 12, 0)
        off = window("00:00", "00:00", {"start": True}, enabled=False)
        self.assertFalse(app.schedule_allows([off], "start", noon))
        no_days = window("00:00", "23:59", {"start": True}, weekdays=[])
        self.assertFalse(app.schedule_allows([no_days], "start", noon))

    def test_equal_start_and_end_means_all_day(self):
        rules = [window("08:00", "08:00", {"progress": True}, weekdays=[0])]
        self.assertTrue(app.schedule_allows(rules, "progress", datetime(2026, 11, 16, 8, 0)))
        self.assertTrue(app.schedule_allows(rules, "progress", datetime(2026, 11, 16, 23, 30)))
        self.assertFalse(app.schedule_allows(rules, "progress", datetime(2026, 11, 17, 12, 0)))
        self.assertFalse(app.schedule_allows(rules, "start", datetime(2026, 11, 16, 12, 0)))

    def test_master_switch_wins_and_force_bypasses_schedule(self):
        rules = [window("00:00", "00:00", {"failed": True})]
        now = datetime(2026, 11, 16, 12, 0)
        self.assertEqual(app.push_gate(False, rules, "failed", now=now), "disabled")
        self.assertEqual(app.push_gate(True, rules, "start", now=now), "window")
        self.assertEqual(app.push_gate(False, rules, "failed", force=True, now=now), "send")

    def test_normalize_wrap_times_and_drop_invalid(self):
        cleaned, err = app.normalize_windows([{
            "enabled": True,
            "start": "22：00",
            "end": "7:05:30",
            "weekdays": [1, 1, 8, "3"],
            "events": {"start": True, "finish": False},
        }], strict=True)
        self.assertIsNone(err)
        self.assertEqual(cleaned[0]["start"], "22:00")
        self.assertEqual(cleaned[0]["end"], "07:05")
        self.assertEqual(cleaned[0]["weekdays"], [1, 3])
        self.assertTrue(cleaned[0]["events"]["start"])
        self.assertFalse(cleaned[0]["events"]["pause"])
        _, err = app.normalize_windows(
            [{"start": "25:00", "end": "01:00", "events": {}}], strict=True
        )
        self.assertIn("HH:MM", err)
        kept, err = app.normalize_windows([
            {"start": "bad", "end": "01:00"},
            {"start": "01:00", "end": "02:00", "events": {"offline": True}},
        ], strict=False)
        self.assertIsNone(err)
        self.assertEqual(len(kept), 1)
        self.assertTrue(kept[0]["events"]["offline"])
        self.assertFalse(kept[0]["events"]["start"])
        too_many = [
            {"start": "01:00", "end": "02:00", "events": {"start": True}}
            for _ in range(app.MAX_WINDOWS + 1)
        ]
        _, err = app.normalize_windows(too_many, strict=True)
        self.assertIn(str(app.MAX_WINDOWS), err)

    def test_notify_sends_once_and_logs_schedule_skip(self):
        sent = []
        original_send = app.dingtalk_send
        original_thread = app.threading.Thread
        original_notify = json.loads(json.dumps(app.config["notify"]))
        original_camera = dict(app.config["camera"])
        original_ding = dict(app.config["dingtalk"])

        class SyncThread:
            def __init__(self, target=None, daemon=None):
                self._target = target

            def start(self):
                self._target()

        def fake_send(title, text, force=False):
            sent.append({"title": title, "text": text, "force": force})
            return True, "推送成功"

        app.dingtalk_send = fake_send
        app.threading.Thread = SyncThread
        app.events.clear()
        try:
            app.config["camera"]["enabled"] = False
            app.config["dingtalk"]["webhook"] = "https://example.invalid/hook"
            app.config["dingtalk"]["secret"] = "keep-me"
            app.config["notify"]["enabled"] = True
            app.config["notify"]["windows"] = []
            app.notify(
                "P2S_1", "开始打印", "正文",
                tag="start", event="start", with_media=True, printer_index=0,
            )
            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0]["title"], "P2S_1 开始打印")
            self.assertNotIn("keep-me", sent[0]["text"])

            sent.clear()
            app.events.clear()
            app.config["notify"]["windows"] = [window("00:00", "00:00", {"finish": True})]
            app.notify(
                "P2S_1", "开始打印", "正文",
                tag="start", event="start", with_media=True, printer_index=0,
            )
            self.assertEqual(sent, [])
            logged = list(app.events)
            self.assertTrue(any(item["type"] == "skip" and "不在推送时间段内，已跳过" in item["text"] for item in logged))
            self.assertTrue(all("keep-me" not in item["text"] for item in logged))

            sent.clear()
            app.config["notify"]["windows"] = [
                window("00:00", "00:00", {"start": True}),
                window("00:00", "00:00", {"start": True, "failed": True}),
            ]
            app.notify(
                "P2S_1", "开始打印", "正文",
                tag="start", event="start", with_media=True, printer_index=0,
            )
            self.assertEqual(len(sent), 1)
        finally:
            app.dingtalk_send = original_send
            app.threading.Thread = original_thread
            app.config["notify"] = original_notify
            app.config["camera"] = original_camera
            app.config["dingtalk"] = original_ding
            app.events.clear()

    def test_dingtalk_test_ignores_switch_and_windows(self):
        original = json.loads(json.dumps(app.config))
        try:
            app.config["notify"]["enabled"] = False
            app.config["notify"]["windows"] = [window("00:00", "00:01", {})]
            app.config["dingtalk"]["webhook"] = ""
            app.config["dingtalk"]["secret"] = "keep-me"
            result = app.api_test_dingtalk()
            self.assertFalse(result["ok"])
            self.assertNotIn("时间段", result["msg"])
            self.assertNotIn("停用", result["msg"])
            self.assertNotIn("keep-me", result["msg"])
        finally:
            app.config = original

    def test_save_windows_keeps_blank_secret(self):
        original = json.loads(json.dumps(app.config))
        try:
            app.config["dingtalk"]["webhook"] = "https://example.invalid/hook"
            app.config["dingtalk"]["secret"] = "keep-me"
            saved = asyncio.run(app.api_post_config(request({
                "dingtalk": {"webhook": "", "secret": "", "keyword": "打印"},
                "notify": {
                    "enabled": True,
                    "on_start": True,
                    "windows": [{
                        "enabled": True,
                        "start": "22:00",
                        "end": "07:00",
                        "weekdays": [0, 1, 2, 3, 4],
                        "events": {"start": True, "failed": True, "progress": False},
                    }],
                },
            })))
            self.assertTrue(saved["ok"])
            self.assertEqual(app.config["dingtalk"]["secret"], "keep-me")
            self.assertEqual(app.config["dingtalk"]["webhook"], "https://example.invalid/hook")
            stored = app.config["notify"]["windows"]
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0]["start"], "22:00")
            self.assertEqual(stored[0]["end"], "07:00")
            self.assertEqual(stored[0]["weekdays"], [0, 1, 2, 3, 4])
            self.assertTrue(stored[0]["events"]["failed"])
            self.assertFalse(stored[0]["events"]["progress"])
            view = app.api_get_config()
            blob = json.dumps(view, ensure_ascii=False)
            self.assertNotIn("keep-me", blob)
            self.assertNotIn("example.invalid", blob)
            self.assertEqual(view["dingtalk"]["secret"], "")
            self.assertTrue(view["dingtalk"]["secret_set"])
            self.assertTrue(view["dingtalk"]["webhook_set"])
            self.assertEqual(view["notify"]["windows"][0]["end"], "07:00")
            self.assertEqual(view["version"], app.__version__)

            rejected = asyncio.run(app.api_post_config(request({
                "notify": {"windows": [{"start": "99:00", "end": "01:00", "events": {}}]},
            })))
            self.assertFalse(rejected["ok"])
            self.assertEqual(app.config["dingtalk"]["secret"], "keep-me")
            self.assertEqual(app.config["notify"]["windows"][0]["start"], "22:00")

            cleared = asyncio.run(app.api_post_config(request({"notify": {"windows": []}})))
            self.assertTrue(cleared["ok"])
            self.assertEqual(app.config["notify"]["windows"], [])
            self.assertEqual(app.config["dingtalk"]["secret"], "keep-me")
        finally:
            app.config = original
            if app.CONFIG_PATH.exists():
                app.CONFIG_PATH.unlink()


if __name__ == "__main__":
    unittest.main()
