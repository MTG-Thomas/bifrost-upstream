"""Stage 3 canary durability: comparison, classification, and retry logic.

These run against the *runner module* only, with no production access: the point
is that a bad cycle becomes a classified record instead of an exception, and
that the comparison used for the evidence cannot silently pass.
"""
import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

CANARY = Path(__file__).resolve().parent.parent / "dev" / "staging" / "prod-canary.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("prod_canary_unit", CANARY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ComparisonTest(unittest.TestCase):
    def setUp(self):
        self.m = load_runner()

    def test_result_order_is_normalised_but_content_is_not(self):
        a = {"caller_mode": "picker", "organizations": [{"id": "1"}, {"id": "2"}]}
        b = {"organizations": [{"id": "2"}, {"id": "1"}], "caller_mode": "picker"}
        self.assertEqual(self.m.digest(a, sort_lists=True), self.m.digest(b, sort_lists=True))
        differing = {"caller_mode": "picker", "organizations": [{"id": "1"}, {"id": "3"}]}
        self.assertNotEqual(self.m.digest(a, sort_lists=True), self.m.digest(differing, sort_lists=True))

    def test_status_change_alone_is_visible(self):
        self.assertNotEqual(self.m.digest({"ok": True}, sort_lists=True),
                            self.m.digest(None, sort_lists=True))

    def test_key_order_does_not_change_the_digest(self):
        self.assertEqual(self.m.digest({"a": 1, "b": 2}), self.m.digest({"b": 2, "a": 1}))


class ClassificationTest(unittest.TestCase):
    def setUp(self):
        self.m = load_runner()

    def _phase(self, **record):
        record.setdefault("match", False)
        record.setdefault("ts", "2026-01-01T00:00:00Z")
        return self.m.classify(record)

    def test_each_failure_mode_has_a_distinct_phase(self):
        self.assertEqual(self._phase(match=True), "ok")
        self.assertEqual(self._phase(auth_failed=True), "session_expired")
        self.assertEqual(self._phase(api_unreachable=True), "api_unreachable")
        self.assertEqual(self._phase(workflow_missing=True), "workflow_missing")
        self.assertEqual(self._phase(module_pin_missing=True), "module_pin_missing")
        self.assertEqual(self._phase(cloud_http=403), "worker_gate_rejected")
        self.assertEqual(self._phase(cloud_http=0), "worker_unreachable")
        self.assertEqual(self._phase(cloud_http=200, cloud_error="TypeError"),
                         "cloud_execution_failed")
        self.assertEqual(self._phase(cloud_http=200), "mismatch")


class RetryTest(unittest.TestCase):
    def setUp(self):
        self.m = load_runner()

    def test_get_retries_a_transient_5xx_then_succeeds(self):
        calls = []

        class Resp(io.BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def fake_urlopen(req, timeout=None):
            calls.append(req.full_url)
            if len(calls) < 3:
                raise self.m.urllib.error.HTTPError(req.full_url, 503, "unavailable", {}, io.BytesIO(b""))
            return Resp(b'{"ok": true}')

        with patch.object(self.m.urllib.request, "urlopen", fake_urlopen), \
                patch.object(self.m.time, "sleep", lambda _s: None):
            status, body = self.m.call("GET", "https://example.invalid/x", retries=3)
        self.assertEqual(status, 200)
        self.assertEqual(body, {"ok": True})
        self.assertEqual(len(calls), 3)

    def test_post_is_not_retried(self):
        calls = []

        def fake_urlopen(req, timeout=None):
            calls.append(req.full_url)
            raise self.m.urllib.error.HTTPError(req.full_url, 503, "unavailable", {}, io.BytesIO(b""))

        with patch.object(self.m.urllib.request, "urlopen", fake_urlopen), \
                patch.object(self.m.time, "sleep", lambda _s: None):
            status, _ = self.m.call("POST", "https://example.invalid/x", body={}, retries=3)
        self.assertEqual(status, 503)
        self.assertEqual(len(calls), 1, "an engine attempt must never be double-dispatched")

    def test_transport_failure_is_reported_not_raised(self):
        def fake_urlopen(req, timeout=None):
            raise self.m.urllib.error.URLError("boom")

        with patch.object(self.m.urllib.request, "urlopen", fake_urlopen), \
                patch.object(self.m.time, "sleep", lambda _s: None):
            status, body = self.m.call("GET", "https://example.invalid/x", retries=1)
        self.assertEqual(status, 0)
        self.assertEqual(body, {"error": "transport"})

    def test_worker_calls_identify_themselves(self):
        seen = {}

        class Resp(io.BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def fake_urlopen(req, timeout=None):
            seen.update({k.lower(): v for k, v in req.header_items()})
            return Resp(b"{}")

        with patch.object(self.m.urllib.request, "urlopen", fake_urlopen):
            self.m.call("GET", "https://example.invalid/x")
        self.assertEqual(seen.get("user-agent"), "bifrost-canary/1.0",
                         "Cloudflare's edge rejects the stock urllib signature with 1010")


class SessionTest(unittest.TestCase):
    def setUp(self):
        self.m = load_runner()

    def test_refresh_rotation_is_persisted(self):
        state = {"access_token": "stale", "refresh_token": "r1"}
        saved = {}
        responses = {
            ("GET", f"{self.m.API}/auth/me"): (401, {}),
            ("POST", f"{self.m.API}/auth/refresh"): (200, {"access_token": "a2", "refresh_token": "r2"}),
        }

        def fake_call(method, url, **kwargs):
            return responses.get((method, url), (500, {}))

        with patch.object(self.m, "load_session", lambda: dict(state)), \
                patch.object(self.m, "save_session", lambda s: saved.update(s)), \
                patch.object(self.m, "call", fake_call):
            token, _ = self.m.access_token()
        self.assertEqual(token, "a2")
        self.assertEqual(saved.get("refresh_token"), "r2",
                         "a rotated refresh token must survive the process")


if __name__ == "__main__":
    unittest.main()
