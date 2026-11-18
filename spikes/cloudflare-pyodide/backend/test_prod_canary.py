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


class AttributionTest(unittest.TestCase):
    """Upgrade attribution must name the code that actually ran."""

    def setUp(self):
        self.m = load_runner()

    def test_worker_identity_is_the_engine_not_the_minimal_child(self):
        def fake_call(method, url, **kwargs):
            if url.endswith("/__code_digest"):
                return 200, {"generic_code_sha256": "minimal", "engine_code_sha256": "engine"}
            return 200, {"version": "2.0.1"}

        with patch.object(self.m, "call", fake_call), \
                patch.object(self.m, "worker_secret", lambda: "s"):
            info = self.m.build_info()
        self.assertEqual(info["worker_code_sha256"], "engine")

    def test_supervisor_and_deployment_are_attributed(self):
        def fake_call(method, url, **kwargs):
            if url.endswith("/__code_digest"):
                return 200, {"engine_code_sha256": "engine", "supervisor_code_sha256": "sup",
                             "worker_version_id": "v1"}
            return 200, {"version": "2.0.1"}

        with patch.object(self.m, "call", fake_call), \
                patch.object(self.m, "worker_secret", lambda: "s"):
            info = self.m.build_info()
        self.assertEqual((info["worker_supervisor_sha256"], info["worker_version_id"]), ("sup", "v1"))

    def test_supervisor_only_change_is_its_own_event(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = {"BASE": str(base), "OUT": str(base / "runs.jsonl"),
                     "EVENTS": str(base / "events.jsonl"), "STATUS": str(base / "status.json"),
                     "ENABLED": str(base / "ENABLED"), "LOCK": str(base / ".lock")}
            (base / "ENABLED").write_text("1")
            fake_fcntl = type(sys)("fcntl")
            fake_fcntl.LOCK_EX, fake_fcntl.LOCK_NB = 2, 4
            fake_fcntl.flock = lambda *_a: None
            cycles = iter([("sup-a", "v1"), ("sup-b", "v2")])

            def cycle():
                sup, version = next(cycles)
                return {"ts": "t", "match": True, "phase": "ok", "worker_code_sha256": "engine",
                        "worker_supervisor_sha256": sup, "worker_version_id": version}

            with patch.dict(sys.modules, {"fcntl": fake_fcntl}), \
                    patch.multiple(self.m, one_cycle=cycle, **paths), \
                    patch("builtins.print"):
                self.m.main()
                self.m.main()
            events = [json.loads(line) for line in (base / "events.jsonl").read_text().splitlines()]
        self.assertEqual([e["kind"] for e in events], ["worker_supervisor_changed", "worker_deployed"])

    def test_module_pin_transition_is_recorded(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = {"BASE": str(base), "OUT": str(base / "runs.jsonl"),
                     "EVENTS": str(base / "events.jsonl"), "STATUS": str(base / "status.json"),
                     "ENABLED": str(base / "ENABLED"), "LOCK": str(base / ".lock")}
            (base / "ENABLED").write_text("1")
            fake_fcntl = type(sys)("fcntl")
            fake_fcntl.LOCK_EX, fake_fcntl.LOCK_NB = 2, 4
            fake_fcntl.flock = lambda *_a: None
            pins = iter(["pin-a", "pin-b"])

            def cycle():
                return {"ts": "t", "match": True, "phase": "ok", "module_sha256": next(pins)}

            with patch.dict(sys.modules, {"fcntl": fake_fcntl}), \
                    patch.multiple(self.m, one_cycle=cycle, **paths), \
                    patch("builtins.print"):
                self.m.main()
                self.m.main()
            events = [json.loads(line) for line in (base / "events.jsonl").read_text().splitlines()]
        self.assertEqual([(e["kind"], e["from"], e["to"]) for e in events],
                         [("module_pin_changed", "pin-a", "pin-b")])


class SupervisorFailureTest(unittest.TestCase):
    def setUp(self):
        self.m = load_runner()

    def test_supervisor_failure_is_an_execution_failure_not_a_mismatch(self):
        payload = {"phase": "dynamic-runtime", "error_type": "Error", "api_calls": []}
        with patch.object(self.m, "call", lambda *a, **k: (502, payload)), \
                patch.object(self.m, "worker_secret", lambda: "s"):
            cloud = self.m.run_cloud("tok", {"id": "u", "email": "e"}, "pin")
        self.assertEqual(cloud["error"], "Error")
        record = {"match": False, "cloud_http": 502, "cloud_error": cloud["error"]}
        self.assertEqual(self.m.classify(record), "cloud_execution_failed")


class TimingTest(unittest.TestCase):
    def setUp(self):
        self.m = load_runner()

    def test_cloud_cold_start_breakdown_is_kept(self):
        payload = {"output": {"status": "Success", "result": {}}, "worker_reused": False,
                   "timings": {"host_ms": 6180, "child_ms": 6100,
                               "child": {"first_request": True, "engine_ms": 900, "served": 1}}}
        with patch.object(self.m, "call", lambda *a, **k: (200, payload)), \
                patch.object(self.m, "worker_secret", lambda: "s"):
            cloud = self.m.run_cloud("tok", {"id": "u", "email": "e"}, "pin")
        self.assertEqual(cloud["timings"], {"host_ms": 6180, "child_ms": 6100,
                                            "first_request": True, "engine_ms": 900,
                                            "api_ms": 0, "api_count": 0})
        self.assertIsNone(cloud["warm"])

    def test_per_call_timings_keep_routes_not_identifiers(self):
        org = "3f2c1a9e-1b2c-4d5e-8f90-a1b2c3d4e5f6"
        calls = [
            {"method": "GET", "path": f"/api/sdk/modules/_solutions/{org}/functions/catalog.py",
             "status": 200, "token_ms": 3, "ms": 41},
            {"method": "GET", "path": f"/api/organizations/{org}", "status": 200, "token_ms": 2, "ms": 120},
            {"method": "GET", "host": "api.example.com", "path": "/secret/path", "status": 200, "ms": 9},
        ]
        payload = {"output": {"status": "Success", "result": {}},
                   "timings": {"host_ms": 300, "child_ms": 280, "api_calls": calls,
                               "child": {"first_request": False, "engine_ms": 250}}}
        with patch.object(self.m, "call", lambda *a, **k: (200, payload)), \
                patch.object(self.m, "worker_secret", lambda: "s"):
            cloud = self.m.run_cloud("tok", {"id": "u", "email": "e"}, "pin")
        self.assertEqual(cloud["api_calls"], [
            ["GET", "/api/sdk/modules/_solutions/{id}/functions/catalog.py", 200, 41, 3],
            ["GET", "/api/organizations/{id}", 200, 120, 2],
            ["GET", "api.example.com", 200, 9, None],
        ])
        self.assertEqual(cloud["timings"]["api_ms"], 170)
        self.assertNotIn(org, json.dumps(cloud))

    def test_cold_path_warm_up_is_kept(self):
        payload = {"output": {"status": "Success", "result": {}},
                   "timings": {"host_ms": 4000, "child_ms": 300,
                               "warm": {"child_ms": 3500, "first_request": True, "prime_listed": 12,
                                        "primed": 12, "origin_ms": 40, "warm_attempts": 1,
                                        "prime_skipped": []},
                               "child": {"first_request": False, "engine_ms": 250}}}
        with patch.object(self.m, "call", lambda *a, **k: (200, payload)), \
                patch.object(self.m, "worker_secret", lambda: "s"):
            cloud = self.m.run_cloud("tok", {"id": "u", "email": "e"}, "pin")
        self.assertEqual(cloud["warm"], {"child_ms": 3500, "first_request": True, "prime_listed": 12,
                                         "primed": 12, "origin_ms": 40, "warm_attempts": 1})


if __name__ == "__main__":
    unittest.main()
