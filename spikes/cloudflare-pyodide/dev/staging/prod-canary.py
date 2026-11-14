#!/usr/bin/env python3
"""Stage 3 shadow canary (runs on the production host, not in the app stack).

Runs ONE read-only production workflow twice: on the production Linux worker
(via the public API) and on the deployed Cloudflare per-tenant worker. Compares
status and result, then appends a hash-only record.

Durable across days and prod image upgrades:
  * GETs retry with backoff (deploy windows, transient 5xx); POSTs do not, so an
    engine attempt is never double-dispatched.
  * Every record carries the prod build id and the worker's code digest, so a
    mismatch can be attributed to a prod upgrade vs a stale canary runtime.
  * Upgrade transitions are written once to events.jsonl.
  * Nothing raises out of main(): every outcome becomes a classified record, so
    the timer keeps filing evidence instead of dying on one bad cycle.
  * An flock prevents overlapping runs if a cycle hangs.

No customer data is stored: each result is reduced to a canonical sha256 and an
organization count.

Kill switch: remove /opt/bifrost-canary/ENABLED, or stop the timer. The worker
also fails closed when SPIKE_EXPIRES_AT lapses or SPIKE_AUTH rotates.

Install (production):
  /opt/bifrost-canary/canary.py           this file, 0700
  /etc/bifrost-canary/last.json           device-flow session, 0600
  /etc/bifrost-canary/worker-secret       canary worker SPIKE_AUTH, 0600
  /opt/bifrost-canary/ENABLED             presence enables runs
  /opt/bifrost-canary/runs.jsonl          evidence, appended
  /opt/bifrost-canary/events.jsonl        build/runtime transitions
  /opt/bifrost-canary/status.json         latest summary
"""
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

API = "http://127.0.0.1:3000"
WORKER = "https://bifrost-pyodide-canary-ddf316c1.midtown-technology-group.workers.dev"
SESSION = "/etc/bifrost-canary/last.json"
WORKER_SECRET = "/etc/bifrost-canary/worker-secret"
BASE = "/opt/bifrost-canary"
OUT = f"{BASE}/runs.jsonl"
EVENTS = f"{BASE}/events.jsonl"
STATUS = f"{BASE}/status.json"
ENABLED = f"{BASE}/ENABLED"
LOCK = f"{BASE}/.lock"
WORKFLOW = "docs_list_organizations"
SOLUTION = "44832b44-87db-4837-afab-b22fc47f88db"
# The dispatcher passes the LOGICAL path plus a solution scope; the child maps it
# to `_solutions/<id>/functions/...` inside get_module_sync. Passing the
# already-scoped path doubles the prefix and resolves to nothing.
LOGICAL = "functions/catalog.py"
MODULE = f"_solutions/{SOLUTION}/{LOGICAL}"
UA = "bifrost-canary/1.0"


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def call(method, url, *, token=None, body=None, headers=None, timeout=60,
         retries=0):
    """One HTTP call. Retries are GET-only by construction.

    An engine attempt must never be dispatched twice, so a non-GET ignores
    `retries` even if a future caller passes it.
    """
    if method.upper() != "GET":
        retries = 0
    data = None
    # Cloudflare's edge blocks the stock Python-urllib signature with 1010 on
    # workers.dev; any explicit UA passes, so this client identifies itself.
    hdrs = {"User-Agent": UA, **dict(headers or {})}
    if body is not None:
        data = json.dumps(body).encode()
        hdrs["Content-Type"] = "application/json"
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    attempt = 0
    while True:
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                parsed = json.loads(raw) if raw else None
            except Exception:
                parsed = {"raw": raw[:200].decode("utf-8", "replace")}
            if not (retries and e.code >= 500 and attempt < retries):
                return e.code, parsed
            delay = 2 * (attempt + 1)
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if not (retries and attempt < retries):
                return 0, {"error": "transport"}
            delay = 2 * (attempt + 1)
        attempt += 1
        time.sleep(delay)


def load_session():
    with open(SESSION) as f:
        return json.load(f)


def save_session(s):
    tmp = SESSION + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(s, f)
    os.replace(tmp, SESSION)


def access_token():
    """Return a live access token, refreshing and persisting rotation."""
    s = load_session()
    tok = s.get("access_token")
    if tok and call("GET", f"{API}/auth/me", token=tok, timeout=20, retries=1)[0] == 200:
        return tok, s
    rt = s.get("refresh_token")
    if not rt:
        raise RuntimeError("no refresh token on the box")
    status, body = call("POST", f"{API}/auth/refresh", body={"refresh_token": rt},
                        timeout=30, retries=0)
    if status != 200 or not (body or {}).get("access_token"):
        raise RuntimeError(f"refresh failed: {status}")
    s["access_token"] = body["access_token"]
    if body.get("refresh_token"):
        s["refresh_token"] = body["refresh_token"]
    save_session(s)
    return s["access_token"], s


def canonical(value, *, sort_lists=False):
    if isinstance(value, list):
        items = [canonical(v, sort_lists=sort_lists) for v in value]
        if sort_lists:
            items.sort()
        return "[" + ",".join(items) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(k) + ":" + canonical(value[k], sort_lists=sort_lists)
            for k in sorted(value)
        ) + "}"
    return json.dumps(value)


def digest(value, *, sort_lists=False):
    return hashlib.sha256(canonical(value, sort_lists=sort_lists).encode()).hexdigest()


def build_info():
    """Prod build id + worker runtime digest: attributes a mismatch to a source."""
    _, version = call("GET", f"{API}/api/version", timeout=20, retries=1)
    _, code = call("GET", f"{WORKER}/__code_digest",
                   headers={"X-Spike-Auth": worker_secret()}, timeout=20, retries=1)
    return {
        "api_version": (version or {}).get("version") if isinstance(version, dict) else None,
        "worker_code_sha256": (code or {}).get("generic_code_sha256") if isinstance(code, dict) else None,
    }


def worker_secret():
    with open(WORKER_SECRET) as f:
        return f.read().strip()


def run_linux(token, workflow_id):
    started = time.time()
    status, body = call("POST", f"{API}/api/workflows/execute", token=token,
                        body={"workflow_id": workflow_id, "sync": False}, timeout=60)
    if status != 200 or not (body or {}).get("execution_id"):
        return {"status": f"SubmitFailed({status})", "result": None, "http": status,
                "ms": int((time.time() - started) * 1000)}
    exec_id = body["execution_id"]
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(2)
        st, ex = call("GET", f"{API}/api/executions/{exec_id}", token=token,
                      timeout=30, retries=1)
        if st == 200 and ex and ex.get("status") not in ("Pending", "Running", None):
            return {"status": ex.get("status"), "result": ex.get("result"),
                    "error": ex.get("error") or ex.get("error_message"),
                    "ms": int((time.time() - started) * 1000)}
    return {"status": "Timeout", "result": None, "ms": int((time.time() - started) * 1000)}


def run_cloud(token, me, pin):
    started = time.time()
    org_id = me.get("organization_id")
    context = {
        "caller": {"user_id": me["id"], "email": me["email"],
                   "name": me.get("name") or me["email"]},
        "organization_id": org_id,
        "organization": ({"id": org_id, "name": "Provider", "is_active": True,
                          "is_provider": True} if org_id else None),
        "name": WORKFLOW,
        "function_name": WORKFLOW,
        "file_path": LOGICAL,
        "content_hash": pin,
        "solution_id": SOLUTION,
        "engine_token": token,
        "transient": True,
        "parameters": {},
    }
    body = {
        "tenant_id": "prod-canary",
        "action": "engine",
        "execution_id": str(uuid.uuid4()),
        "requirements": {"modules": [MODULE]},
        "manifest": {MODULE: pin},
        "context": context,
        "public_url": "https://bifrost.midtowntg.com",
        "egress": {"hosts": []},
    }
    status, payload = call("POST", WORKER, body=body,
                           headers={"X-Spike-Auth": worker_secret()}, timeout=300)
    out = (payload or {}).get("output") or {}
    return {
        "http": status,
        "phase": (payload or {}).get("phase"),
        "status": out.get("status") or (payload or {}).get("phase"),
        "result": out.get("result"),
        "error": out.get("error_type") or out.get("error_message"),
        "reused": (payload or {}).get("worker_reused"),
        "queue_ms": (payload or {}).get("queue_wait_ms"),
        "ms": int((time.time() - started) * 1000),
    }


def classify(record):
    """One word for what kind of cycle this was, so failures are greppable."""
    if record["match"]:
        return "ok"
    if record.get("auth_failed"):
        return "session_expired"
    if record.get("api_unreachable"):
        return "api_unreachable"
    if record.get("workflow_missing"):
        return "workflow_missing"
    if record.get("module_pin_missing"):
        return "module_pin_missing"
    http = record.get("cloud_http")
    if http == 403:
        return "worker_gate_rejected"
    if http == 0:
        return "worker_unreachable"
    if record.get("cloud_error"):
        return "cloud_execution_failed"
    return "mismatch"


def append_jsonl(path, payload):
    with open(path, "a") as f:
        f.write(json.dumps(payload) + "\n")


def record_event(kind, detail):
    append_jsonl(EVENTS, {"ts": now_iso(), "kind": kind, **detail})


def write_status(record):
    rows = 0
    consecutive = 0
    try:
        with open(OUT) as f:
            for line in f:
                rows += 1
    except FileNotFoundError:
        pass
    # Consecutive failures come from the tail, newest last.
    try:
        with open(OUT) as f:
            tail = f.readlines()[-20:]
        for line in reversed(tail):
            if json.loads(line).get("match"):
                break
            consecutive += 1
    except FileNotFoundError:
        pass
    status = {
        "updated_at": record["ts"],
        "phase": record["phase"],
        "match": record["match"],
        "runs": rows,
        "consecutive_failures": consecutive,
        "api_version": record.get("api_version"),
        "worker_code_sha256": record.get("worker_code_sha256"),
        "last": {k: record.get(k) for k in (
            "linux_status", "cloud_status", "org_count", "linux_ms", "cloud_ms",
            "worker_reused", "cloud_http", "cloud_error", "linux_error")},
    }
    tmp = STATUS + ".tmp"
    with open(tmp, "w") as f:
        json.dump(status, f, indent=1)
    os.replace(tmp, STATUS)


def one_cycle():
    record = {"ts": now_iso(), "workflow": WORKFLOW, "solution": SOLUTION}
    try:
        info = build_info()
    except Exception:
        info = {}
    record.update(info)

    try:
        token, _ = access_token()
    except Exception as exc:
        record.update({"match": False, "auth_failed": True,
                       "linux_status": None, "cloud_status": None,
                       "error": str(exc)[:160]})
        record["phase"] = classify(record)
        return record

    status, me = call("GET", f"{API}/auth/me", token=token, timeout=30, retries=1)
    if status != 200:
        record.update({"match": False, "auth_failed": True,
                       "linux_status": None, "cloud_status": None,
                       "error": f"auth/me {status}"})
        record["phase"] = classify(record)
        return record

    status, wfs = call("GET", f"{API}/api/workflows", token=token, timeout=60, retries=1)
    if status != 200:
        record.update({"match": False, "api_unreachable": True,
                       "linux_status": None, "cloud_status": None,
                       "error": f"workflows {status}"})
        record["phase"] = classify(record)
        return record
    rows = wfs if isinstance(wfs, list) else (wfs or {}).get("workflows") or []
    match = [w for w in rows if w.get("name") == WORKFLOW]
    if not match:
        record.update({"match": False, "workflow_missing": True,
                       "linux_status": None, "cloud_status": None})
        record["phase"] = classify(record)
        return record
    workflow_id = match[0]["id"]

    status, module = call("GET", f"{API}/api/sdk/modules/{MODULE}", token=token,
                          timeout=60, retries=1)
    pin = (module or {}).get("hash")
    if status != 200 or not pin:
        record.update({"match": False, "module_pin_missing": True,
                       "linux_status": None, "cloud_status": None,
                       "error": f"module pin {status}"})
        record["phase"] = classify(record)
        return record
    record["module_sha256"] = pin

    linux = run_linux(token, workflow_id)
    cloud = run_cloud(token, me, pin)

    same_status = linux["status"] == cloud["status"]
    same_result = (canonical(linux["result"], sort_lists=True)
                   == canonical(cloud["result"], sort_lists=True))
    orgs = (linux.get("result") or {}).get("organizations")
    record.update({
        "linux_status": linux["status"],
        "cloud_status": cloud["status"],
        "match": bool(same_status and same_result),
        "same_status": same_status,
        "same_result": same_result,
        "linux_result_sha256": (digest(linux["result"], sort_lists=True)
                                if linux["result"] is not None else None),
        "cloud_result_sha256": (digest(cloud["result"], sort_lists=True)
                                if cloud["result"] is not None else None),
        "org_count": len(orgs) if isinstance(orgs, list) else None,
        "linux_ms": linux["ms"],
        "cloud_ms": cloud["ms"],
        "worker_reused": cloud.get("reused"),
        "cloud_http": cloud.get("http"),
        "cloud_phase": cloud.get("phase"),
        "cloud_error": (str(cloud["error"])[:160] if cloud.get("error") else None),
        "linux_error": (str(linux["error"])[:160] if linux.get("error") else None),
    })
    record["phase"] = classify(record)
    return record


def main():
    if not os.path.exists(ENABLED):
        print("DISABLED")
        return 0
    os.makedirs(BASE, exist_ok=True)
    # Linux-only, imported where used so the module stays importable (and
    # testable) on a workstation.
    import fcntl

    lock = open(LOCK, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("SKIPPED (previous run still active)")
        return 0

    record = one_cycle()
    append_jsonl(OUT, record)
    previous = None
    try:
        with open(STATUS) as f:
            previous = json.load(f)
    except Exception:
        previous = None
    if previous:
        for field, kind in (("api_version", "prod_build_changed"),
                            ("worker_code_sha256", "worker_runtime_changed"),
                            ("module_sha256", "module_pin_changed")):
            if record.get(field) and previous.get(field) and record[field] != previous[field]:
                record_event(kind, {"field": field, "from": previous[field], "to": record[field]})
    write_status(record)
    print(json.dumps({k: record.get(k) for k in (
        "phase", "linux_status", "cloud_status", "org_count", "linux_ms",
        "cloud_ms", "worker_reused", "cloud_http")}))
    return 0 if record["match"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - a crash must still leave evidence
        try:
            append_jsonl(OUT, {"ts": now_iso(), "workflow": WORKFLOW, "match": False,
                               "phase": "runner_error",
                               "error": f"{type(exc).__name__}: {str(exc)[:160]}"})
        except Exception:
            pass
        print("CANARY ERROR", type(exc).__name__, str(exc)[:200])
        sys.exit(2)
