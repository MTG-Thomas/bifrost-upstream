"""Exercise the complete combined qualification using the local provider."""

import argparse
import ipaddress
import json
import os
import re
import socketserver
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID

from phase5a import approve_workflow
from phase6 import seed_solution_workflows

HERE = Path(__file__).resolve().parent
MANAGEMENT = "local-rehearsal-management-token"
ACTIVE_JOB = {"queued", "running", "waiting"}
WORKERS_OVERFLOW_AGE_SECONDS = 2
ENABLE_RESULT_SECONDS = 180
CONTAINERS_ACTIVE_SECONDS = 20 * 60
# Match cloudflare_containers_rest.py (copy and settlement) and catalog.py
# (capacity.cloudflare_enable). A retry may still need to copy if no tag is visible.
CONTAINERS_IMAGE_COPY_SECONDS = 90
CONTAINERS_IMAGE_SETTLEMENT_SECONDS = 180
CONTAINERS_ENABLE_RETRY_DELAYS = (15, 45)
CONTAINERS_ENABLE_MARGIN_SECONDS = 30
CONTAINERS_ENABLE_RESULT_SECONDS = (
    (1 + len(CONTAINERS_ENABLE_RETRY_DELAYS))
    * (CONTAINERS_IMAGE_COPY_SECONDS + CONTAINERS_IMAGE_SETTLEMENT_SECONDS)
    + sum(CONTAINERS_ENABLE_RETRY_DELAYS)
    + CONTAINERS_ENABLE_MARGIN_SECONDS
)
# Allow one 15-minute Disconnect attempt and time for it to be claimed.
DISCONNECT_RESULT_SECONDS = 16 * 60


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


LOCAL_HTTP = build_opener(ProxyHandler({}), NoRedirect)


def wait(label, condition, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = condition()
        if state:
            return state
        time.sleep(0.2)
    raise RuntimeError(f"{label} was not observable within {seconds}s")


def http(url, method, payload=None, token=None, *, content_type="application/json", timeout=12):
    if not ipaddress.ip_address(urlsplit(url).hostname).is_private:
        raise ValueError("The rehearsal client requires a private IP endpoint")
    return request_json(url, method, payload, token, content_type=content_type, timeout=timeout)


def request_json(url, method, payload=None, token=None, *, content_type="application/json", timeout=12):
    headers = {"Content-Type": content_type}
    if token:
        headers["Authorization"] = "Bearer " + token
    if payload is not None and not isinstance(payload, bytes):
        payload = json.dumps(payload).encode()
    try:
        with LOCAL_HTTP.open(Request(url, data=payload, method=method, headers=headers), timeout=timeout) as reply:
            return reply.status, None if reply.status == 204 else json.load(reply)
    except HTTPError as exc:
        return exc.code, None


class Rehearsal:
    activation_request = None
    enable_result_seconds = ENABLE_RESULT_SECONDS

    def __init__(self):
        self.cfg = json.loads(Path(".rehearsal/config.json").read_text())
        self.compose = ["docker", "compose", "-p", self.cfg["project"],
                        "-f", "rehearsal-compose.yml", "-f", "rehearsal-overlay.yml"]
        self.boundary = f"http://{self.cfg['address']}:{self.cfg['providerPort']}"
        self.account = "a" * 32
        self.login()

    def login(self):
        address = self.verify_isolation()
        self.api_container = subprocess.check_output(self.compose + ["ps", "-q", "api"], text=True, timeout=8).strip()
        self.origin = f"http://{address}:8000"
        self.provider("/local/ingress", "POST", {"host": address})
        credentials = dict(line.split("=", 1) for line in Path(".env").read_text().splitlines())
        code, data = http(self.origin + "/auth/login", "POST", urlencode({
            "username": credentials["BIFROST_DEFAULT_USER_EMAIL"],
            "password": credentials["BIFROST_DEFAULT_USER_PASSWORD"],
        }).encode(), content_type="application/x-www-form-urlencoded")
        assert code == 200
        self.admin = data["access_token"]
        self.workflows = {}
        self.holds = []
        self.runs = {}
        self.tenants = {}

    def verify_isolation(self, *, successor=None):
        provider = self.cfg.get("providerContainer")
        if provider:
            isolated, = json.loads(subprocess.check_output(["docker", "inspect", provider]))
            links = isolated["NetworkSettings"]["Networks"]
            if set(links) != {self.cfg["network"]} or links[self.cfg["network"]]["IPAddress"] != self.cfg["address"]:
                raise RuntimeError("The provider must use only the rehearsal bridge")
            routes = subprocess.check_output(["docker", "exec", provider, "cat", "/proc/net/route"], text=True)
            if any(row.split()[1] == "00000000" for row in routes.splitlines()[1:]):
                raise RuntimeError("The rehearsal provider has a default route")
        containers = {}
        api_address = None
        for service in ("api", "worker", "cloudflare-connector"):
            container = successor if service == "api" and successor else subprocess.check_output(
                self.compose + ["ps", "-q", service], text=True,
            ).strip()
            containers[service] = container
            inspection, = json.loads(subprocess.check_output(["docker", "inspect", container]))
            networks = inspection["NetworkSettings"]["Networks"]
            if self.cfg["network"] not in networks:
                raise RuntimeError(f"{service} is missing the rehearsal bridge")
            if service == "api":
                api_address = networks[self.cfg["network"]]["IPAddress"]
            for name in networks:
                network, = json.loads(subprocess.check_output(["docker", "network", "inspect", name]))
                if not network["Internal"] or network["EnableIPv6"]:
                    raise RuntimeError(f"{service} requires internal IPv4-only rehearsal networks")
                if (not provider and name == self.cfg["network"]
                        and network["IPAM"]["Config"][0]["Gateway"] != self.cfg["address"]):
                    raise RuntimeError("The provider address does not match the rehearsal bridge")
        for service in ("api", "worker"):
            check = subprocess.run(["docker", "exec", "--user", "1000:1000", containers[service], "python", "-c", """
import asyncio
import os
from pathlib import Path
import httpx
from src.services.worker_ingress_probes import _probe_address, _public_addresses
from src.services.worker_pool.cloudflare_workers_rest import API
address = os.environ["REHEARSAL_ADDRESS"]
if (API != f"http://{address}:{os.environ['REHEARSAL_PROVIDER_PORT']}/client/v4"
        or httpx.AsyncClient.send.__name__ != "send_local"
        or _public_addresses.__name__ != "local_addresses"
        or _probe_address.__name__ != "local_probe"):
    raise SystemExit("Local Cloudflare fixture hooks are missing")
if asyncio.run(_public_addresses("existing.example.test")) != (address,):
    raise SystemExit("Ingress probe is outside the local fixture")
if os.getenv("REHEARSAL_CONTAINERS") == "1":
    from src.services.worker_pool.cloudflare_activation import load_containers_release as activation_release
    from src.services.execution.cloudflare_eligibility import load_containers_release as approval_release
    from src.services.worker_pool.cloudflare_canary import asyncio as cloudflare_asyncio
    from src.services.worker_pool.cloudflare_containers_release import load_containers_release, run_oci_tool
    if (
        activation_release is not load_containers_release
        or approval_release is not load_containers_release
        or load_containers_release.__name__ != "local_release"
        or run_oci_tool.__name__ != "local_tool"
        or type(cloudflare_asyncio).__name__ != "LocalAsyncio"
    ):
        raise SystemExit("Local Containers fixture hooks are missing")
if any(row.split()[1] == "00000000" for row in Path("/proc/net/route").read_text().splitlines()[1:]):
    raise SystemExit("The rehearsal container has a default route")
"""], check=False, timeout=15)
            if check.returncode:
                raise RuntimeError(f"{service} failed local isolation verification. Connect or Upgrade is blocked.")
        evidence("isolation", {"internal_networks": True, "api_worker_hooks_verified": True,
                               "provider_isolated": bool(provider)})
        if not api_address:
            raise RuntimeError("The API has no rehearsal bridge address")
        return api_address

    def send(self, path, payload=None, _admin=None, method="POST", *, content_type=None, timeout=12):
        code, data = http(self.origin + path, method, payload, self.admin,
                          content_type=content_type or "application/json", timeout=timeout)
        if not 200 <= code < 300:
            raise RuntimeError(f"Bifrost {method} {path.split('?')[0]} returned HTTP {code}")
        return data

    def provider(self, path, method="GET", payload=None, *, status=200):
        code, data = http(self.boundary + path, method, payload, MANAGEMENT)
        assert code == status, f"Local provider {method} returned HTTP {code}, expected {status}"
        return data.get("result", data) if data else None

    def inventory(self, path):
        return self.provider(path)

    def sql(self, query):
        return json.loads(subprocess.check_output(self.compose + [
            "exec", "-T", "postgres", "psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-U", "bifrost", "-d", "bifrost",
        ], input=query, text=True, timeout=7))

    def install(self):
        return self.send("/api/capacity/cloudflare", method="GET")["installation"]

    def observe_status(self, checkpoint):
        status = self.send("/api/capacity/cloudflare", method="GET")
        workers = next((lane for lane in status["lanes"] if lane["lane"] == "workers"), None)
        workers_only = self.workers_only_status(status) if self.cfg.get("providerContainer") else {}
        evidence("capacity-status", {**workers_only,
            "checkpoint": checkpoint,
            "placement": sorted(item["code"] for item in status["placement_reasons"]),
            "workers": sorted(item["code"] for item in workers["reasons"]) if workers else [],
            "eligible_queued": workers["eligible_queued"] if workers else None,
            "running": workers["running"] if workers else None,
        })

    def workers_only_status(self, status):
        lanes = [lane["lane"] for lane in status["lanes"]]
        assert "containers" not in lanes
        installation = status["installation"]
        if installation is None:
            return {"lanes": lanes}
        fields = (
            "fleet_envelope_usd", "workers_fleet_envelope_usd", "containers_fleet_envelope_usd",
            "containers_remaining_instance_hours", "containers_remaining_qualification_canaries",
            "containers_instance_budget_resets_at",
        )
        values = {key: installation.get(key) for key in fields}
        assert Decimal(values["fleet_envelope_usd"]) == Decimal(values["workers_fleet_envelope_usd"]) > 0
        assert all(values[key] is None for key in fields[2:])
        return {"lanes": lanes, **values}

    def job(self, action, extra=None):
        install = self.install()
        result = self.send("/api/capacity/cloudflare/" + action, {
            "installation_id": install["installation_id"], "expected_revision": install["desired_revision"],
            **(extra or {}),
        })
        return str(UUID(result["job_id"]))

    def completed(self, job, outcome, *, window=90):
        started = time.monotonic()
        last_progress = None

        def terminal():
            nonlocal last_progress
            state = self.send("/api/platform-jobs/" + job, method="GET")
            if outcome == "disconnected":
                progress = {
                    "status": state["status"],
                    "attempt": state["attempt"],
                    "phase": (state.get("progress") or {}).get("phase"),
                    "code": (state.get("result") or {}).get("code") or (state.get("error") or {}).get("code"),
                }
                if progress != last_progress:
                    evidence("disconnect-job", {**progress, "elapsed_seconds": round(time.monotonic() - started, 3)})
                    last_progress = progress
            return state if state["status"] not in ACTIVE_JOB else None

        state = wait(outcome, terminal, window)
        result = state.get("result") or {}
        if state["status"] != "succeeded" or result.get("outcome") != outcome:
            code = result.get("code") or (state.get("error") or {}).get("code")
            print(json.dumps({"outcome": outcome, "status": state["status"], "code": code,
                              "failure": result.get("canary_failure"),
                              "details": result.get("details") if code == "CF_DEPLOYMENT_UNVERIFIED" else None}),
                  flush=True)
            raise RuntimeError(f"{outcome} job did not succeed")
        return result

    def review_activation(self, mode, *, containers=False):
        if self.origin != f"http://{self.verify_isolation()}:8000":
            raise RuntimeError("The rehearsal API changed addresses. Activation is blocked.")
        account = self.account
        base = "/api/capacity/cloudflare"
        status = self.send(base, method="GET")
        policy = status["suggested_policy"]
        assert Decimal(policy["monthly_limit_usd"]) == Decimal("75")
        workers = policy["workers"]
        assert (workers["launcher_cap"], workers["slots_per_launcher"], workers["installation_slots"],
                workers["max_lifetime_seconds"]) == (3, 4, 9, 3600)
        staged = self.send(base + "/credential", {
            "account_id": account, "api_token": self.cfg.get("managementToken", MANAGEMENT), "purpose": "connect",
        })
        body = {"account_id": account, "credential_ref": staged["credential_ref"], "policy": policy}
        if containers:
            body["containers"] = True
        if mode == "managed":
            body.update(reachability_mode="managed_tunnel", zone_id="b" * 32)
        else:
            body.update(reachability_mode="existing_https_endpoint",
                        worker_ingress_origin="https://" + self.cfg.get("hostname", "existing.example.test"))
        plan = self.send(base + "/preflight", body)
        assert plan["preflight_receipt"] and plan["account_id"] == account
        assert plan["preview"]["policy"]["workers"]["queue_prewarm_per_tenant"] == 0
        if containers:
            preview = plan["preview"]
            assert preview["containers_policy"]["enabled"]
            assert preview["containers_policy"]["monthly_instance_hours"] == 100
            assert Decimal(preview["containers_remaining_instance_hours"]) == 100
            assert 0 < Decimal(preview["containers_fleet_envelope_usd"]) < Decimal(preview["fleet_envelope_usd"]) <= 75
        assert status["installation"] is None and self.install() is None
        self.host = urlsplit(plan["worker_ingress_origin"]).hostname
        self.activation_request = {
            "credential_ref": staged["credential_ref"], "preflight_receipt": plan["preflight_receipt"],
            "consent": True, "consent_version": plan["consent_version"], "monthly_limit_usd": policy["monthly_limit_usd"],
        }
        evidence("policy", {"mode": mode, "lifetime_seconds": workers["max_lifetime_seconds"],
                            "fleet_envelope_usd": plan["preview"]["fleet_envelope_usd"]})

    def preflight(self):
        completed = subprocess.run([
            str(HERE.parent / "cloudflare-launcher" / "preflight-compose-worker.sh"),
            "https://" + self.host, *self.compose[2:],
        ], text=True, capture_output=True, timeout=35)
        result = json.loads(completed.stdout)
        evidence("preflight", {"checks": result["route_checks"], "connections": result["http_connections"],
                               "failure": result["failure"]})
        assert completed.returncode == 0 and result["reachable"] and result["preflight_execution_class"] == "remote_launcher"
        assert result["route_checks"] == 100 and result["http_connections"]

    def enable(self):
        if self.activation_request is not None:
            accepted = self.send("/api/capacity/cloudflare/activate", self.activation_request)
            job = str(UUID(accepted["job_id"]))
            self.activation_request = None
        else:
            job = self.job("enable")
        result = self.completed(job, "enabled", window=self.enable_result_seconds)
        assert result["execution_class"] == "remote_launcher" and result["http_connections"]
        installation = self.install()
        assert installation["admission_open"]
        self.verify_namespace_lag(job, installation["desired_revision"], "enable")
        evidence("enable", {"class": result["execution_class"], "connections": result["http_connections"]})

    def verify_namespace_lag(self, job, revision, stage):
        lag_ms = self.cfg.get("workerNamespaceLagMs", 0)
        if not lag_ms:
            return
        wire = [item for item in self.provider("/local/evidence") if item.get("revision") == revision]
        assert any(item["kind"] == "namespace-lag" for item in wire)
        settled, = [item for item in wire if item["kind"] == "namespace-lag-cleared"]
        assert settled["elapsedMs"] >= lag_ms
        assert sum(item["kind"] == "upload" and item["role"] == "capacity" for item in wire) == 1
        attempts = self.sql(f"SELECT attempt FROM platform_jobs WHERE id='{job}'")
        assert attempts == 2
        evidence(stage + "_namespace_lag", {"attempts": attempts, "capacity_uploads": 1,
                                           "fixture_lag_ms": lag_ms, "settled_read_ms": settled["elapsedMs"]})

    def register(self, name, source, org=None, memory=None, *, timeout=110):
        path = "workflows/" + name + ".py"
        self.send("/api/files/write", {"path": path, "location": "workspace", "content": source})
        workflow = self.send("/api/workflows/register", {"path": path, "function_name": name, "organization_id": org})
        assert workflow["organization_id"] == org
        self.send("/api/workflows/" + workflow["id"], {"timeout_seconds": timeout, "memory_request_mib": memory}, method="PATCH")
        return workflow["id"]

    def seed(self):
        provider, = [org for org in self.send("/api/organizations", method="GET") if org["is_provider"]]
        self.tenants = {"provider": provider["id"], "global": None}
        for group in ("third", "fourth"):
            self.tenants[group] = self.send("/api/organizations", {"name": "Rehearsal " + group})["id"]
        for group, org in self.tenants.items():
            name = "qualification_" + group
            source = WORKFLOW.format(name=name)
            wid = self.register(name, source, org, 110)
            approve_workflow(self.send, self.admin, wid, source.encode(), 12, 1000)
            self.workflows[group] = wid
        fixture = HERE / "fixture-hosted.py"
        original = fixture.read_bytes()
        try:
            fixture.write_text(WORKFLOW.format(name="phase5b_fixture"))
            ids, _, _ = seed_solution_workflows(self.admin, self.send, [("solution", self.tenants["provider"])],
                                                  hosted=True, timeout_seconds=110)
        finally:
            fixture.write_bytes(original)
        self.workflows["solution"] = ids["solution"]
        self.tenants["solution"] = self.tenants["provider"]
        evidence("approvals", {"accepted_loose": len(self.tenants) - 1, "accepted_solution": len(ids)})

    def seed_upgrade(self):
        provider, = [org for org in self.send("/api/organizations", method="GET") if org["is_provider"]]
        name = "qualification_upgrade"
        source = WORKFLOW.format(name=name)
        workflow = self.register(name, source, provider["id"], 110)
        approve_workflow(self.send, self.admin, workflow, source.encode(), 12, 1000)
        self.tenants["third"] = provider["id"]
        self.workflows["third"] = workflow

    def submit(self, case, group, *, hold=0):
        assert self.install()["admission_open"]
        response = self.send("/api/workflows/execute", {"workflow_id": self.workflows[group],
            "org_id": self.tenants[group] or "global", "input_data": {"case": case, "hold": hold}, "sync": False})
        self.runs[case] = str(UUID(response["execution_id"]))
        return self.runs[case]

    def primary_hold(self, *, worker_hostname=None):
        def primaries():
            return self.sql("""SELECT coalesce(json_agg(json_build_object(
                'current', hostname = '""" + (worker_hostname or "") + """',
                'budget', memory_budget_bytes/1048576, 'reserved', memory_reserved_bytes/1048576,
                'pressured', memory_pressured, 'lease_live', remote_owner_lease_expires_at > now()
                ) ORDER BY memory_budget_bytes DESC), '[]')
                FROM execution_workers WHERE form='static' AND state='ready' AND execution_lane='normal'
                  AND supported_handlers @> '["workflow.execute"]'::jsonb""")

        def idle_primaries():
            rows = primaries()
            if worker_hostname and (len(rows) != 1 or not rows[0]["current"]):
                return None
            return rows if rows and all(
                row["lease_live"] and row["reserved"] == 0 and not row["pressured"] for row in rows
            ) else None

        # Terminal job readback can precede the local runner releasing memory.
        rows = wait("primary workers are ready and idle", idle_primaries, 30)
        current = [row["budget"] for row in rows]
        assert current and min(current) > 160
        for index, size in enumerate(current):
            name = f"primary_hold_{index}"
            if name not in self.workflows:
                self.workflows[name] = self.register(name, f"""import asyncio
from bifrost import workflow
@workflow(name="{name}")
async def {name}():
    await asyncio.sleep(180)
""", memory=size - 24, timeout=200)
            else:
                # A replacement primary can advertise a different memory budget.
                self.send("/api/workflows/" + self.workflows[name], {"memory_request_mib": size - 24}, method="PATCH")
            location = "/api/executions/" + self.send("/api/workflows/execute", {
                "workflow_id": self.workflows[name], "org_id": "global", "sync": False,
            })["execution_id"]
            self.holds.append(location)
            try:
                wait("local hold running", lambda: self.send(location, method="GET")["status"] == "Running", 14)
            except RuntimeError:
                execution = str(UUID(location.rsplit("/", 1)[1]))
                pending = self.sql(f"""SELECT coalesce((SELECT json_build_object(
                    'requested_mib', memory_request_bytes/1048576, 'lane', execution_lane,
                    'assigned', assigned_worker_id IS NOT NULL
                    ) FROM work_dispatches WHERE subject_id='{execution}'), 'null')""")
                evidence("primary_hold_failure", {"requested_mib": size - 24, "pending_dispatch": pending,
                                                 "ready_primaries": primaries()})
                raise
        subprocess.run([sys.executable, str(HERE / "wait-primary-hold.py"), str(len(current)), "--", *self.compose[2:]],
                       check=True)

    def release_primary(self):
        for location in self.holds:
            status = self.send(location, method="GET")["status"]
            if status in {"Pending", "Queued", "Running"}:
                self.send(location + "/cancel")
            wait("local hold terminal", lambda: self.send(location, method="GET")["status"] in {"Cancelled", "Success"}, 15)
        self.holds.clear()

    def slot_rows(self, cases):
        alias = {self.runs[case]: case for case in cases}
        ids = ",".join(f"'{identity}'" for identity in alias)
        rows = self.sql(f"""SELECT coalesce(json_agg(json_build_object(
            'id',s.execution_id,'ordinal',s.ordinal,'tenant',s.tenant,'worker',s.worker_id,'fence',s.fence,
            'sent',s.sent,'released',s.released_at,'controller',w.launched_by,
            'revision',(SELECT r.deployment_revision FROM cloudflare_worker_reservations r WHERE r.worker_id=s.worker_id),
            'status',e.status,'queued',e.created_at,'completed',e.completed_at,
            'entry',(SELECT min(l.timestamp) FROM execution_logs l WHERE l.execution_id=e.id
                      AND l.message LIKE 'qualification entry %'),
            'claim',(SELECT c.id FROM execution_worker_claims c WHERE c.subject_id=e.id AND c.family='workflow_execution')
            )), '[]') FROM cloudflare_workflow_slots s
            JOIN execution_workers w ON w.id=s.worker_id JOIN executions e ON e.id=s.execution_id
            WHERE s.execution_id IN ({ids})""")
        return {alias[row["id"]]: row for row in rows}

    def sent_slots(self, cases):
        rows = self.slot_rows(cases)
        if len(rows) == len(cases) and all(row["sent"] and not row["released"] for row in rows.values()):
            return rows
        return None

    def held(self, cases):
        rows = self.sent_slots(cases)
        entered = set(self.provider("/local/entered"))
        if rows and all(row["id"] in entered for row in rows.values()):
            return rows
        return None

    def settle(self, case, status="Success", *, window=38):
        def terminal():
            row = self.slot_rows([case]).get(case)
            if row and row["released"] and row["status"] not in {"Pending", "Queued", "Running", "Cancelling"}:
                assert row["status"] == status, f"{case} ended {row['status']}, expected {status}"
                assert row["sent"] and (status == "Cancelled" or row["entry"])
                return row
        return wait(f"{case} terminal and released fence", terminal, window)

    def latency(self):
        receipts = {}
        for case in ("cold", "warm"):
            self.submit(case, "provider")
            if case == "cold":
                self.observe_status("cold-dispatch")
            slot = self.settle(case)
            wait(f"{case} terminal receipt ACK", lambda settled=slot: self.terminal_ack(settled), 10)
            receipts[case] = slot
        cold, warm = receipts["cold"], receipts["warm"]
        assert (cold["worker"], cold["ordinal"]) == (warm["worker"], warm["ordinal"])
        execution_id, worker_id = UUID(cold["id"]), UUID(cold["worker"])
        timing = self.sql(f"""
            SELECT launch_timing || cloudflare_timing
            FROM executions, cloudflare_worker_reservations
            WHERE executions.id='{execution_id}' AND worker_id='{worker_id}'
        """)
        stages = {
            "gate": "gate_eligible_at", "decision": "controller_decision_at",
            "launcher_start": "launcher_start_sent_at", "launcher_accepted": "launcher_start_accepted_at",
            "hello": "session_ready_at", "claim": "claim_at", "prepare": "prepare_sent_at",
            "engine_ready": "engine_ready_at", "dispatch": "dispatch_sent_at", "entry": "entry_at",
        }
        assert timing is not None and set(stages.values()) <= timing.keys(), timing
        elapsed = {stage: duration(timing["queued_at"], timing[field]) for stage, field in stages.items()}
        assert elapsed["gate"] == WORKERS_OVERFLOW_AGE_SECONDS * 1000
        assert elapsed["entry"] >= max(elapsed["dispatch"], elapsed["engine_ready"])
        evidence("latency", {"cold_queue_to_stage_ms": elapsed, **{label: {
            "queue_to_entry_ms": duration(row["queued"], row["entry"]),
            "queue_to_completion_ms": duration(row["queued"], row["completed"]),
            "ordinal": row["ordinal"],
            "terminal_ack": True,
        } for label, row in receipts.items()}})

    def terminal_ack(self, slot):
        wire = self.provider("/local/evidence")
        dispatch, = [entry for entry in wire if entry["kind"] == "dispatch" and entry["execution"] == slot["id"]]
        assert dispatch["worker"] == slot["worker"] and dispatch["route"].endswith(":" + slot["fence"])
        terminals = {entry["id"] for entry in wire if entry["kind"] == "result"
                     and entry["route"] == dispatch["route"] and entry["attempt"] == "completed"}
        return any(entry["kind"] == "ack" and entry["worker"] == slot["worker"]
                   and entry["id"] in terminals for entry in wire)

    def burst(self):
        cases = [f"{group}-{idx}" for group in ("provider", "global") for idx in range(1, 5)]
        cancelled = "global-4"
        for index, case in enumerate(cases):
            group = "solution" if case == "provider-4" else case.split("-")[0]
            self.submit(case, group, hold=-1 if case == cancelled else 70)
            if index == 0:
                self.observe_status("burst-first-dispatch")
        sent = wait("eight sent burst slots", lambda: self.sent_slots(cases), 28)
        sent_observed_at = time.monotonic()
        eight = wait("eight entered burst slots after dispatch", lambda: self.held(cases), 28)
        assert all(eight[case]["fence"] == sent[case]["fence"] for case in cases)
        dispatch_to_full_entry_ms = round((time.monotonic() - sent_observed_at) * 1000)
        for group in ("provider", "global"):
            assert {row["ordinal"] for name, row in eight.items() if name.startswith(group)} == {0, 1, 2, 3}
        self.submit("ninth", "third", hold=62)
        nine = cases + ["ninth"]
        cohort = wait("ninth reserved slot", lambda: self.held(nine), 22)
        assert all(cohort[case]["fence"] == eight[case]["fence"] for case in cases)
        target = self.submit("tenth", "fourth")
        controller, = {row["controller"] for row in cohort.values()}

        def marker():
            return json.loads(subprocess.check_output(self.compose + [
                "exec", "-T", "redis", "redis-cli", "--raw", "HGET", "bifrost:worker-pool:controllers", controller,
            ], text=True, timeout=5))["at"]

        baseline = marker()
        queued_at = self.sql(f"""SELECT to_json(extract(epoch FROM created_at)) FROM work_dispatches
            WHERE subject_id='{target}' AND family='workflow_execution'""")
        eligible_at = queued_at + WORKERS_OVERFLOW_AGE_SECONDS
        baseline = wait("first overflow-eligible controller pass",
                        lambda previous=baseline: stamp if (stamp := marker()) > previous and stamp >= eligible_at else None, 8)
        self.observe_status("tenth-dispatch-after-launch-gate")
        for _ in range(2):
            baseline = wait("next managed controller pass", lambda previous=baseline: stamp if (stamp := marker()) > previous else None, 8)
            held = self.held(nine)
            assert held and all(held[case]["fence"] == cohort[case]["fence"] for case in nine)
            assert self.sql(f"""SELECT to_json(coalesce((SELECT assigned_worker_id IS NULL FROM work_dispatches
                WHERE subject_id='{target}' AND family='workflow_execution'), false)
                AND NOT EXISTS(SELECT 1 FROM execution_worker_claims WHERE subject_id='{target}')
                AND NOT EXISTS(SELECT 1 FROM cloudflare_workflow_slots WHERE execution_id='{target}'))""")
        assert self.runs[cancelled] in self.provider("/local/held-reads")
        self.send("/api/executions/" + self.runs[cancelled] + "/cancel")
        released = self.settle(cancelled, "Cancelled")
        wire = self.provider("/local/evidence")
        victim, = [line for line in wire if line["kind"] == "dispatch" and line["execution"] == self.runs[cancelled]]
        assert any(line["kind"] == "result" and line["route"] == victim["route"] and line["attempt"] == "stopped"
                   for line in wire)
        survivors = [case for case in nine if case != cancelled]
        live = self.held(survivors)
        assert live and all(live[case]["fence"] == cohort[case]["fence"] for case in survivors)
        tenth = self.settle("tenth")
        assert duration(released["released"], tenth["entry"]) >= 0
        for case in survivors:
            self.settle(case)
        solution = self.send("/api/executions/" + self.runs["provider-4"], method="GET")
        assert solution["result"]["install_value"] == "solution"
        evidence("burst", {"provider": 4, "global": 4, "reserved": 1, "queued_observations": 2,
                           "cancel_unchanged_fences": len(survivors), "scoped_solution": True,
                           "last_dispatch_read_to_full_entry_ms": dispatch_to_full_entry_ms})

    def scripts(self):
        return {row["id"] for row in self.provider("/client/v4/accounts/" + self.account + "/workers/scripts")}

    def worker_receipts(self, *, installation_id=None):
        owner = f" AND installation_id='{UUID(installation_id)}'" if installation_id is not None else ""
        return self.sql(f"""SELECT coalesce(json_agg(json_build_object(
            'script', target_name, 'state', state, 'observed', observed_at,
            'revision', desired_revision, 'namespaces', namespace_ids)), '[]')
            FROM cloudflare_owned_resources WHERE kind='worker_script'{owner}""")

    def verify_receipts_absent(self, receipts):
        root = "/client/v4/accounts/" + self.account
        scripts = {row["script"] for row in receipts}
        namespaces = {value for row in receipts for value in row["namespaces"].values()}
        assert scripts.isdisjoint(self.scripts())
        inventory = self.inventory(root + "/workers/durable_objects/namespaces")
        assert namespaces.isdisjoint(row["id"] for row in inventory)
        assert scripts.isdisjoint(row.get("script") for row in inventory)
        for script in sorted(scripts):
            self.provider(root + "/workers/scripts/" + script + "/settings", status=404)
        for namespace in sorted(namespaces):
            self.provider(root + "/workers/durable_objects/namespaces/" + namespace + "/objects", status=404)
        return {"workers_404": len(scripts), "namespaces_404": len(namespaces)}

    def scale_in(self, *, active=False):
        scripts = self.scripts()
        if active:
            self.primary_hold()
            self.submit("drain", "third", hold=35)
            slot = wait("active drain workflow", lambda: self.held(["drain"]))["drain"]
            self.release_primary()
        job = self.job("scale-in")
        if active:
            wait("closed admission", lambda: not self.install()["admission_open"], 10)
            original = self.held(["drain"])
            assert original and original["drain"]["fence"] == slot["fence"]
        if active:
            self.settle("drain")
        self.completed(job, "scaled_in")
        assert self.scripts() == scripts and len(scripts) == 3
        assert self.sql("SELECT count(*) FROM cloudflare_usage_incarnations WHERE deleted_at IS NULL") == 0

    def disconnect(self, *, enqueue=None):
        receipts = self.worker_receipts()
        job = self.job("disconnect") if enqueue is None else enqueue()
        if "managementToken" not in self.cfg:
            self.observe_status("disconnect-requested")
        self.completed(job, "disconnected", window=DISCONNECT_RESULT_SECONDS)
        assert self.install() is None
        absent = self.verify_receipts_absent(receipts)
        expected = (0, 0, 0, 0)
        counts = self.sql("""SELECT json_build_array(
            (SELECT count(*) FROM cloudflare_usage_incarnations WHERE deleted_at IS NULL),
            (SELECT count(*) FROM cloudflare_workflow_slots WHERE released_at IS NULL),
            (SELECT count(*) FROM cloudflare_capacity_credentials WHERE ciphertext IS NOT NULL),
            (SELECT count(*) FROM cloudflare_owned_resources WHERE state<>'absent'))""")
        assert tuple(counts) == expected
        evidence("disconnect", {**absent, "live_ledger_rows": 0})

    def managed(self):
        self.review_activation("managed")
        self.enable()
        initial_shape = self.cfg["initialTunnelConfig"]
        assert {"kind": "initial-tunnel-config", "shape": initial_shape} in self.provider("/local/evidence")
        root = "/client/v4/accounts/" + "a"*32
        tunnel, = self.provider(root + "/cfd_tunnel")
        path = root + "/cfd_tunnel/" + tunnel["id"]
        config = self.provider(path + "/configurations")["config"]
        assert config["warp-routing"] == {"enabled": False}
        record, = self.provider("/client/v4/zones/" + "b"*32 + "/dns_records?name.exact=" + self.host)
        assert record["proxied"] and record["type"] == "CNAME"
        assert record["content"] == tunnel["id"] + ".cfargotunnel.com"
        assert self.provider(root + "/teamnet/routes?tunnel_id=" + tunnel["id"] + "&is_deleted=false") == []
        old = {session["id"] for session in self.provider(path + "/connections")}
        assert old
        token = self.provider(path + "/token")
        self.scale_in()
        self.completed(self.job("rotate", {"credential": "connector_token"}), "rotated")
        def evicted():
            new = {session["id"] for session in self.provider(path + "/connections")}
            return new if new and not (old & new) else None
        new = wait("connector eviction and replacement", evicted, 20)
        self.provider("/local/tunnel-connect", "POST", {"token": token}, status=403)
        self.preflight()
        self.enable()
        evidence("managed", {"initial_config": initial_shape, "warp_routing_enabled": False, "route_list_http": 200,
                             "previous_sessions_evicted": len(old), "replacement_sessions": len(new)})
        return tunnel["id"], record["id"]

    def upgrade(self, *, replay=True):
        self.primary_hold()
        execution = self.submit("upgrade", "third", hold=65)
        if "managementToken" not in self.cfg:
            self.observe_status("upgrade-original-dispatch")
        old = wait("upgrade live original claim", lambda: self.held(["upgrade"]))["upgrade"]
        assert old["claim"]
        self.release_primary()
        install_id = str(UUID(self.install()["installation_id"]))
        receipts = [row for row in self.worker_receipts(installation_id=install_id) if row["revision"] == old["revision"]]
        assert len(receipts) == 3 and sum(len(row["namespaces"]) for row in receipts) == 4
        original_api = subprocess.check_output(
            self.compose + ["ps", "-q", "api"], text=True, timeout=8,
        ).strip()
        assert re.fullmatch("[a-f0-9]{12,64}", original_api)
        # Keep the old API's ingress alive until the new package installs its fence.
        env = {**os.environ, "TRIAL_RELEASE": "2"}
        image = self.cfg["image"] + ":release-2"
        expected = subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}", image], text=True).strip()
        run_options = ["--use-aliases"] if not replay else []
        successor = subprocess.check_output(self.compose + ["run", "-d", "--no-deps", *run_options, "api"],
                                            text=True, env=env, timeout=30).strip()
        actual = subprocess.check_output(["docker", "inspect", "--format", "{{.Image}}", successor], text=True).strip()
        assert actual == expected
        inspected, = json.loads(subprocess.check_output(["docker", "inspect", successor]))
        address = inspected["NetworkSettings"]["Networks"][self.cfg["network"]]["IPAddress"]
        self.origin = f"http://{address}:8000"

        def http_ready():
            try:
                return http(self.origin + "/health/ready", "GET")[0] == 200
            except OSError:
                return False

        wait("replacement API ready", http_ready, 30)
        self.api_container = successor
        subprocess.run(self.compose + ["up", "-d", "--no-deps", "--pull", "never", "worker"],
                       check=True, env=env, timeout=40, stdout=subprocess.DEVNULL)
        assert self.verify_isolation(successor=successor) == address
        upgrade_events_start = len(self.provider("/local/evidence")) if "managementToken" not in self.cfg else None
        job = self.job("upgrade")
        pinned = Path(".release-2.checked").read_text().split()[1]

        def fenced():
            row = self.sql(f"""SELECT json_build_object('admitted', admitted_revision, 'upgrade', upgrade_release)
                FROM cloudflare_installations WHERE id='{install_id}'""")
            return row["admitted"] is None and row["upgrade"] == pinned

        wait("persisted replacement admission and send fence", fenced, 10)
        current = self.held(["upgrade"])["upgrade"]
        assert current["claim"] == old["claim"] and current["fence"] == old["fence"]
        self.provider("/local/ingress", "POST", {"host": address})
        if replay:
            self.provider("/local/interrupt", "POST", {"worker": old["worker"]})
        settled = self.settle("upgrade", window=70)
        wait("original route terminal ACK", lambda: self.terminal_ack(settled), 10)
        if not replay:
            def checkpoint():
                result = self.sql(f"""SELECT json_build_object(
                    'drained',upgrade_drained_at,'admitted',admitted_revision,'desired',desired_revision
                ) FROM cloudflare_installations WHERE id='{install_id}'""")
                return result if result["drained"] is not None else None

            checkpoint = wait("Upgrade verified retirement checkpoint", checkpoint, 35)
            assert checkpoint["admitted"] is None
            old_receipts = [row for row in self.worker_receipts(installation_id=install_id)
                            if row["revision"] == old["revision"]]
            assert len(old_receipts) == 3 and all(row["state"] == "absent" and row["observed"] for row in old_receipts)
            drained = datetime.fromisoformat(checkpoint["drained"])
            assert max(datetime.fromisoformat(row["observed"]) for row in old_receipts) <= drained
        result = self.completed(job, "enabled", window=ENABLE_RESULT_SECONDS)
        installation = self.install()
        assert result["http_connections"] and installation["admission_open"]
        self.verify_namespace_lag(job, installation["desired_revision"], "upgrade")
        absent = self.verify_receipts_absent(receipts)
        wire = self.provider("/local/evidence")
        dispatch, = [entry for entry in wire if entry["kind"] == "dispatch" and entry["execution"] == execution]
        replies = [entry for entry in wire if entry["kind"] == "result" and entry["route"] == dispatch["route"]]
        assert replies and any(entry["kind"] == "ack" and entry["id"] == replies[-1]["id"] for entry in wire)
        evidence_fields = {"original_claim": True, "dispatch_count": 1, "terminal_ack": True,
                           "old_workers_404": absent["workers_404"], "old_namespaces_404": absent["namespaces_404"]}
        if replay:
            cut = next(number for number, entry in enumerate(wire)
                       if entry["kind"] == "interrupt" and entry["worker"] == old["worker"])
            assert any(entry["kind"] == "hello" and entry["worker"] == old["worker"] for entry in wire[cut+1:])
            assert wire.index(replies[-1]) > cut
            latest_revision = max(item["revision"] for item in wire if item["kind"] == "upload")
            first_upload = next(number for number, entry in enumerate(wire)
                                if entry["kind"] == "upload" and entry["revision"] == latest_revision)
            assert sum(entry["kind"] == "delete" for entry in wire[cut+1:first_upload]) == 3
            evidence_fields["reconnect_replay"] = True
        else:
            assert replies[-1]["connection"] == dispatch["connection"]
            assert any(entry["kind"] == "ack" and entry["id"] == replies[-1]["id"]
                       and entry["connection"] == dispatch["connection"] for entry in wire)
            new_revision = checkpoint["desired"]
            replacement = [row for row in self.worker_receipts(installation_id=install_id)
                           if row["revision"] == new_revision]
            assert len(replacement) == 3 and all(row["state"] == "present" and row["observed"] for row in replacement)
            assert drained < min(datetime.fromisoformat(row["observed"]) for row in replacement)
            assert new_revision > old["revision"]
            if upgrade_events_start is not None:
                upgrade_events = wire[upgrade_events_start:]
                first_upload = next(number for number, entry in enumerate(upgrade_events)
                                    if entry["kind"] == "upload" and entry["revision"] == new_revision)
                prior_events = upgrade_events[:first_upload]
                deletes = [entry for entry in prior_events if entry["kind"] == "delete"]
                assert len(deletes) == 3 and all(entry["revision"] == old["revision"] for entry in deletes)
                old_namespace_ids = {value for row in receipts for value in row["namespaces"].values()}
                inventory_reads = [entry["ids"] for entry in prior_events if entry["kind"] == "namespace-inventory"]
                assert inventory_reads and old_namespace_ids.isdisjoint(inventory_reads[-1])
            # The original may still hold the singleton scheduler lease. Its
            # older release cannot admit the new digest. The ACK is already
            # witnessed, so retire the original API before submitting new work.
            subprocess.run(["docker", "stop", "--time", "30", original_api], check=True,
                           timeout=40, stdout=subprocess.DEVNULL)
            successor_hostname = inspected["Config"]["Hostname"]
            assert re.fullmatch("[a-f0-9]{12,64}", successor_hostname)
            wait("replacement scheduler lease", lambda: self.sql(f"""
                SELECT to_json(coalesce((SELECT split_part(owner_id, ':', 1) = '{successor_hostname}'
                  AND lease_expires_at > now() FROM scheduler_leases WHERE name='scheduler-triggers'), false))
            """), 40)
            subprocess.run(self.compose + ["up", "-d", "--no-deps", "--force-recreate", "--wait",
                                           "--wait-timeout", "35", "--pull", "never", "worker"],
                           check=True, env={**env, "TRIAL_CONTROL_API_URL": self.origin},
                           timeout=45, stdout=subprocess.DEVNULL)
            worker = subprocess.check_output(self.compose + ["ps", "-q", "worker"], text=True, timeout=8).strip()
            worker_info, = json.loads(subprocess.check_output(["docker", "inspect", worker], timeout=8))
            assert "BIFROST_API_URL=" + self.origin in worker_info["Config"]["Env"]
            assert self.verify_isolation(successor=successor) == address
            worker_hostname = worker_info["Config"]["Hostname"]
            assert re.fullmatch("[a-f0-9]{12,64}", worker_hostname)
            self.primary_hold(worker_hostname=worker_hostname)
            self.submit("replacement", "third")
            if self.cfg.get("providerContainer"):
                self.observe_status("replacement-submitted")
            replacement_started = time.monotonic()
            try:
                new = self.settle("replacement")
                assert new["revision"] == new_revision
                wait("replacement route terminal ACK", lambda: self.terminal_ack(new), 10)
            except (RuntimeError, AssertionError):
                try:
                    self.record_replacement_settlement(replacement_started)
                except (RuntimeError, OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                    evidence("replacement-settlement-readback-error", {"type": type(error).__name__})
                raise
            self.record_replacement_settlement(replacement_started)
            if self.cfg.get("providerContainer"):
                self.observe_status("replacement-readback")
            self.release_primary()
            evidence_fields.update(admission_fenced=True, same_control_connection=True, successor_scheduler_lease=True,
                                   worker_rebound_to_successor=True,
                                   old_absence_checkpoint_before_replacement=True,
                                   first_replacement_observation_after_checkpoint_ms=duration(
                                       checkpoint["drained"], min(row["observed"] for row in replacement),
                                   ), replacement_signed_canary=True, replacement_execution=True)
        evidence("upgrade", evidence_fields)

    def record_replacement_settlement(self, started):
        # Take the readback before releasing the primary or disconnecting.
        if "managementToken" in self.cfg:
            return
        execution = str(UUID(self.runs["replacement"]))
        wire = self.provider("/local/evidence")
        dispatches = [item for item in wire if item["kind"] == "dispatch" and item["execution"] == execution]
        timelines = []
        for dispatch in dispatches:
            terminals = [item for item in wire if item["kind"] == "result" and item["route"] == dispatch["route"]]
            terminal_ids = {item["id"] for item in terminals}
            acks = [item for item in wire if item["kind"] == "ack" and item["id"] in terminal_ids]
            timelines.append({
                "terminal_outcomes": [item["attempt"] for item in terminals],
                "dispatch_to_terminal_ms": [item["observedMs"] - dispatch["observedMs"] for item in terminals],
                "dispatch_to_ack_ms": [item["observedMs"] - dispatch["observedMs"] for item in acks],
            })
        slot = self.slot_rows(["replacement"]).get("replacement")
        state = self.send("/api/executions/" + execution, method="GET")
        pending = self.sql(f"""SELECT coalesce((SELECT json_build_object(
            'lane', execution_lane, 'assigned', assigned_worker_id IS NOT NULL,
            'workers_approved', cloudflare_revision IS NOT NULL) FROM work_dispatches
            WHERE subject_id='{execution}'), 'null')""")
        evidence("replacement-settlement", {
            "observed_seconds": round(time.monotonic() - started, 3),
            "execution_status": state["status"],
            "dispatch": pending,
            "slot": None if slot is None else {
                "revision": slot["revision"], "sent": slot["sent"], "released": bool(slot["released"]),
                "entry": bool(slot["entry"]), "claim_present": bool(slot["claim"]),
            },
            "routes": timelines,
        })

    def meter(self):
        result = self.install()["spend"]
        assert result["state"] == "ready" and result["estimated_usd"] is not None
        amount = Decimal(result["estimated_usd"])
        limit = Decimal("0.50")
        ledger = self.sql("""
            SELECT json_build_object(
                'retired', count(*) FILTER (WHERE deleted_at IS NOT NULL),
                'uncertain', count(*) FILTER (WHERE meter_uncertain),
                'uncertain_with_final_receipt', count(*) FILTER (WHERE meter_uncertain AND meter_final IS NOT NULL)
            )
            FROM cloudflare_usage_incarnations
        """)
        evidence("meter_guard", {"estimated_usd": str(amount), "limit_usd": str(limit),
                                 "within_limit": 0 <= amount < limit, "ledger": ledger})
        assert 0 <= amount < limit, f"Trial spend estimate {amount} is outside the {limit} guard"
        analytics = self.provider("/client/v4/graphql", "POST", {
            "query": "{ viewer { accounts { workersInvocationsAdaptive(limit: 1) { sum { requests cpuTimeUs } } } } }",
        })
        rows = analytics["data"]["viewer"]["accounts"][0]["workersInvocationsAdaptive"]
        requests = sum(row["sum"]["requests"] for row in rows)
        cpu_us = sum(row["sum"]["cpuTimeUs"] for row in rows)
        rates = json.loads(subprocess.check_output(["docker", "exec", self.api_container, "python", "-c", """
import json
from src.services.worker_pool.cloudflare_spend import _WORKER_CPU_MS_USD, _WORKER_REQUEST_USD
print(json.dumps([str(_WORKER_REQUEST_USD),str(_WORKER_CPU_MS_USD)]))
"""], text=True, timeout=10))
        provider = requests * Decimal(rates[0]) + cpu_us * Decimal(rates[1]) / 1000
        assert provider > 0
        containers = {}
        if self.cfg.get("providerContainer"):
            containers = self.sql("""
                SELECT json_build_object(
                    'enabled_installations', (SELECT count(*) FROM cloudflare_installations
                        WHERE containers_policy->>'enabled' = 'true'),
                    'reservations', count(*) FILTER (WHERE scope = 'containers'),
                    'canaries', count(*) FILTER (WHERE scope = 'containers' AND canary_job_id IS NOT NULL)
                ) FROM cloudflare_worker_reservations
            """)
            assert not any(containers.values())
            self.observe_status("synthetic-meter")
        evidence("meter", {"state": result["state"], "synthetic_provider_samples": len(rows),
                           "workers_only_test_ratio": str(amount / provider), **containers})


def duration(start, end):
    return round((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() * 1000)


def evidence(stage, values):
    print(json.dumps({"stage": stage, **values}), flush=True)


WORKFLOW = """import asyncio
import logging
from bifrost import tables, workflow

@workflow(name='{name}')
async def {name}(case: str, hold: int):
    rows = await tables.list()
    logging.getLogger("workflow").info("qualification entry %s", case)
    if hold < 0:
        await tables.list()
    else:
        await asyncio.sleep(hold)
    return {{"case": case, "table_count": len(rows)}}
"""


class ContainerRehearsal(Rehearsal):
    def prepare_local_guest(self, incarnation, *, preserve_failed_exit=False):
        image = self.cfg["containers"]["imageId"]
        identities = subprocess.check_output(["docker", "ps", "-q", "--filter", f"ancestor={image}"], text=True).split()
        guests = json.loads(subprocess.check_output(["docker", "inspect", *identities]))
        guest, = [item for item in guests if item["Image"] == image and item["State"]["Running"]
                  and f"BIFROST_WORKER_INCARNATION_ID={incarnation}" in item["Config"]["Env"]]
        assert guest["HostConfig"]["NetworkMode"] != "host"
        # The local Docker engine does not apply the provider's standard-1
        # limit. Give the guest the same cgroup boundary before relaying hello.
        # Otherwise launcher pressure is sampled from the whole test host.
        memory = str(4 * 1024**3)
        # Workerd's local Docker restart policy can relaunch the failed guest
        # and replace its exit receipt. Apply this only to the selected fault
        # fixture before forwarding its first signed hello.
        restart = ["--restart=no"] if preserve_failed_exit else []
        subprocess.run([
            "docker", "update", "--memory", memory, "--memory-swap", memory, *restart, guest["Id"],
        ], stdout=subprocess.DEVNULL, check=True)
        actual = subprocess.check_output([
            "docker", "inspect", "--format", "{{.HostConfig.Memory}}", guest["Id"],
        ], text=True).strip()
        assert actual == memory
        if preserve_failed_exit:
            restart_policy = subprocess.check_output([
                "docker", "inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", guest["Id"],
            ], text=True).strip()
            assert restart_policy == "no"
        with open(f"/proc/{guest['State']['Pid']}/ns/net", "rb") as namespace:
            fd = namespace.fileno()
            assert os.fstat(fd).st_ino != os.stat("/proc/self/ns/net").st_ino
            subprocess.run([
                "nsenter", f"--net=/proc/self/fd/{fd}", "--", "iptables", "-I", "OUTPUT",
                "-p", "tcp", "-m", "multiport", "--dports", "2053,8443", "-j", "REJECT", "--reject-with", "tcp-reset",
            ], check=True, pass_fds=(fd,))
        evidence("container-raw-tcp-boundary", {"worker": str(incarnation), "memory_bytes": int(actual)})

    @contextmanager
    def raw_tcp_boundary(self, *, preserve_failed_exit=False):
        assert self.cfg["project"].startswith("bifrost-test-containers-")
        path = Path(".rehearsal/raw-tcp.sock")
        assert not path.exists()

        class Handler(socketserver.StreamRequestHandler):
            def handle(handler):
                incarnation = UUID(handler.rfile.readline(38).decode("ascii").strip())
                self.prepare_local_guest(incarnation, preserve_failed_exit=preserve_failed_exit)
                handler.wfile.write(b"ready\n")

        try:
            with socketserver.UnixStreamServer(str(path), Handler) as server:
                thread = threading.Thread(target=server.serve_forever)
                thread.start()
                try:
                    yield
                finally:
                    server.shutdown()
                    thread.join()
        finally:
            path.unlink(missing_ok=True)

    def run(self):
        self.successor = None
        try:
            with self.raw_tcp_boundary():
                self._run()
        finally:
            if self.successor is not None:
                subprocess.run(["docker", "rm", "-f", self.successor], check=True, stdout=subprocess.DEVNULL)

    def _run(self):
        assert "containers" in self.cfg and "managementToken" not in self.cfg
        apps_path = "/client/v4/accounts/" + self.account + "/containers/applications"
        images_path = "/local/containers-registry"
        assert not self.scripts() and self.provider(apps_path) == [] and self.provider(images_path) == []
        self.review_activation("existing", containers=True)
        try:
            self.enable()
            self.preflight()
            self.sample_container_spend("enabled")
            installation = self.install()
            installation_id = str(UUID(installation["installation_id"]))
            revision = installation["desired_revision"]
            scripts = self.scripts()
            application, = self.provider(apps_path)
            image, = self.provider(images_path)
            assert len(scripts) == 4 and image["repository"] == application["name"]
            assert application["name"] + "-containers" in scripts
            namespace_path = "/client/v4/accounts/" + self.account + "/workers/durable_objects/namespaces"
            namespaces = {row["id"] for row in self.provider(namespace_path) if row.get("script") in scripts}
            assert len(namespaces) == 6
            app_path = apps_path + "/" + application["id"]
            self.container_workflow(installation_id)
            assert self.provider(app_path)["health"]["instances"] == {"active": 0, "starting": 0}
            self.sample_container_spend("idle-stop")

            self.completed(self.job("scale-in"), "scaled_in")
            assert not self.install()["admission_open"]
            assert self.install()["desired_revision"] == revision
            assert self.scripts() == scripts
            assert self.provider(images_path) == [image] and self.provider(apps_path) == [application]
            assert self.provider(app_path)["health"]["instances"] == {"active": 0, "starting": 0}
            assert namespaces == {row["id"] for row in self.provider(namespace_path) if row.get("script") in scripts}
            self.sample_container_spend("scale-in")
            evidence("container-scale-in", {"revision": revision, "retained_scripts": len(scripts)})

            self.enable()
            assert self.install()["desired_revision"] == revision
            assert self.scripts() == scripts and self.provider(images_path) == [image]
            assert self.provider(app_path)["id"] == application["id"]
            assert namespaces == {row["id"] for row in self.provider(namespace_path) if row.get("script") in scripts}
            canaries = self.sql(f"""SELECT count(*) FROM cloudflare_worker_reservations
                WHERE installation_id='{installation_id}' AND scope='containers'
                  AND canary_job_id IS NOT NULL AND container_stopped_at IS NOT NULL""")
            assert canaries == 2
            self.sample_container_spend("scale-out")
            self.meter()
            evidence("container-scale-out", {"revision": revision, "canaries_settled": canaries})
        except (AssertionError, RuntimeError, TimeoutError):
            evidence("container-provider-observations", {"observations": self.provider("/local/evidence")})
            raise
        finally:
            try:
                self.release_primary()
            finally:
                if self.install() is not None:
                    self.disconnect()
        self.provider(app_path, status=404)
        assert self.provider(apps_path) == [] and self.provider(images_path) == [] and not self.scripts()
        evidence("container-disconnect", {"applications": 0, "images": 0, "scripts": 0})

    def sample_container_spend(self, stage):
        installation = self.install()
        status = installation["spend"]
        assert status["state"] == "ready" and status["estimated_usd"] is not None
        amount = Decimal(status["estimated_usd"])
        remaining = Decimal(installation["containers_remaining_instance_hours"])
        assert 0 <= amount < Decimal("0.50") and 0 <= remaining < 100
        evidence("container-spend", {"checkpoint": stage, "estimated_usd": str(amount), "hours_remaining": str(remaining)})

    def approve_container_workflow(self, installation_id):
        provider, = [org for org in self.send("/api/organizations", method="GET") if org["is_provider"]]
        self.tenants["linux"] = provider["id"]
        source = """import _ssl
import subprocess
import sys
from bifrost import tables, workflow

@workflow(name="qualification_container")
async def qualification_container(case: str, hold: int):
    tables_visible = await tables.list()
    await tables.list()
    captured = subprocess.check_output([sys.executable, "-c", "print('linux-child')"], text=True).strip()
    return {"case": case, "child": captured, "native": _ssl.OPENSSL_VERSION, "table_count": len(tables_visible)}
"""
        workflow = self.register("qualification_container", source, provider["id"], 96, timeout=30)
        self.workflows["linux"] = workflow
        approval_path = f"/api/workflows/{UUID(workflow)}/cloudflare-approval"
        proposal = self.send(approval_path + "/propose", {"runtime": "containers"})["proposal"]
        assert proposal["container"]["installation_id"] == installation_id
        assert set(proposal["container"]["findings"]) == {"CF_NATIVE", "CF_PROCESS"}
        accepted = self.send(approval_path + "/accept", {
            "runtime": "containers", "review_sha256": proposal["review_sha256"],
            "subrequest_allowance": proposal["subrequest_allowance"], "cpu_budget_ms": proposal["cpu_budget_ms"],
        })
        assert accepted["status"] == "approved" and accepted["current_revision"] == proposal["review_sha256"]
        return approval_path

    def container_workflow(self, installation_id):
        approval_path = self.approve_container_workflow(installation_id)
        original_api = self.api_container
        # The static launcher reconnects to the Compose service name.
        self.successor = subprocess.check_output(
            self.compose + ["run", "-d", "--no-deps", "--use-aliases", "api"], text=True,
        ).strip()
        successor_address = self.verify_isolation(successor=self.successor)
        successor_origin = f"http://{successor_address}:8000"

        def successor_ready():
            try:
                return http(successor_origin + "/health/ready", "GET")[0] == 200
            except OSError:
                return False

        wait("successor API ready", successor_ready)
        self.primary_hold()
        execution = self.submit("container-overflow", "linux")

        def witness():
            row = self.sql(f"""SELECT coalesce((SELECT json_build_object(
                'status', e.status, 'result', e.result, 'worker', w.id,
                'stopped', r.container_stopped_at, 'deleted', w.capacity_resource_deleted_at,
                'provider_started', r.container_envelope->>'observed_started_at')
                FROM executions e JOIN cloudflare_worker_reservations r
                  ON r.container_envelope->>'execution_id'=e.id::text
                JOIN execution_workers w ON w.id=r.worker_id
                WHERE e.id='{execution}' AND r.scope='containers'), 'null')""")
            if row and row["status"] not in {"Pending", "Queued", "Running", "Cancelling"}:
                return row
            return None

        try:
            wait("Container SDK request in flight", lambda: execution in self.provider("/local/held-reads"))
            self.sample_container_spend("workflow-running")
            # The fixture primary dials the original Compose service name.
            # Release its hold before retiring that API. The Container keeps its claim.
            self.release_primary()
            dispatch, = [row for row in self.provider("/local/evidence")
                         if row["kind"] == "dispatch" and row["execution"] == execution]
            worker = str(UUID(dispatch["worker"]))
            generation = self.sql(f"SELECT remote_session_generation FROM execution_workers WHERE id='{worker}'")
            self.provider("/local/ingress", "POST", {"host": successor_address})
            self.provider("/local/interrupt", "POST", {"worker": worker})
            self.origin = successor_origin
            self.api_container = self.successor
            subprocess.run(["docker", "stop", original_api], check=True, stdout=subprocess.DEVNULL)
            wait("original Container adopts the successor API", lambda: self.sql(
                f"SELECT remote_session_generation FROM execution_workers WHERE id='{worker}'"
            ) > generation)
            self.provider("/local/release-sdk", "POST", {"execution": execution})
            terminal = wait("native Container workflow", witness)
            assert terminal["status"] == "Success"
            assert terminal["result"]["case"] == "container-overflow"
            assert terminal["result"]["child"] == "linux-child" and terminal["result"]["native"].startswith("OpenSSL")
            assert terminal["worker"] == worker
            assert sum(row["kind"] == "dispatch" and row["execution"] == execution
                       for row in self.provider("/local/evidence")) == 1
            evidence("container-api-handoff", {"worker": worker, "original_dispatches": 1})
            evidence("container-overflow", {"execution": execution, "worker": terminal["worker"], "child_exit": 0})
        finally:
            self.release_primary()

        def retired():
            row = witness()
            return row if row and row["stopped"] and row["deleted"] and row["provider_started"] else None

        settled = wait("Container idle stop and retirement", retired, 120 + 30)
        assert settled["worker"] == terminal["worker"]
        revoked = self.send(approval_path, method="DELETE")
        assert revoked["current_revision"] is None
        evidence("container-idle-stop", {"worker": settled["worker"], "placement_settled": True})


class HostedContainers(ContainerRehearsal):
    """Drive Containers through the public surfaces with a fenced account inventory."""

    enable_result_seconds = CONTAINERS_ENABLE_RESULT_SECONDS

    def __init__(self, config):
        if config.stat().st_mode & 0o077:
            raise ValueError("The hosted configuration must be private")
        self.cfg = json.loads(config.read_text())
        self.account = self.cfg["account"]
        assert re.fullmatch("[a-f0-9]{32}", self.account)
        assert re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)+", self.cfg["hostname"])
        project = self.cfg["project"]
        assert re.fullmatch("bifrost-test-[a-z0-9-]+", project)
        compose_files = self.cfg["compose_files"]
        assert 1 <= len(compose_files) <= 2 and all(re.fullmatch("[a-z0-9-]+\\.yml", name) for name in compose_files)
        self.compose = ["docker", "compose", "-p", project]
        for name in compose_files:
            self.compose.extend(("-f", name))
        self.provider_origin = self.cfg.get("provider_origin", "https://api.cloudflare.com")
        if self.provider_origin != "https://api.cloudflare.com":
            endpoint = urlsplit(self.provider_origin)
            assert endpoint.scheme == "http" and not any((endpoint.path, endpoint.query, endpoint.fragment, endpoint.username))
            assert ipaddress.ip_address(endpoint.hostname).is_private
        self.origin = f"http://{self.verify_isolation()}:8000"
        self.api_container = subprocess.check_output(self.compose + ["ps", "-q", "api"], text=True, timeout=8).strip()
        credentials = dict(line.split("=", 1) for line in Path(".env").read_text().splitlines())
        code, data = http(self.origin + "/auth/login", "POST", urlencode({
            "username": credentials["BIFROST_DEFAULT_USER_EMAIL"],
            "password": credentials["BIFROST_DEFAULT_USER_PASSWORD"],
        }).encode(), content_type="application/x-www-form-urlencoded")
        assert code == 200
        self.admin = data["access_token"]
        self.workflows, self.holds, self.runs, self.tenants = {}, [], {}, {}
        self.deadline = time.monotonic() + CONTAINERS_ACTIVE_SECONDS
        self.live_target = Decimal(self.cfg["live_target_usd"])
        self.live_cap = Decimal(self.cfg["live_abort_usd"])
        self.settled_cap = Decimal(self.cfg["settled_cap_usd"])
        self.high_water = Decimal(0)
        self.enable_attempts = []
        assert 0 < self.settled_cap <= self.live_target < self.live_cap

    def verify_isolation(self, *, successor=None):
        return HostedUpgrade.verify_isolation(self, successor=successor)

    def provider(self, path, method="GET", payload=None, *, status=200):
        assert path.startswith("/client/v4/accounts/" + self.account + "/") and method == "GET" and payload is None
        code, data = request_json(self.provider_origin + path, "GET", token=self.cfg["managementToken"])
        if code != status or (status == 200 and not data["success"]):
            raise RuntimeError(f"Expected provider read HTTP {status}, received {code}")
        return data["result"] if status == 200 else None

    def inventory_page(self, path, params):
        query = "?" + urlencode(params) if params else ""
        code, body = request_json(self.provider_origin + path + query, "GET",
                                  token=self.cfg["managementToken"])
        assert code == 200, f"Provider inventory returned HTTP {code}"
        if isinstance(body, list) and path.endswith("/containers/applications") and not params:
            return body, None
        assert isinstance(body, dict) and body.get("success") is True, "Malformed inventory envelope"
        assert "result_info" not in body or isinstance(body["result_info"], dict), "Malformed inventory metadata"
        return body["result"], body.get("result_info")

    def inventory(self, path, *, cursor=False, single_page=False):
        rows, tokens = {}, set()
        # Applications default to an unbounded list without per_page or
        # page_token. Only request a bounded page when following a cursor.
        params = {}
        if not single_page and not cursor:
            params = {"page": 1, "per_page": 100}
        total = pages = None
        page_size = None
        for page in range(1, 101):
            batch, info = self.inventory_page(path, params)
            if info is None:
                assert single_page or (cursor and page == 1), "Missing paginated inventory metadata"
                info = {}
            assert isinstance(batch, list) and isinstance(info, dict), "Malformed inventory envelope"
            count = info.get("count", len(batch))
            assert type(count) is int and count == len(batch), "Inventory page count mismatch"
            if "per_page" in info:
                size = info["per_page"]
                assert type(size) is int and size > 0 and (single_page or size <= (500 if cursor else 1000)), \
                    "Invalid inventory page size"
                assert page_size is None or page == 1 or size == page_size, "Inventory page size changed"
                page_size = size
            assert page_size is None or len(batch) <= page_size, "Oversized inventory page"
            assert all(isinstance(row, dict) and isinstance(row.get("id"), str) and row["id"]
                       for row in batch), "Inventory row has no ID"
            ids = {row["id"] for row in batch}
            assert len(ids) == len(batch) and not ids.intersection(rows), "Provider inventory repeated an ID"
            rows.update((row["id"], row) for row in batch)
            if "total_count" in info:
                reported = info["total_count"]
                assert type(reported) is int and reported >= len(rows) and total in (None, reported), \
                    "Inventory total_count mismatch"
                total = reported
            assert total is None or len(rows) <= total, "Inventory exceeds total_count"

            token = info.get("next_page_token")
            echoed = info.get("page_token")
            assert echoed is None or echoed == params.get("page_token", ""), "Inventory cursor echo mismatch"
            if not cursor and not single_page:
                assert token in (None, "") and echoed is None, "Mixed inventory pagination modes"
                reported_page = info.get("page", page)
                assert type(reported_page) is int and reported_page == page, "Inventory page number mismatch"
                if "total_pages" in info:
                    reported_pages = info["total_pages"]
                    assert type(reported_pages) is int and 0 <= reported_pages <= 100, "Invalid inventory total_pages"
                    assert pages in (None, reported_pages), "Inventory total_pages changed"
                    pages = reported_pages
                assert pages is None or page <= pages or (page == 1 and pages == 0 and not batch), \
                    "Inventory page exceeds total_pages"
                # Match the product namespace reader: a stable total_count
                # can drive continuation even when total_pages is absent.
                assert total is not None or pages is not None or page_size is not None, \
                    "Numbered inventory has no total or page size"
                more = total > len(rows) if total is not None else page_size == len(batch)
                if pages is not None:
                    assert total is None or (page < pages) == more, "Inventory numbered continuation is inconsistent"
                    more = page < pages
                    if more and total is None:
                        assert len(batch) == (page_size or params["per_page"]), "Short nonterminal numbered page"
                if more:
                    assert batch, "Inventory numbered continuation is inconsistent"
                    params["page"] = page + 1
                    continue
            else:
                assert "page" not in info and "total_pages" not in info, "Mixed inventory pagination modes"
            if token not in (None, ""):
                assert not single_page and isinstance(token, str) and token not in tokens and batch \
                    and total != len(rows), "Invalid inventory cursor continuation"
                tokens.add(token)
                page_size = page_size or 100
                params = {"page_token": token, "per_page": page_size}
                continue
            else:
                assert pages is not None or total is not None or page_size is None or len(batch) < page_size, \
                    "Full inventory page has no continuation or total"
            assert total is None or total == len(rows), "Inventory ended before total_count"
            return list(rows.values())
        raise RuntimeError("Provider inventory exceeded its pagination bound")

    @property
    def account_root(self):
        return f"/client/v4/accounts/{self.account}"

    def script_inventory(self):
        # The Workers scripts list is documented as a single-page endpoint.
        return self.inventory(self.account_root + "/workers/scripts", single_page=True)

    def scripts(self):
        return {row["id"] for row in self.script_inventory()}

    def provider_snapshot(self):
        # Worker Scripts is a single-page endpoint. DO namespaces use page
        # numbers and Containers applications use opaque page tokens.
        scripts = self.script_inventory()
        namespaces = self.inventory(self.account_root + "/workers/durable_objects/namespaces")
        applications = self.inventory(self.account_root + "/containers/applications", cursor=True)
        registry = self.image_probe({"account": self.account, "token": self.cfg["managementToken"]})
        assert isinstance(registry, list) and all(
                   isinstance(row, dict) and isinstance(row.get("name"), str) and row["name"]
                   and isinstance(row.get("tags"), list)
                   and all(isinstance(tag, str) and tag for tag in row["tags"]) for row in registry)
        snapshot = {
            "scripts": {row["id"]: row.get("etag") for row in scripts},
            "namespaces": {row["id"]: (row.get("script"), row.get("name"), row.get("class")) for row in namespaces},
            "applications": {row["id"]: row["name"] for row in applications},
            "registry": {row["name"].removeprefix(self.account + "/"): frozenset(row["tags"]) for row in registry},
        }
        assert tuple(map(len, snapshot.values())) == tuple(map(len, (scripts, namespaces, applications, registry)))
        return snapshot

    @staticmethod
    def inventory_evidence(snapshot):
        return {kind: {"count": len(rows), "ids_sha256": sha256(json.dumps(sorted(rows)).encode()).hexdigest()}
                for kind, rows in snapshot.items()}

    def image_probe(self, inputs):
        # Execute the probe inside the exact running API image. Secrets stay
        # on stdin, and the trial checkout contributes no product Python code.
        return json.loads(subprocess.check_output([
            "docker", "exec", "-i", self.api_container, "/usr/local/bin/python", "-c",
            (HERE / "container-trial-planner.py").read_text(),
        ], input=json.dumps(inputs), text=True, timeout=75))

    def plan_activation(self):
        assert self.canaries() == 0
        self.review_activation("existing", containers=True)
        plan = self.image_probe({"receipt": self.activation_request["preflight_receipt"]})
        assert plan["account"] == self.account
        return plan

    @staticmethod
    def plan_matches(snapshot, plan, *, namespace_ids=frozenset()):
        scripts = set(plan["scripts"])
        return {
            "scripts": scripts.intersection(snapshot["scripts"]),
            "namespaces": {identity for identity, (script, _, _) in snapshot["namespaces"].items()
                           if script in scripts or identity in namespace_ids},
            "applications": {identity for identity, name in snapshot["applications"].items()
                             if name == plan["application"]},
            "registry": {plan["registry_repository"]}.intersection(snapshot["registry"]),
        }

    def opening_inventory(self, plan):
        baseline = self.provider_snapshot()
        occupied = sorted(kind for kind, matches in self.plan_matches(baseline, plan).items() if matches)
        if occupied:
            raise RuntimeError("Trial ownership collision: " + ", ".join(occupied))
        evidence("foreign-inventory-baseline", self.inventory_evidence(baseline))
        return baseline

    def closing_inventory(self, baseline, plan, *, namespace_ids=frozenset()):
        after = self.provider_snapshot()
        before_matches = self.plan_matches(baseline, plan, namespace_ids=namespace_ids)
        leftovers = self.plan_matches(after, plan, namespace_ids=namespace_ids)
        drift = False
        changes = {}
        for kind, before in baseline.items():
            current = after[kind]
            planned = before_matches[kind] | leftovers[kind]
            differences = {
                "added": current.keys() - before.keys(),
                "removed": before.keys() - current.keys(),
                "changed": {identity for identity in before.keys() & current.keys()
                            if before[identity] != current[identity]},
            }
            changes[kind] = {}
            for change, identities in differences.items():
                drift |= bool(identities - planned)
                changes[kind][change] = [{"id": identity, "matches_plan": identity in planned}
                                         for identity in sorted(identities)]
        evidence("closing-provider-inventory", {
            "snapshot": self.inventory_evidence(after), "changes": changes, "foreign_drift": drift,
            "trial_leftovers": {kind: sorted(identities) for kind, identities in leftovers.items()},
        })
        assert not any(leftovers.values()), "Trial-owned resources are still present"

    def teardown(self, baseline, plan, app_path, original, *, enqueue_disconnect=None):
        first_error = None
        namespace_ids = set()

        def attempt(step, action):
            nonlocal first_error
            try:
                action()
            except BaseException as error:
                if first_error is None:
                    first_error = error
                evidence("trial-teardown-failed", {"step": step, "error_type": type(error).__name__})

        def disconnect_if_installed():
            if self.install() is not None:
                if enqueue_disconnect is None:
                    self.disconnect()
                else:
                    self.disconnect(enqueue=enqueue_disconnect)

        def record_namespaces():
            for receipt in self.worker_receipts(installation_id=plan["installation"]):
                namespace_ids.update(receipt["namespaces"].values())

        try:
            attempt("release-primary", self.release_primary)
            attempt("namespace-receipts", record_namespaces)
            attempt("disconnect", disconnect_if_installed)
        finally:
            attempt("closing-inventory", lambda: self.closing_inventory(baseline, plan, namespace_ids=namespace_ids))
            if app_path is not None:
                attempt("application-absence", lambda: self.provider(app_path, status=404))
        # An in-flight journey exception resumes automatically after finally.
        if original is None and first_error is not None:
            raise first_error

    def canaries(self, *, job=None, settled=False):
        filters = (" AND container_stopped_at IS NOT NULL" if settled else "")
        if job is not None:
            filters += f" AND canary_job_id='{UUID(job)}'"
        return self.sql(f"""SELECT count(*) FROM cloudflare_worker_reservations
            WHERE scope='containers' AND canary_job_id IS NOT NULL{filters}""")

    def record_enable_failure(self, state):
        result = state.get("result") or {}
        qualification = result.get("container_qualification")
        if qualification is None:
            try:
                status = self.send("/api/capacity/cloudflare", method="GET")
                qualification = (status.get("deployment") or {}).get("container_qualification")
            except (AssertionError, OSError, RuntimeError, ValueError):
                # Evidence collection must not delay teardown beyond one request
                # or hide the failure that stopped qualification.
                pass
        fields = (
            "generation", "stage", "fatal_generation", "fatal_stage", "exception_class", "reason",
            "close_generation", "close_code", "close_category", "provider_terminal_state", "provider_exit_code",
        )
        evidence("hosted-shaped-enable-failed", {
            "code": result.get("code") or (state.get("error") or {}).get("code"),
            "canary_failure": result.get("canary_failure"),
            "container_qualification": {key: qualification[key] for key in fields if key in qualification}
            if isinstance(qualification, dict) else None,
        })

    def completed(self, job, outcome, *, window=90):
        if outcome != "enabled":
            if outcome != "disconnected":
                window = self.active_window(window)
            return super().completed(job, outcome, window=window)

        canary_deadline = None
        state = {}

        def terminal():
            nonlocal canary_deadline, state
            state = self.send(f"/api/platform-jobs/{job}", method="GET")
            result = state.get("result") or {}
            code = result.get("code") or (state.get("error") or {}).get("code")
            if code in {"CF_CONTAINERS_CLEANUP_PENDING", "CF_CONTAINERS_QUALIFICATION_BACKOFF"}:
                raise RuntimeError(f"Qualification stopped ({code})")
            if state["status"] not in ACTIVE_JOB and (
                state["status"] != "succeeded" or result.get("outcome") != outcome
            ):
                raise RuntimeError(f"Enable did not succeed ({code})")
            count = self.canaries()
            job_canaries = self.canaries(job=job)
            if count > 2 or job_canaries > 1:
                raise RuntimeError(f"Qualification stopped ({code or 'canary fence'})")
            self.observe_scheduler("qualification")
            if state["status"] == "succeeded":
                assert job_canaries == self.canaries(job=job, settled=True) == 1
                evidence("hosted-shaped-enable", {"attempt": state["attempt"], "settled_canaries": job_canaries})
                self.enable_attempts.append(state["attempt"])
                return result
            # Keep the wait after the first paid reservation below the five-minute
            # qualification retry window, even when image preparation took longer.
            if job_canaries and canary_deadline is None:
                canary_deadline = time.monotonic() + ENABLE_RESULT_SECONDS
            if canary_deadline is not None and time.monotonic() >= canary_deadline:
                raise RuntimeError("Container canary did not settle within the Enable wait")
            return None

        try:
            return wait(outcome, terminal, self.active_window(min(window, self.enable_result_seconds)))
        except (AssertionError, OSError, RuntimeError, ValueError):
            self.record_enable_failure(state)
            self.send(f"/api/platform-jobs/{job}/cancel")
            raise

    def active_window(self, seconds):
        return max(0, min(seconds, self.deadline - time.monotonic()))

    def observe_scheduler(self, stage, *, settled=False, starting=False):
        status = self.send("/api/capacity/cloudflare", method="GET")
        deployment = status["installation"]
        spend = deployment["spend"]
        assert spend["state"] == "ready" and spend["settled_usd"] is not None and spend["reserved_usd"] is not None
        paid, reserved = Decimal(spend["settled_usd"]), Decimal(spend["reserved_usd"])
        estimate = paid + reserved
        self.high_water = max(self.high_water, estimate)
        if (
            estimate >= self.live_cap or (settled and paid >= self.settled_cap)
            or (starting and estimate >= self.live_target) or time.monotonic() >= self.deadline
        ):
            raise RuntimeError("Trial spend or time stop")
        lane = next((entry for entry in status["lanes"] if entry["lane"] == "containers"), None)
        evidence("hosted-shaped-scheduler", {
            "checkpoint": stage, "lane": lane, "estimated_usd": str(estimate), "canaries": self.canaries(),
            "settled_usd": str(paid), "reserved_usd": str(reserved), "high_water_usd": str(self.high_water),
        })

    def enable(self):
        assert self.canaries() < 2
        before = self.canaries()
        if before:
            self.observe_scheduler("before-enable", starting=True)
        super().enable()
        assert self.canaries() == before + 1
        assert self.canaries(settled=True) == before + 1

    @staticmethod
    def verify_stopped_placement(stopped, placement, application_id):
        assert stopped["id"] == placement and stopped["application_id"] == application_id
        assert stopped["status"]["state"] in ("stopped", "inactive")

    @staticmethod
    def verify_empty_application(application):
        counts = application["health"]["instances"]
        assert all(type(counts.get(key)) is int and counts[key] == 0 for key in ("active", "starting"))

    def run(self, plan=None):
        if plan is None:
            plan = self.plan_activation()
        baseline = self.opening_inventory(plan)
        root = self.account_root + "/containers/applications"
        namespace_path = self.account_root + "/workers/durable_objects/namespaces"
        scripts = set(plan["scripts"])

        def owned_namespaces():
            return {row["id"] for row in self.inventory(namespace_path) if row.get("script") in scripts}

        app_path = None
        try:
            self.enable()
            self.preflight()
            self.observe_scheduler("enabled", settled=True)
            installation = self.install()
            assert str(UUID(installation["installation_id"])) == plan["installation"]
            revision = installation["desired_revision"]
            app, = [row for row in self.inventory(root, cursor=True) if row["name"] == plan["application"]]
            app_path = f"{root}/{app['id']}"
            assert len(scripts) == 4 and scripts <= self.scripts()
            namespaces = owned_namespaces()
            assert len(namespaces) == 6
            approval = self.approve_container_workflow(str(UUID(installation["installation_id"])))
            self.primary_hold()
            self.observe_scheduler("before-workflow", starting=True)
            execution = self.submit("container-overflow", "linux")

            def completed_work():
                self.observe_scheduler("workflow")
                current = self.send(f"/api/executions/{execution}", method="GET")
                return current if current["status"] not in {"Pending", "Queued", "Running", "Cancelling"} else None

            terminal = wait("Container workflow", completed_work, self.active_window(90))
            assert terminal["status"] == "Success" and terminal["result"]["child"] == "linux-child"
            assert terminal["result"]["native"].startswith("OpenSSL")
            self.release_primary()
            placement = wait("Container placement retirement", lambda: self.sql(f"""SELECT COALESCE((
                SELECT container_envelope->'provider_instance_id' FROM cloudflare_worker_reservations
                WHERE container_envelope->>'execution_id'='{execution}' AND container_stopped_at IS NOT NULL
                ), 'null')"""), self.active_window(150))
            assert self.sql(f"SELECT count(*) FROM execution_worker_claims WHERE subject_id='{execution}'") == 1
            stopped = self.provider(f"{app_path}/instances/{placement}")
            self.verify_stopped_placement(stopped, placement, app["id"])
            revoked = self.send(approval, method="DELETE")
            assert revoked["current_revision"] is None
            self.observe_scheduler("idle-stop", settled=True)
            self.completed(self.job("scale-in"), "scaled_in")
            assert not self.install()["admission_open"] and scripts <= self.scripts()
            assert owned_namespaces() == namespaces
            self.verify_empty_application(self.provider(app_path))
            self.observe_scheduler("scale-in", settled=True)
            self.enable()
            assert self.install()["desired_revision"] == revision and scripts <= self.scripts()
            assert owned_namespaces() == namespaces
            assert self.provider(app_path)["id"] == app["id"] and self.canaries() == 2
            self.observe_scheduler("scale-out", settled=True)
        finally:
            self.teardown(baseline, plan, app_path, sys.exception())


class HostedUpgrade(Rehearsal):
    """Control a dedicated, already provisioned Existing HTTPS trial."""

    def __init__(self, config):
        if config.stat().st_mode & 0o077:
            raise ValueError("The hosted configuration must be private")
        self.cfg = json.loads(config.read_text())
        self.account = self.cfg["account"]
        assert re.fullmatch("[a-f0-9]{32}", self.account)
        assert re.fullmatch("[a-z0-9-]+(?:\\.[a-z0-9-]+)+", self.cfg["hostname"])
        project = self.cfg["project"]
        assert re.fullmatch("bifrost-test-[a-z0-9-]+", project)
        self.compose = ["docker", "compose", "-p", project, "-f", "trial-compose.yml"]
        self.boundary = f"http://127.0.0.1:{int(self.cfg['providerPort'])}"
        self.phase = "connect_enable"
        self.spending = {}
        self.last_spend_sample = 0
        self.cleanup = False
        self.login()

    def verify_isolation(self, *, successor=None):
        for service in ("api", "worker"):
            container = successor if service == "api" and successor else subprocess.check_output(
                self.compose + ["ps", "-q", service], text=True, timeout=8,
            ).strip()
            info, = json.loads(subprocess.check_output(["docker", "inspect", container], timeout=8))
            assert info["Config"]["Labels"]["com.docker.compose.project"] == self.cfg["project"]
            assert not info["HostConfig"]["PortBindings"]
            address = info["NetworkSettings"]["Networks"][self.cfg["network"]]["IPAddress"]
            assert ipaddress.ip_address(address).is_private
            if service == "api":
                api_address = address
        return api_address

    def provider(self, path, method="GET", payload=None, *, status=200):
        if path.startswith("/local/"):
            code, data = http(self.boundary + path, method, payload, self.cfg["relayToken"])
            assert code == status
            return data["result"]
        assert path.startswith("/client/v4/accounts/" + self.account + "/") and method == "GET"
        token = self.cfg["managementToken"]
        if path.endswith("/workers/durable_objects/namespaces"):
            inventory = []
            for page in range(1, 101):
                code, data = request_json("https://api.cloudflare.com" + path + f"?page={page}&per_page=100", "GET", token=token)
                assert code == 200 and data["success"]
                rows = data["result"]
                inventory.extend(rows)
                if len(rows) < 100:
                    return inventory
            raise RuntimeError("Namespace inventory did not settle")
        code, data = request_json("https://api.cloudflare.com" + path, "GET", token=token)
        if code != status:
            raise RuntimeError(f"Expected provider read HTTP {status}, received {code}")
        return data["result"] if status == 200 and data["success"] else None

    def scripts(self):
        known = {row["script"] for row in self.worker_receipts()}
        return super().scripts() & known

    def sample_spend(self, *, settled=False):
        self.last_spend_sample = time.monotonic()
        current = super().send("/api/capacity/cloudflare", method="GET")["installation"]
        if not current:
            return
        value = current["spend"]
        assert value and value["state"] == "ready" and value["estimated_usd"] is not None
        cost = Decimal(value["estimated_usd"])
        phase = self.spending.setdefault(self.phase, {"peak_live_usd": Decimal(0)})
        phase["peak_live_usd"] = max(phase["peak_live_usd"], cost)
        if settled:
            phase["settled_api_usd"] = cost
        if not self.cleanup and cost >= Decimal("4.50"):
            raise RuntimeError("Live spend abort")

    def send(self, path, payload=None, _admin=None, method="POST", *, content_type=None, timeout=12):
        if not self.cleanup:
            if time.time() - self.cfg["started"] >= 1500:
                raise RuntimeError("Teardown reserve started")
            if time.monotonic() - self.last_spend_sample >= 1:
                self.sample_spend()
        return super().send(path, payload, _admin, method, content_type=content_type, timeout=timeout)

    def run(self):
        try:
            self.seed_upgrade()
            self.review_activation("existing")
            self.enable()
            self.preflight()
            self.sample_spend(settled=True)
            if time.time() - self.cfg["started"] < 1200 and max(self.spending["connect_enable"].values()) < Decimal("3.60"):
                self.phase = "upgrade"
                self.upgrade(replay=False)
                self.scale_in()
                self.sample_spend(settled=True)
            else:
                evidence("upgrade", {"status": "dropped_for_headroom"})
        finally:
            self.cleanup = True
            try:
                self.release_primary()
            finally:
                if self.install() is not None:
                    self.phase = "disconnect"
                    try:
                        self.sample_spend()
                    finally:
                        self.disconnect()
                evidence("spend", {phase: {key: str(value) for key, value in readings.items()}
                                   for phase, readings in self.spending.items()})


def queue_disconnect_after_worker_loss(trial, recovery):
    """Pause the sole physical worker and let the real liveness sweep fence it."""
    worker, = subprocess.check_output(trial.compose + ["ps", "-q", "worker"], text=True, timeout=8).split()

    def physical_identity():
        return subprocess.check_output([
            "docker", "inspect", "--format", "{{.Config.Hostname}} {{.State.StartedAt}}", worker,
        ], text=True, timeout=8).split()

    physical = physical_identity()

    def ready_workers():
        return trial.sql("""SELECT coalesce(json_agg(json_build_object(
            'id', id, 'launcher', remote_launcher_id, 'hostname', hostname
            )), '[]') FROM execution_workers WHERE form='static' AND state='ready'
            AND execution_lane='normal' AND memory_budget_bytes IS NOT NULL
            AND remote_owner_lease_expires_at > clock_timestamp()""")

    original, = ready_workers()
    assert original["hostname"] == physical[0]
    incarnation = str(UUID(original["id"]))
    started = time.monotonic()
    try:
        subprocess.run(["docker", "pause", worker], check=True, capture_output=True, timeout=12)
        # Allow the 20s owner lease, 30s heartbeat age and 15s dispatch sweep.
        wait("static worker lost after heartbeat stopped", lambda: trial.sql(f"""SELECT to_json(EXISTS(
            SELECT 1 FROM execution_workers WHERE id='{incarnation}' AND state='lost' AND end_reason='heartbeat_lost'
            ))"""), 90)
        assert ready_workers() == []
        job = trial.job("disconnect")
        queued = trial.send(f"/api/platform-jobs/{job}", method="GET")
        assert queued["status"] == "queued" and queued["attempt"] == 0
        evidence("disconnect-queued-without-worker", {
            "status": queued["status"], "attempt": queued["attempt"],
            "loss_seconds": round(time.monotonic() - started, 3),
        })
    finally:
        paused = subprocess.check_output([
            "docker", "inspect", "--format", "{{.State.Paused}}", worker,
        ], text=True, timeout=8).strip()
        if paused == "true":
            subprocess.run(["docker", "unpause", worker], check=True, capture_output=True, timeout=12)

    successor, = wait("replacement static incarnation ready", ready_workers, 45)
    assert successor["id"] != original["id"] and successor["launcher"] != original["launcher"]
    assert successor["hostname"] == physical[0] and physical_identity() == physical
    assert subprocess.check_output(trial.compose + ["ps", "-q", "worker"], text=True, timeout=8).split() == [worker]
    recovery.update(job=job, new_incarnation=True, same_container=True)
    return job


def exercise_image_preparation_failure(fixture, trial):
    """Exercise the local registry effect that precedes a terminal preparation error."""
    fixture.provider("/local/image-preparation-mode", "POST", {"mode": "error"})
    plan = trial.plan_activation()
    baseline = trial.opening_inventory(plan)
    recovery = {}
    try:
        accepted = trial.send("/api/capacity/cloudflare/activate", trial.activation_request)
        job = str(UUID(accepted["job_id"]))

        def completed():
            state = trial.send(f"/api/platform-jobs/{job}", method="GET")
            return state if state["status"] not in ACTIVE_JOB else None

        state = wait("Injected image preparation failure", completed, trial.active_window(trial.enable_result_seconds))
        assert state["status"] == "requires_action" and state["result"]["code"] == "CF_DEPLOYMENT_UNVERIFIED"
        details = state["result"]["details"]
        assert details["preparation_status"] == "error" and details["preparation_http_status"] == "200"
        assert "fixture-provider-value" not in json.dumps(details)
        installation = str(UUID(plan["installation"]))
        image = trial.sql(f"""SELECT json_build_object(
            'state', state, 'sent', provider_details->'creation_sent', 'provider_id', provider_id
            ) FROM cloudflare_owned_resources WHERE installation_id='{installation}' AND kind='container_image'""")
        assert image == {"state": "creating", "sent": True, "provider_id": None} and trial.canaries() == 0
        assert plan["registry_repository"] in trial.provider_snapshot()["registry"]
    finally:
        original = sys.exception()
        trial.teardown(baseline, plan, None, original, enqueue_disconnect=(
            (lambda: queue_disconnect_after_worker_loss(trial, recovery)) if original is None else None
        ))
    assert trial.install() is None and trial.canaries() == 0
    job = recovery.pop("job", None)
    assert job is not None, "The recovered Disconnect was not recorded"
    state = trial.send(f"/api/platform-jobs/{job}", method="GET")
    assert state["status"] == "succeeded" and state["attempt"] == 1
    evidence("static-worker-recovered-disconnect", {**recovery, "attempt": state["attempt"]})
    evidence("failed-image-preparation-disconnected", {"preparation": details, "canaries": 0})
    fixture.provider("/local/image-preparation-mode", "POST", {"mode": "slow"})


GUEST_INIT_FAULTS = (
    "identity", "prctl", "capabilities", "ca-unreadable", "scratch-unwritable", "shm-unavailable",
    "runtime-import", "settings", "hook", "ready-exit", "ready-signal",
)


def exercise_guest_fatal(fixture, trial, *, reconnect=False, init_fault=None):
    """Fail the real Docker guest after bootstrap and inspect the product evidence."""
    plan = trial.plan_activation()
    baseline = trial.opening_inventory(plan)
    fixture.provider("/local/guest-fatal-mode", "POST", {"mode": init_fault or ("reconnect" if reconnect else "fatal")})
    try:
        accepted = trial.send("/api/capacity/cloudflare/activate", trial.activation_request)
        job = str(UUID(accepted["job_id"]))

        def completed():
            state = trial.send(f"/api/platform-jobs/{job}", method="GET")
            return state if state["status"] not in ACTIVE_JOB else None

        state = wait("Injected post-hello guest failure", completed, trial.active_window(trial.enable_result_seconds))
        assert state["status"] == "requires_action" and state["result"]["code"] == "CF_CONTAINERS_UNVERIFIED"
        result = state["result"]["container_qualification"]
        assert result["generation"] == result["fatal_generation"] == result["close_generation"] == (2 if reconnect else 1)
        assert result["stage"] == result["fatal_stage"] == ("guest-init" if init_fault else "requirements")
        if init_fault:
            assert result["exception_class"] == "GuestInitError" and result["reason"] == init_fault
            assert result["child_returncode"] == {"ready-exit": 7, "ready-signal": -9}.get(init_fault, 1)
            facts = result["startup_facts"]
            assert facts["supervisor"]["uid"] == 1000 and facts["supervisor"]["pid_namespace"]
            if init_fault not in ("ready-exit", "ready-signal"):
                child = facts["child"]
                assert child["uid"] == 1001 and child["pid_namespace"]
                if init_fault == "capabilities":
                    assert int(child["CapInh"], 16) != 0
                elif init_fault == "ca-unreadable":
                    assert child["ca_exists"] and not child["ca_readable"]
                elif init_fault == "scratch-unwritable":
                    assert child["scratch_exists"] and not child["scratch_writable"]
                elif init_fault == "shm-unavailable":
                    assert not child["shm_exists"]
        else:
            assert result["exception_class"] == "RuntimeError" and result["reason"] == "runtime"
        assert (result["close_code"], result["close_category"]) == (1011, "internal")
        assert (result["provider_terminal_state"], result["provider_exit_code"]) == ("failed", 1)
        status = trial.send("/api/capacity/cloudflare", method="GET")
        assert status["deployment"]["container_qualification"] == result
        assert not status["installation"]["admission_open"]
        assert trial.canaries(job=job) == trial.canaries(job=job, settled=True) == 1
        assert "rehearsal-secret-in-guest-exception" not in json.dumps([state, status])
        image, = [row for row in fixture.provider("/local/evidence") if row["kind"] == "guest-fault-image"]
        assert image["hook_loaded_from_read_only_mount"] is True
        _, _, digest = fixture.cfg["containers"]["image"].partition("@")
        assert image["image_digest"] == (digest or None)
        evidence("guest-fault-image", {"image_digest": image["image_digest"], "hook_loaded_from_read_only_mount": True})
        trial.record_enable_failure(state)
        evidence("guest-fatal-after-hello", result)
    finally:
        original = sys.exception()
        try:
            trial.teardown(baseline, plan, None, original)
        finally:
            fixture.provider("/local/guest-fatal-mode", "POST", {"mode": "off"})
    assert trial.install() is None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--managed-only", action="store_true", help="Rehearse items 6–9 on a fresh stack")
    modes.add_argument("--upgrade-only", action="store_true", help="Rehearse uninterrupted Upgrade with Existing HTTPS")
    modes.add_argument("--containers-only", action="store_true", help="Rehearse the opted-in local Container journey")
    modes.add_argument("--hosted-upgrade-config", type=Path, help="Run the rehearsed Upgrade against a dedicated hosted ingress")
    modes.add_argument("--containers-hosted-shape", action="store_true", help="Run the Containers trial sequence")
    parser.add_argument("--hosted-containers-config", type=Path, help="Use this hosted config with --containers-hosted-shape")
    exercises = parser.add_mutually_exclusive_group()
    exercises.add_argument("--exercise-image-preparation-failure", action="store_true",
                        help="Locally rehearse a failed preparation and Disconnect before the slow happy path")
    exercises.add_argument("--exercise-guest-fatal", action="store_true",
                        help="Locally rehearse one post-hello Docker guest failure and Disconnect")
    exercises.add_argument("--exercise-guest-fatal-after-reconnect", action="store_true",
                        help="Locally drop bootstrap once, then rehearse a fatal Docker guest exit and Disconnect")
    exercises.add_argument("--exercise-guest-init-fault", choices=GUEST_INIT_FAULTS,
                        help="Locally exercise a classified child startup failure and Disconnect")
    args = parser.parse_args()
    if args.hosted_containers_config and not args.containers_hosted_shape:
        parser.error("--hosted-containers-config requires --containers-hosted-shape")
    if args.exercise_image_preparation_failure and (not args.containers_hosted_shape or args.hosted_containers_config):
        parser.error("--exercise-image-preparation-failure requires the local hosted-shaped fixture")
    guest_fatal = args.exercise_guest_fatal or args.exercise_guest_fatal_after_reconnect or bool(args.exercise_guest_init_fault)
    if guest_fatal and (not args.containers_hosted_shape or args.hosted_containers_config):
        parser.error("Guest failure exercises require the local hosted-shaped fixture")
    if args.hosted_upgrade_config:
        HostedUpgrade(args.hosted_upgrade_config).run()
        return
    if args.hosted_containers_config:
        HostedContainers(args.hosted_containers_config).run()
        return
    if args.containers_hosted_shape:
        fixture = ContainerRehearsal()
        assert "containers" in fixture.cfg and "managementToken" not in fixture.cfg
        assert fixture.cfg["holdSecondSdkRead"] is False
        with (
            fixture.raw_tcp_boundary(preserve_failed_exit=guest_fatal),
            TemporaryDirectory(prefix="containers-hosted-shape-") as directory,
        ):
            config = Path(directory) / "trial.json"
            config.touch(mode=0o600)
            config.write_text(json.dumps({
                "account": fixture.account, "project": fixture.cfg["project"], "network": fixture.cfg["network"],
                "hostname": "existing.example.test", "managementToken": MANAGEMENT,
                "provider_origin": fixture.boundary, "compose_files": ["rehearsal-compose.yml", "rehearsal-overlay.yml"],
                "live_target_usd": "0.03", "live_abort_usd": "0.05", "settled_cap_usd": "0.02",
            }))
            trial = HostedContainers(config)
            if guest_fatal:
                exercise_guest_fatal(fixture, trial, reconnect=args.exercise_guest_fatal_after_reconnect,
                                     init_fault=args.exercise_guest_init_fault)
                return
            if args.exercise_image_preparation_failure:
                exercise_image_preparation_failure(fixture, trial)
            if fixture.cfg.get("foreignInventory"):
                plan = trial.plan_activation()
                fixture.provider("/local/foreign-inventory", "POST", {
                    **plan, "mixed": fixture.cfg["foreignInventory"] == "mixed",
                })
                collision = {"script": plan["scripts"][0]}
                fixture.provider("/local/trial-script-collision", "POST", collision)
                try:
                    trial.opening_inventory(plan)
                except RuntimeError as exc:
                    assert str(exc) == "Trial ownership collision: scripts"
                    assert trial.install() is None and trial.canaries() == 0
                    evidence("exact-collision-refused-before-activation", {"kind": "scripts"})
                else:
                    raise AssertionError("The trial accepted an occupied script name")
                finally:
                    fixture.provider("/local/trial-script-collision", "DELETE", collision)
                trial.run(plan)
            else:
                trial.run()
            if fixture.cfg["workerNamespaceLagMs"]:
                assert trial.enable_attempts[0] > 1
            if args.exercise_image_preparation_failure:
                states = [item["status"] for item in fixture.provider("/local/evidence")
                          if item["kind"] == "container-image-preparation"]
                assert states[:4] == ["error", "pending", "pending", "ready"]
                evidence("slow-image-preparation", {"states": states})
        return
    if args.containers_only:
        ContainerRehearsal().run()
        return
    trial = Rehearsal()
    if args.upgrade_only:
        trial.seed_upgrade()
        trial.review_activation("existing")
        trial.enable()
        try:
            trial.preflight()
            trial.upgrade(replay=False)
            trial.scale_in()
            trial.meter()
        finally:
            try:
                trial.release_primary()
            finally:
                trial.disconnect()
        return
    trial.seed()
    if not args.managed_only:
        trial.review_activation("existing")
        trial.enable()
        trial.preflight()
        trial.primary_hold()
        trial.latency()
        trial.burst()
        trial.release_primary()
        scripts = trial.scripts()
        trial.completed(trial.job("reconcile"), "verified")
        assert trial.scripts() == scripts
        trial.scale_in(active=True)
        evidence("reconcile-and-scale-in", {"active_drain": True, "scripts_retained": 3})
        trial.disconnect()
    tunnel, dns = trial.managed()
    try:
        trial.upgrade(replay=args.managed_only)
        trial.scale_in()
        trial.meter()
    finally:
        try:
            trial.release_primary()
        finally:
            trial.disconnect()
        root = "/client/v4/accounts/" + "a"*32 + "/cfd_tunnel"
        assert trial.provider(root) == []
        assert trial.provider(root + "/" + tunnel + "/connections") == []
        assert trial.provider(root + "/" + tunnel)["deleted_at"]
        trial.provider("/client/v4/zones/" + "b"*32 + "/dns_records/" + dns, status=404)
        evidence("managed-teardown", {"tunnel_deleted": True, "sessions": 0})


if __name__ == "__main__":
    main()
