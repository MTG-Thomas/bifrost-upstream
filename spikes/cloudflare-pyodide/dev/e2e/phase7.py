#!/usr/bin/env python3
"""Hosted checks against a private, disposable CT128 debug API."""

import argparse
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

from phase5a import (
    API_ENV, HERE, ROOT, SUMMARY_ENDPOINT, enroll_manual, local_api, prepare,
    seed_agents, seed_summary, seed_workflows, sha256,
)
from phase6 import seed_linux_workflows, seed_solution_workflows


def prepare_hosted(hostname, workers_only=False):
    if not re.fullmatch(r"bifrost-hq-[a-f0-9]{9}\.midtowntg\.com", hostname):
        raise SystemExit("Use the dedicated trial hostname in midtowntg.com")
    prepare(hosted=True, phase6=True, summary=not workers_only, agent=not workers_only, qualification=True)
    endpoint = f"https://{hostname}/__fixture/v1"
    ca = ROOT / ".phase7-ca.pem"
    ca.write_bytes((ROOT / ".phase6-ca.pem").read_bytes() +
                   Path("/etc/ssl/certs/ca-certificates.crt").read_bytes())
    ca.chmod(0o644)  # The non-root API reads this public trust bundle.
    compose = ROOT / "docker-compose.debug.yml"
    compose.write_bytes(compose.read_bytes().replace(
        b"./.phase6-ca.pem:/app/.phase6-ca.pem", b"./.phase7-ca.pem:/app/.phase6-ca.pem",
    ))
    manual_token = "" if workers_only else f"BIFROST_WORKER_CAPACITY_TOKEN={secrets.token_hex(32)}\n"
    API_ENV.write_text(API_ENV.read_text().replace(SUMMARY_ENDPOINT, endpoint) +
                       manual_token + "BIFROST_POOL_MAX_LIFETIME_SECONDS=600\n")


def manual(project, admin, send, prior_enrollment):
    sys.path.insert(0, str(ROOT / "api"))
    from shared.worker_control_auth import external_worker_control_key

    env = dict(line.split("=", 1) for line in API_ENV.read_text().splitlines() if line)
    endpoint = env["BIFROST_CLOUDFLARE_AGENT_ENDPOINT"]
    build = "phase7-hosted"
    if prior_enrollment:
        if prior_enrollment.stat().st_mode & 0o077:
            raise SystemExit("The prior enrollment must be private")
        previous = json.loads(prior_enrollment.read_text())
        incarnation, launcher = UUID(previous["incarnation"]), UUID(previous["launcher"])
    else:
        incarnation, launcher = uuid4(), uuid4()
    resource = f"phase7:do:{incarnation}"
    key = external_worker_control_key(env["BIFROST_WORKER_CAPACITY_TOKEN"], incarnation, resource)
    if prior_enrollment and (
        previous["resource"] != resource or previous["build"] != build
        or not hmac.compare_digest(previous["controlKey"], key)
    ):
        raise SystemExit("The prior enrollment does not match the prepared trial key and identity")
    workflows, approval = seed_workflows(project, admin, send, True)
    enroll_manual(project, incarnation, launcher, resource, build)
    return {
        "workflows": workflows, "incarnation": str(incarnation), "launcher": str(launcher),
        "resource": resource, "build": build,
        "controlKey": key,
        "summary": seed_summary(project, endpoint), "agent": seed_agents(project, admin, send, endpoint),
    }, approval


def placement(admin, send, project, provision, *, workers_only=False):
    if provision.stat().st_mode & 0o077:
        raise SystemExit("The capacity handoff must be private")
    hosted = json.loads(provision.read_text())
    url_keys = ("workersUrl",) if workers_only else ("workersUrl", "containersUrl")
    for key in url_keys:
        if not re.fullmatch(r"https://bifrost-hq-[a-f0-9]{9}-[a-z]+\.[a-z0-9-]+\.workers\.dev", hosted[key]):
            raise SystemExit("Use the dedicated hosted capacity providers")
    token_keys = ("workersToken",) if workers_only else ("workersToken", "containersToken")
    for key in token_keys:
        if not re.fullmatch(r"[a-f0-9]{64}", hosted[key]):
            raise SystemExit("Expected dedicated random capacity tokens")
    tenants = [send("/api/organizations", {"name": f"Phase 7 Tenant {label}"}, admin)["id"] for label in ("A", "B")]
    workflows, loose = seed_workflows(project, admin, send, False,
                                      tenant_fixtures=(("phase6-a", tenants[0]),), hosted_tenants=True)
    installed, solution, results = seed_solution_workflows(project, admin, send, (("phase6-b", tenants[1]),), hosted=True)
    workflows.update(installed)
    if workers_only:
        send("/api/admin/kubernetes/policy", {
            "warm_workers": 1, "max_workers": 1, "idle_seconds": 120,
        }, admin, method="PUT")
        runtime_workflows, runtime_approvals = seed_workflows(project, admin, send, True)
        workflows.update(runtime_workflows)
        loose += runtime_approvals
    else:
        seed_linux_workflows(admin, send, workflows, True)
    return {
        "workflows": workflows,
        "phase6": {
            "token": hosted.pop("workersToken"), "providerHost": (ROOT / ".phase6-host").read_text(),
            "tenants": tenants, "results": results,
        },
        "qualification": {"workersOnly": hosted["workersUrl"]} if workers_only else {"placement": {
            **hosted, "policy": {
                "warm_workers": 1, "max_workers": 3, "worker_memory_mib": 1024,
                "largest_worker_mib": 1024, "idle_seconds": 10,
            },
        }},
    }, loose + solution


def run(runtime, provision, prior_enrollment, workers_only=None):
    if not ROOT.name.startswith("hosted-") or ROOT.stat().st_mode & 0o077:
        raise SystemExit("Use a private hosted archive")
    origin, project, admin, send = local_api(True)
    config, approval = (placement(admin, send, project, workers_only or provision, workers_only=bool(workers_only))
                        if workers_only or provision
                        else manual(project, admin, send, prior_enrollment))
    print(json.dumps({"project": project, "workflows": config["workflows"], "runtime_sha256": sha256(runtime.read_bytes()),
                      "approved_entries": [item["entry_sha256"] for item in approval]}), flush=True)
    subprocess.run(["node", str(HERE / "run.mjs")], check=True, cwd=ROOT, text=True, input=json.dumps({
        "origin": origin, "project": project, "admin": admin, "runtime": str(runtime),
        "hosted": True, "qualification": {}, "port": 31847, **config,
    }))


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare")
    preparation.add_argument("hostname")
    preparation.add_argument("--workers-only", action="store_true")
    runner = commands.add_parser("run")
    runner.add_argument("runtime", type=Path)
    mode = runner.add_mutually_exclusive_group()
    mode.add_argument("--placement", type=Path, help="Private hosted provider handoff JSON")
    mode.add_argument("--workers-only", type=Path, help="Private Workers provider handoff JSON")
    mode.add_argument("--reuse-enrollment", type=Path, help="Private, previously deployed identity for a fresh dev database")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare_hosted(args.hostname, args.workers_only)
    else:
        run(args.runtime.resolve(strict=True), args.placement, args.reuse_enrollment, args.workers_only)
