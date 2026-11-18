#!/usr/bin/env python3
"""Prepare and enroll a disposable phase 5 debug stack."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
API_ENV = ROOT / ".phase5-api.env"
ENTRY = "workflows/phase5a_fixture.py"
FUNCTION = "phase5a_fixture"


def command(*args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, text=True, **kwargs).strip()


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def prepare(hosted=False):
    prefix = "phase5b-" if hosted else "phase5-"
    if not ROOT.name.startswith(prefix) or command("git", "remote") or command(
        "git", "status", "--porcelain"
    ):
        raise SystemExit(f"Use a clean, freshly initialized {prefix}* archive without Git remotes")
    if ROOT.stat().st_mode & 0o077:
        raise SystemExit("Create the disposable archive with mode 0700")
    # Uvicorn drops privileges inside the container. Only this private archive
    # and the API container's read-only bind mount can reach the dotenv.
    fd = os.open(API_ENV, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "w") as env:
        os.fchmod(env.fileno(), 0o644)
        env.write("BIFROST_CLOUDFLARE_WORKER_HELLO_ENABLED=true\n")
        env.write(f"BIFROST_WORKER_CAPACITY_TOKEN={secrets.token_hex(32)}\n")
    for directory in ("node_modules", "sdk_src"):
        (ROOT / "api/src/services/sdk_package" / directory).mkdir(parents=True, exist_ok=True)
    compose = ROOT / "docker-compose.debug.yml"
    original = compose.read_bytes()
    before, api = original.split(b"\n  api:\n", 1)
    api = api.replace(
        b"    volumes:\n",
        b'    ports:\n      - "127.0.0.1::8000"\n'
        b"    volumes:\n      - ./.phase5-api.env:/app/.env:ro\n",
        1,
    )
    original = before + b"\n  api:\n" + api
    if not Path("/dev/net/tun").exists():
        # The client runs in port mode without its optional tunnel device.
        before, client = original.split(b"\n  client:\n", 1)
        client, after = client.split(b"\n  netbird:\n", 1)
        client = client.replace(b"    devices:\n      - /dev/net/tun\n", b"", 1)
        original = before + b"\n  client:\n" + client + b"\n  netbird:\n" + after
    compose.write_bytes(original)
    overlay = ROOT / "docker-compose.debug.port.yml"
    overlay.write_bytes(overlay.read_bytes().replace(b'"${DEBUG_CLIENT_PORT}', b'"127.0.0.1:${DEBUG_CLIENT_PORT}'))
    if hosted:
        defaults = ROOT / ".env.debug"
        defaults.chmod(0o600)
        defaults.write_bytes(defaults.read_bytes().replace(
            b"BIFROST_DEFAULT_USER_PASSWORD=password",
            f"BIFROST_DEFAULT_USER_PASSWORD={secrets.token_hex(32)}".encode(),
        ) + f"\nBIFROST_SECRET_KEY={secrets.token_hex(32)}\n".encode())


def local_api(hosted):
    status = command("env", "BIFROST_FORCE_PORT=1", "./debug.sh", "status")
    project = re.search(r"^Project:\s+(bifrost-debug-[0-9a-f]+)$", status, re.MULTILINE)[1]
    address = command("docker", "port", f"{project}-api-1", "8000/tcp")
    if not re.fullmatch(r"127\.0\.0\.1:\d+", address):
        raise SystemExit("Expected the task-owned API to publish a loopback port")
    login = re.search(r"^Login:\s+(\S+) / (.+)$", status, re.MULTILINE)
    if login is None:
        raise SystemExit("The debug API did not report its seeded login")
    if hosted and not re.fullmatch(r"[0-9a-f]{64}", login[2]):
        raise SystemExit("The hosted run requires the random seeded admin password")
    origin = f"http://{address}"

    def send(path, body, token=None, method="POST"):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if token:
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            body = json.dumps(body).encode()
        try:
            with urlopen(Request(origin + path, data=body, headers=headers, method=method), timeout=15) as response:
                return None if response.status == 204 else json.load(response)
        except HTTPError as error:
            raise SystemExit(f"{method} {path} returned HTTP {error.code}") from None

    admin = send("/auth/login", urlencode({"username": login[1], "password": login[2]}).encode())["access_token"]
    return origin, project, admin, send


def seed_workflows(project, admin, send, hosted):
    fixtures = (
        ("hosted", "phase5b_fixture", "fixture-hosted.py", 10, 1000),
        ("allowance", "phase5b_allowance", "fixture-hosted.py", 4, 1000),
    ) if hosted else (("local", FUNCTION, "fixture.py", 10, 1000),)
    approval = []
    workflows = {}
    for kind, function, filename, allowance, cpu in fixtures:
        entry = f"workflows/{function}.py" if hosted else ENTRY
        content = (HERE / filename).read_bytes()
        if kind == "allowance":
            content = content.replace(b"phase5b_fixture", b"phase5b_allowance")
        send("/api/files/write", {"path": entry, "content": content.decode(), "location": "workspace"}, admin)
        workflow = send("/api/workflows/register", {"path": entry, "function_name": function}, admin)
        workflow_id = workflows[kind] = workflow["id"]
        send(f"/api/workflows/{workflow_id}", {"timeout_seconds": 60}, admin, method="PATCH")
        entry_hash = sha256(content)
        approval.append({
            "workflow_id": workflow_id, "entry_path": entry, "function_name": function,
            "entry_sha256": entry_hash, "module_sha256": {},
            "manifest_sha256": sha256(json.dumps({entry: entry_hash}, sort_keys=True, separators=(",", ":")).encode()),
            "packages": [], "https_hosts": [] if hosted else ["phase5-fixture.invalid"], "required_https_hosts": [],
            "estimated_subrequests": min(6, allowance), "subrequest_allowance": allowance,
            "estimated_cpu_ms": 100, "cpu_budget_ms": cpu,
        })
    with API_ENV.open("a") as env:
        env.write(f"BIFROST_CLOUDFLARE_WORKFLOW_APPROVALS='{json.dumps(approval, separators=(',', ':'))}'\n")

    # Re-exec the API application under its existing Uvicorn reload supervisor.
    # This reloads the cached settings while leaving its container and data up.
    since = str(int(time.time()))
    (ROOT / "api/src/config.py").touch()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        # Read the configured approval in the running application indirectly
        # via startup log lines emitted by the new application process.
        logs = command("docker", "logs", "--since", since, f"{project}-api-1", stderr=subprocess.STDOUT)
        reload = logs.rsplit("src/config.py", 1)
        if len(reload) == 2 and "Application startup complete" in reload[1]:
            break
        time.sleep(0.25)
    else:
        raise SystemExit("The debug API did not finish reloading the workflow approval")
    return workflows, approval


def postgres(project, sql):
    return command("docker", "exec", "-i", f"{project}-postgres-1", "psql", "-XqAt",
                   "-v", "ON_ERROR_STOP=1", "-U", "bifrost", "-d", "bifrost", input=sql)


def resume_enrollment(project):
    enrollment = json.loads((ROOT / ".phase5b-deploy.json").read_text())
    incarnation, launcher = UUID(enrollment["incarnation"]), UUID(enrollment["launcher"])
    idle = postgres(project, f"""
        SELECT count(*) FROM execution_workers
        WHERE id='{incarnation}' AND remote_launcher_id='{launcher}' AND state IN ('starting', 'ready')
          AND runtime_profile='cloudflare-python-v1' AND active_runs=0
          AND NOT EXISTS (
              SELECT FROM execution_worker_claims c JOIN executions e ON e.id=c.subject_id
              WHERE c.worker_id='{incarnation}' AND e.completed_at IS NULL
          );
    """)
    if idle != "1":
        raise SystemExit("Only an idle hosted enrollment can resume")
    if not 1024 <= enrollment["port"] <= 65535:
        raise SystemExit("Invalid hosted relay port")
    value = next(line.partition("=")[2] for line in API_ENV.read_text().splitlines()
                 if line.startswith("BIFROST_CLOUDFLARE_WORKFLOW_APPROVALS="))
    approval = json.loads(value[1:-1])
    workflows = {kind: str(UUID(item["workflow_id"])) for kind, item in zip(
        ("hosted", "allowance"), approval, strict=True)}
    return workflows, approval, incarnation, launcher, enrollment["port"]


def run(runtime, hosted, resume):
    sys.path.insert(0, str(ROOT / "api"))
    from shared.worker_control_auth import external_worker_control_key

    prefix = "phase5b-" if hosted else "phase5-"
    if not API_ENV.is_file() or not ROOT.name.startswith(prefix):
        raise SystemExit("Run prepare and start this disposable archive's debug stack first")
    origin, project, admin, send = local_api(hosted)
    port = 0
    if resume:
        workflows, approval, incarnation, launcher, port = resume_enrollment(project)
    else:
        workflows, approval = seed_workflows(project, admin, send, hosted)
        incarnation, launcher = uuid4(), uuid4()
    resource = f"{'phase5b' if hosted else 'phase5a'}:do:{incarnation}"
    build = "phase5b-hosted" if hosted else "phase5a-local"
    sql = f"""
        INSERT INTO execution_workers (
            id, form, state, resource_class, execution_lane, runtime_profile,
            capacity_resource_uid, remote_launcher_id, version, supported_handlers,
            remote_hello_sequence, remote_session_generation, active_runs, runs_handled,
            memory_reserved_bytes, remote_owner_id, remote_owner_lease_expires_at,
            capacity_resource_deleted_at, memory_budget_bytes, capacity_scope
        ) VALUES (
            '{incarnation}', 'external', 'starting', 'standard', 'normal', 'cloudflare-python-v1',
            '{resource}', '{launcher}', '{build}', '["workflow.execute"]',
            0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL
        );
    """
    if not resume:
        postgres(project, sql)
    capacity = next(line.partition("=")[2] for line in API_ENV.read_text().splitlines()
                    if line.startswith("BIFROST_WORKER_CAPACITY_TOKEN="))
    key = external_worker_control_key(capacity, incarnation, resource)
    print(json.dumps({"prepared": str(incarnation), "project": project,
                      "workflows": workflows, "entry_sha256": [item["entry_sha256"] for item in approval],
                      "runtime_sha256": sha256(runtime.read_bytes())}), flush=True)
    subprocess.run(
        ["node", str(HERE / "run.mjs")], cwd=ROOT, check=True, text=True,
        input=json.dumps({"origin": origin, "project": project, "admin": admin,
                          "workflows": workflows, "runtime": str(runtime), "hosted": hosted, "port": port,
                          "incarnation": str(incarnation), "launcher": str(launcher),
                          "resource": resource, "build": build, "controlKey": key}),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("prepare").add_argument("--hosted", action="store_true")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("runtime", type=Path, help="Prepared generated-runtime.mjs")
    run_parser.add_argument("--hosted", action="store_true")
    run_parser.add_argument("--resume-enrollment", action="store_true")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.hosted)
    else:
        if args.resume_enrollment and not args.hosted:
            parser.error("--resume-enrollment requires --hosted")
        run(args.runtime.resolve(strict=True), args.hosted, args.resume_enrollment)
