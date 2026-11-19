#!/usr/bin/env python3
"""Prepare and enroll a disposable phase 5 debug stack."""

import argparse
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
API_ENV = ROOT / ".phase5-api.env"
ENTRY = "workflows/phase5a_fixture.py"
FUNCTION = "phase5a_fixture"
SUMMARY_ENDPOINT = "https://phase5-fixture.invalid/v1"


def command(*args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, text=True, **kwargs).strip()


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def prepare(hosted=False, phase6=False, summary=False):
    prefix = "explore-agent-" if summary else "phase6-" if phase6 else "phase5b-" if hosted else "phase5-"
    valid_prefix = (prefix, "explore-sol-") if phase6 else prefix
    if not ROOT.name.startswith(valid_prefix) or command("git", "remote") or command(
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
        if summary:
            env.write(f"BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT={SUMMARY_ENDPOINT}\n")
        if not phase6:
            env.write(f"BIFROST_WORKER_CAPACITY_TOKEN={secrets.token_hex(32)}\n")
    for directory in ("node_modules", "sdk_src"):
        (ROOT / "api/src/services/sdk_package" / directory).mkdir(parents=True, exist_ok=True)
    compose = ROOT / "docker-compose.debug.yml"
    original = compose.read_bytes()
    before, api = original.split(b"\n  api:\n", 1)
    api = api.replace(
        b"    volumes:\n",
        b'    ports:\n      - "127.0.0.1::8000"\n'
        b"    volumes:\n      - ./api/scripts:/app/scripts:ro\n"
        b"      - ./.phase5-api.env:/app/.env:ro\n",
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
    if phase6:
        host = command("docker", "network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}")
        if not re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host):
            raise SystemExit("Expected a local IPv4 Docker bridge gateway")
        key = ROOT / ".phase6-key.pem"
        cert = ROOT / ".phase6-ca.pem"
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-days", "1",
            "-subj", "/CN=phase6 local fixture", "-addext", f"subjectAltName=IP:{host}",
            "-keyout", str(key), "-out", str(cert),
        ], cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cert.chmod(0o644)
        (ROOT / ".phase6-host").write_text(host)
        before, api = compose.read_bytes().split(b"\n  api:\n", 1)
        api = api.replace(b"    environment:\n", b"    environment:\n      SSL_CERT_FILE: /app/.phase6-ca.pem\n", 1)
        api = api.replace(b"    volumes:\n", b"    volumes:\n      - ./.phase6-ca.pem:/app/.phase6-ca.pem:ro\n", 1)
        compose.write_bytes(before + b"\n  api:\n" + api)


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

    def send(path, body, token=None, method="POST", *, content_type=None):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if token:
            headers = {"Content-Type": content_type or "application/json", "Authorization": f"Bearer {token}"}
            if content_type is None:
                body = json.dumps(body).encode() if body is not None else None
        try:
            with urlopen(Request(origin + path, data=body, headers=headers, method=method), timeout=15) as response:
                return None if response.status == 204 else json.load(response)
        except HTTPError as error:
            raise SystemExit(f"{method} {path} returned HTTP {error.code}") from None

    admin = send("/auth/login", urlencode({"username": login[1], "password": login[2]}).encode())["access_token"]
    return origin, project, admin, send


def seed_workflows(project, admin, send, hosted, *, tenant_fixtures=()):
    fixtures = (
        ("hosted", "phase5b_fixture", "fixture-hosted.py", 10, 1000, None),
        ("allowance", "phase5b_allowance", "fixture-hosted.py", 4, 1000, None),
    ) if hosted else (("local", FUNCTION, "fixture.py", 10, 1000, None),)
    if tenant_fixtures:
        fixtures = tuple(
            (kind, kind.replace("-", "_"), "fixture.py", 10, 1000, tenant)
            for kind, tenant in tenant_fixtures
        )
    approval = []
    workflows = {}
    for kind, function, filename, allowance, cpu, tenant in fixtures:
        entry = ENTRY if kind == "local" else f"workflows/{function}.py"
        content = (HERE / filename).read_bytes()
        if kind == "allowance":
            content = content.replace(b"phase5b_fixture", b"phase5b_allowance")
        if tenant:
            content = content.replace(FUNCTION.encode(), function.encode())
        send("/api/files/write", {"path": entry, "content": content.decode(), "location": "workspace"}, admin)
        registration = {"path": entry, "function_name": function}
        if tenant:
            registration["organization_id"] = tenant
        workflow = send("/api/workflows/register", registration, admin)
        if tenant and workflow["organization_id"] != tenant:
            raise SystemExit("Fixture workflow was registered in the wrong tenant")
        workflow_id = workflows[kind] = workflow["id"]
        send(f"/api/workflows/{workflow_id}", {"timeout_seconds": 60}, admin, method="PATCH")
        approval.append(approve_workflow(project, workflow_id, content, allowance, cpu))
    return workflows, approval


def approve_workflow(project, workflow_id, content, allowance, cpu, *, solution_id=None):
    # Keep database diagnostics on stderr and the proposal JSON on stdout.
    operator = ["docker", "exec", f"{project}-api-1", "python", "-c", """
import contextlib
import sys
output = sys.stdout
with contextlib.redirect_stdout(sys.stderr):
    from scripts import cloudflare_workflow_approval as operator
    operator.print = lambda *args, **kwargs: print(*args, file=output, **kwargs)
    operator.main()
"""]
    arguments = [
        workflow_id, "--subrequest-allowance", str(allowance), "--cpu-budget-ms", str(cpu),
    ]
    proposal = json.loads(command(*operator, "propose", *arguments))
    if (proposal["workflow_id"] != workflow_id or proposal["entry_sha256"] != sha256(content)
        or (proposal.get("solution") or {}).get("install_id") != solution_id):
        raise SystemExit("Generated proposal does not match the disposable fixture")
    return json.loads(command(
        *operator, "accept", *arguments, "--review-sha256", proposal["review_sha256"],
        "--operator", "phase5/6 disposable fixture",
    ))


def postgres(project, sql):
    return command("docker", "exec", "-i", f"{project}-postgres-1", "psql", "-XqAt",
                   "-v", "ON_ERROR_STOP=1", "-U", "bifrost", "-d", "bifrost", input=sql)


def resume_enrollment(project, reset_route):
    enrollment = json.loads((ROOT / ".phase5b-deploy.json").read_text())
    incarnation, launcher = UUID(enrollment["incarnation"]), UUID(enrollment["launcher"])
    pending = UUID(reset_route.split(":")[1]) if reset_route else UUID(int=0)
    idle = postgres(project, f"""
        SELECT count(*) FROM execution_workers
        WHERE id='{incarnation}' AND remote_launcher_id='{launcher}' AND state IN ('starting', 'ready')
          AND runtime_profile='cloudflare-python-v1'
          AND active_runs <= {1 if reset_route else 0}
          AND (active_runs=0 OR EXISTS (
              SELECT FROM execution_worker_claims WHERE worker_id='{incarnation}' AND subject_id='{pending}'
          ))
          AND NOT EXISTS (
              SELECT FROM execution_worker_claims c JOIN executions e ON e.id=c.subject_id
              WHERE c.worker_id='{incarnation}' AND e.completed_at IS NULL AND e.id <> '{pending}'
          );
    """)
    if idle != "1":
        raise SystemExit("Expected an idle hosted enrollment or the named interrupted claim")
    if not 1024 <= enrollment["port"] <= 65535:
        raise SystemExit("Invalid hosted relay port")
    approval = json.loads(postgres(project, """
        SELECT json_agg(a.declaration || jsonb_build_object('workflow_id', w.id)
                        ORDER BY w.function_name DESC)
        FROM cloudflare_workflow_approvals a JOIN workflows w ON a.workflow_id=w.id
        WHERE a.is_current AND w.organization_id IS NULL AND w.solution_id IS NULL
          AND w.function_name IN ('phase5b_fixture', 'phase5b_allowance')
          AND w.path = 'workflows/' || w.function_name || '.py';
    """) or "null")
    if approval is None or len(approval) != 2:
        raise SystemExit("Expected the two durable hosted fixture approvals")
    workflows = {kind: str(UUID(item["workflow_id"])) for kind, item in zip(
        ("hosted", "allowance"), approval, strict=True)}
    return workflows, approval, incarnation, launcher, enrollment["port"]


def run(runtime, hosted, resume, reset_route, from_reset, summary=False):
    sys.path.insert(0, str(ROOT / "api"))
    from shared.worker_control_auth import external_worker_control_key

    prefix = "explore-agent-" if summary else "phase5b-" if hosted else "phase5-"
    if not API_ENV.is_file() or not ROOT.name.startswith(prefix):
        raise SystemExit("Run prepare and start this disposable archive's debug stack first")
    origin, project, admin, send = local_api(hosted)
    port = 0
    summary_config = None
    if summary:
        run_id = uuid4()
        database_key = "api-fixture-only-" + secrets.token_hex(16)
        summary_config = {"run": str(run_id), "endpoint": SUMMARY_ENDPOINT,
                          "databaseKey": database_key}
        subprocess.run(["docker", "exec", "-i", f"{project}-api-1", "python", "-",
                        str(run_id), SUMMARY_ENDPOINT, database_key], cwd=ROOT, check=True, text=True, input='''
import asyncio
import sys
from uuid import UUID

from src.core.database import get_db_context
from src.models.orm.agent_runs import AgentRun
from src.services.ai_model_service import AIModelService

async def seed():
    async with get_db_context() as db:
        ai = AIModelService(db)
        connection = await ai.create_connection(name="Disposable stub", provider="openai_compatible",
                                                api_key=sys.argv[3], endpoint=sys.argv[2])
        profile = await ai.create_profile(name="Disposable summary", connection_id=connection.id,
                                          model="gpt-4o-mini", capabilities=None, enabled_for_chat=False)
        await ai.set_assignment("summarization", profile.id)
        profile.openai_transport = "chat_completions"
        db.add(AgentRun(id=UUID(sys.argv[1]), status="completed", trigger_type="sdk",
                        input={"question": "Cloudflare summary fixture"}, output={"answer": "done"}))
        await db.commit()

asyncio.run(seed())
''')
        workflows, approval, incarnation, launcher = {}, [], uuid4(), uuid4()
    elif resume:
        workflows, approval, incarnation, launcher, port = resume_enrollment(project, reset_route)
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
                          "resetRoute": reset_route,
                          "fromReset": from_reset, "summary": summary_config,
                          "incarnation": str(incarnation), "launcher": str(launcher),
                          "resource": resource, "build": build, "controlKey": key}),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prepare_modes = sub.add_parser("prepare").add_mutually_exclusive_group()
    prepare_modes.add_argument("--hosted", action="store_true")
    prepare_modes.add_argument("--summary", action="store_true")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("runtime", type=Path, help="Prepared generated-runtime.mjs")
    run_modes = run_parser.add_mutually_exclusive_group()
    run_modes.add_argument("--hosted", action="store_true")
    run_modes.add_argument("--summary", action="store_true")
    run_parser.add_argument("--resume-enrollment", action="store_true")
    reset = run_parser.add_mutually_exclusive_group()
    reset.add_argument("--resume-reset", metavar="WORKFLOW_ROUTE")
    reset.add_argument("--from-reset", action="store_true")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.hosted, summary=args.summary)
    else:
        if args.resume_enrollment and not args.hosted:
            parser.error("--resume-enrollment requires --hosted")
        if args.from_reset and not args.resume_enrollment:
            parser.error("--from-reset requires --resume-enrollment")
        if args.resume_reset and (
            not args.resume_enrollment or
            not re.fullmatch(r"workflow:[0-9a-f-]{36}:[0-9a-f-]{36}", args.resume_reset)
        ):
            parser.error("--resume-reset requires --resume-enrollment and an interrupted workflow route")
        run(args.runtime.resolve(strict=True), args.hosted, args.resume_enrollment,
            args.resume_reset, args.from_reset, summary=args.summary)
