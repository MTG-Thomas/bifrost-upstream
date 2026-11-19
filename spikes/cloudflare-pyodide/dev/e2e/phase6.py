#!/usr/bin/env python3
"""Qualify managed Cloudflare capacity against a disposable local debug API."""

import argparse
import json
import secrets
import subprocess
import time
from io import BytesIO
from pathlib import Path
from uuid import UUID, uuid4, uuid5
from zipfile import ZipFile

from phase5a import API_ENV, ENTRY, FUNCTION, HERE, ROOT, approve_workflow, local_api, prepare, seed_workflows, sha256


def seed_solution_workflows(project, admin, send, tenant_fixtures):
    content = (HERE / "fixture.py").read_text().replace(
        "from bifrost import tables, workflow\n",
        "from bifrost import tables, workflow\nfrom phase6_support.values import INSTALL_VALUE\n",
    ).replace('"table_count": len(rows)', '"table_count": len(rows), "install_value": INSTALL_VALUE')
    manifest_id = uuid4()
    workflows, approvals, results = {}, [], {}
    for scenario, tenant in tenant_fixtures:
        install = send("/api/solutions", {
            "slug": "phase6-fixture", "name": "Phase 6 Fixture", "organization_id": tenant,
        }, admin)["id"]
        helper = f"INSTALL_VALUE = {scenario!r}\n"
        package = BytesIO()
        with ZipFile(package, "w") as archive:
            archive.writestr("bifrost.solution.yaml", "slug: phase6-fixture\nname: Phase 6 Fixture\nversion: '1.0.0'\n")
            archive.writestr(".bifrost/workflows.yaml", json.dumps({"workflows": {str(manifest_id): {
                "name": FUNCTION, "function_name": FUNCTION, "path": ENTRY, "type": "workflow",
                "timeout_seconds": 60,
            }}}))
            archive.writestr(ENTRY, content)
            archive.writestr("phase6_support/values.py", helper)
        boundary = uuid4().hex
        body = (
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"fixture.zip\"\r\n"
            "Content-Type: application/zip\r\n\r\n"
        ).encode() + package.getvalue() + f"\r\n--{boundary}--\r\n".encode()
        job = send(f"/api/solutions/{install}/deploy", body, admin,
                   content_type=f"multipart/form-data; boundary={boundary}")["deploy_job_id"]
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            status = send(f"/api/solutions/deploy-jobs/{job}", None, admin, method="GET")
            if status["status"] == "succeeded":
                break
            if status["status"] == "failed":
                raise SystemExit(f"Fixture Solution deploy failed: {status['error']}")
            time.sleep(0.25)
        else:
            raise SystemExit("Fixture Solution deploy did not settle")
        workflow_id = workflows[scenario] = str(uuid5(UUID(install), str(manifest_id)))
        approval = approve_workflow(project, workflow_id, content.encode(), 12, 1000, solution_id=install)
        assert approval["organization_id"] == tenant
        assert approval["solution"]["version"] == "1.0.0"
        assert approval["module_sha256"] == {"phase6_support/values.py": sha256(helper.encode())}
        approvals.append(approval)
        results[scenario] = {"install_value": scenario}
    return workflows, approvals, results


def run(runtime: Path, solution: bool) -> None:
    if not ROOT.name.startswith(("phase6-", "explore-sol-")) or not API_ENV.is_file() or ROOT.stat().st_mode & 0o077:
        raise SystemExit("Run prepare and start this private phase6 archive's debug stack first")
    origin, project, admin, send = local_api(False)
    send("/api/admin/kubernetes/policy", {"warm_workers": 0, "max_workers": 1, "idle_seconds": 4},
         admin, method="PUT")
    tenants = [send("/api/organizations", {"name": name}, admin)["id"]
               for name in ("Phase 6 Tenant A", "Phase 6 Tenant B")]
    # Register in the actual owner organizations and accept their approvals.
    tenant_fixtures = tuple(zip(("phase6-a", "phase6-b"), tenants, strict=True))
    if solution:
        workflows, approval, results = seed_solution_workflows(project, admin, send, tenant_fixtures)
    else:
        workflows, approval = seed_workflows(project, admin, send, False, tenant_fixtures=tenant_fixtures)
        results = {}
    path = "workflows/phase6_linux_only.py"
    send("/api/files/write", {
        "path": path, "location": "workspace",
        "content": "from bifrost import workflow\n\n@workflow(name='phase6_linux_only')\n"
                   "async def phase6_linux_only():\n    return 6\n",
    }, admin)
    workflows["linux"] = send("/api/workflows/register", {
        "path": path, "function_name": "phase6_linux_only",
    }, admin)["id"]
    print(json.dumps({"project": project, "workflows": workflows, "tenants": tenants,
                      "entry_sha256": {item["workflow_id"]: item["entry_sha256"] for item in approval},
                      "runtime_sha256": sha256(runtime.read_bytes())}), flush=True)
    subprocess.run(["node", str(HERE / "run.mjs")], cwd=ROOT, check=True, text=True, input=json.dumps({
        "phase6": {"token": secrets.token_hex(32), "scope": "phase6-local",
                   "providerHost": (ROOT / ".phase6-host").read_text(), "slots": 2,
                   "tenants": tenants, "results": results},
        "build": "phase6-local", "origin": origin, "project": project, "admin": admin,
        "workflows": workflows, "runtime": str(runtime), "port": 0,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("prepare")
    runner = sub.add_parser("run")
    runner.add_argument("runtime", type=Path)
    runner.add_argument("--solution", action="store_true")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(phase6=True)
    else:
        run(args.runtime.resolve(strict=True), args.solution)
