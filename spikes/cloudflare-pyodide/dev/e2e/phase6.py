#!/usr/bin/env python3
"""Qualify managed Cloudflare capacity against a disposable local debug API."""

import argparse
import json
import secrets
import subprocess
from pathlib import Path

from phase5a import API_ENV, HERE, ROOT, local_api, prepare, seed_workflows, sha256


def run(runtime: Path) -> None:
    if not ROOT.name.startswith("phase6-") or not API_ENV.is_file() or ROOT.stat().st_mode & 0o077:
        raise SystemExit("Run prepare and start this private phase6 archive's debug stack first")
    origin, project, admin, send = local_api(False)
    send("/api/admin/kubernetes/policy", {"warm_workers": 0, "max_workers": 1, "idle_seconds": 4},
         admin, method="PUT")
    workflows, approval = seed_workflows(project, admin, send, False)
    path = "workflows/phase6_linux_only.py"
    send("/api/files/write", {
        "path": path, "location": "workspace",
        "content": "from bifrost import workflow\n\n@workflow(name='phase6_linux_only')\n"
                   "async def phase6_linux_only():\n    return 6\n",
    }, admin)
    workflows["linux"] = send("/api/workflows/register", {
        "path": path, "function_name": "phase6_linux_only",
    }, admin)["id"]
    print(json.dumps({"project": project, "workflows": workflows,
                      "entry_sha256": approval[0]["entry_sha256"],
                      "runtime_sha256": sha256(runtime.read_bytes())}), flush=True)
    subprocess.run(["node", str(HERE / "run.mjs")], cwd=ROOT, check=True, text=True, input=json.dumps({
        "phase6": {"token": secrets.token_hex(32), "scope": "phase6-local",
                   "providerHost": (ROOT / ".phase6-host").read_text()},
        "build": "phase6-local", "origin": origin, "project": project, "admin": admin,
        "workflows": workflows, "runtime": str(runtime), "port": 0,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("prepare")
    sub.add_parser("run").add_argument("runtime", type=Path)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(phase6=True)
    else:
        run(args.runtime.resolve(strict=True))
