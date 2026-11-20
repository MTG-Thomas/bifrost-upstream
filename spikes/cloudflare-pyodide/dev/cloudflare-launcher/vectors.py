"""Conformance vectors for protocol.mjs, produced by the Python modules themselves.

Loads api/src/worker/remote/protocol.py from two git refs (upstream main and
this branch, which adds the signed runtime profile) and #1424's
api/shared/worker_control_auth.py, then writes vectors.json. protocol.py
imports only the standard library, so it loads without the application.

    python vectors.py --main origin/main --branch HEAD --out vectors.json
    python vectors.py ... --check vectors.json   # fails if upstream behaviour moved

Re-run after fetching upstream; a changed vector means the JS side must follow.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

PROTOCOL = "api/src/worker/remote/protocol.py"


def load(ref: str, path: str, name: str):
    source = subprocess.run(["git", "show", f"{ref}:{path}"], check=True, capture_output=True).stdout
    tmp = Path(tempfile.mkdtemp()) / f"{name}.py"
    tmp.write_bytes(source)
    spec = importlib.util.spec_from_file_location(name, tmp)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module


LAUNCHER = UUID("6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f")
INCARNATION = UUID("0e9d8c7b-6a59-4847-b6a5-948372615041")
NONCE = "n0nce-abcdefghijklmnopqrstuvwxyz-0123"


def hello_vectors(protocol, *, with_profile: bool) -> list[dict]:
    cases = [
        {"hostname": "cf-launcher-1", "resource_uid": "cf:launcher:1", "hello_sequence": 1},
        # Non-ASCII hostname: Python escapes it in the signed bytes.
        {"hostname": "cf-launcher-é-漢", "resource_uid": "cf.launcher_2", "hello_sequence": 7},
    ]
    out = []
    for case in cases:
        kwargs = dict(
            launcher_id=LAUNCHER, incarnation_id=INCARNATION, job_uid=None, pod_uid=None,
            hostname=case["hostname"], form="external", issued_at=1794970000, nonce=NONCE,
            hello_sequence=case["hello_sequence"], resource_uid=case["resource_uid"],
        )
        if with_profile:
            kwargs["runtime_profile"] = "cloudflare-python-v1"
        hello = protocol.Hello(**kwargs)
        signed = protocol.sign_hello("control-key-for-vectors", hello)
        out.append({"input": {k: (str(v) if isinstance(v, UUID) else v) for k, v in kwargs.items()},
                    "secret": "control-key-for-vectors",
                    "canonical": hello.canonical_bytes().decode(),
                    "signed": signed.to_dict()})
    return out


def envelope_vectors(protocol) -> dict:
    small = protocol.ControlEnvelope(session_generation=3, sequence=41, kind="event",
                                     payload={"event": "result", "route_id": "r-1",
                                              "result": {"text": "naïve ✓", "n": [1, 2.5, None, True]}})
    # Deterministic message id so fragment bytes are reproducible.
    big = protocol.ControlEnvelope(session_generation=2, sequence=10, kind="command",
                                   payload={"id": "req-1", "action": "workflow",
                                            "payload": {"blob": "x" * 900_000 + "é" * 1000}})
    with patch.object(protocol.secrets, "token_urlsafe", lambda _n: "fixed-message-id-000001"):
        frames = [frame.to_json() for frame in protocol.fragment(big)]
    return {
        "small": {"json": small.to_json()},
        "big": {"session_generation": 2, "sequence": 10, "kind": "command",
                "payload_spec": {"x": 900_000, "e_acute": 1000},
                "message_id": "fixed-message-id-000001",
                "frame_count": len(frames),
                "frame_sequences": [json.loads(f)["sequence"] for f in frames],
                "frames_sha256": hashlib.sha256("\n".join(frames).encode()).hexdigest(),
                "max_frame_bytes": max(len(f.encode()) for f in frames)},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main", default="origin/main")
    parser.add_argument("--branch", default="HEAD")
    parser.add_argument("--check")
    parser.add_argument("--out")
    args = parser.parse_args()
    main_protocol = load(args.main, PROTOCOL, "protocol_main")
    branch_protocol = load(args.branch, PROTOCOL, "protocol_branch")
    sources = {"main": args.main, "branch": args.branch}
    vectors = {
        "sources": {name: subprocess.run(["git", "rev-parse", ref], check=True, capture_output=True,
                                         text=True).stdout.strip() for name, ref in sources.items()},
        "cloudflare_wire": {
            "max_logical_bytes": branch_protocol.CLOUDFLARE_MAX_LOGICAL_BYTES,
            "incarnation_ended": branch_protocol.CLOUDFLARE_INCARNATION_ENDED,
        },
        "hello_main": hello_vectors(main_protocol, with_profile=False),
        "hello_branch_profile": hello_vectors(branch_protocol, with_profile=True),
        "envelopes": envelope_vectors(main_protocol),
    }
    rendered = json.dumps(vectors, indent=1, ensure_ascii=False, sort_keys=True) + "\n"
    if args.check:
        committed = json.loads(Path(args.check).read_text(encoding="utf-8"))
        committed.pop("sources", None)
        current = json.loads(rendered)
        current.pop("sources", None)
        if committed != current:
            print("vectors changed: upstream protocol behaviour moved; update protocol.mjs", file=sys.stderr)
            return 1
        print("vectors unchanged")
        return 0
    if not args.out:
        parser.error("--out or --check is required")
    Path(args.out).write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
