"""Bounded physical Git and descriptor computation for a Container stage."""

from __future__ import annotations

import os
import resource
import subprocess
import sys
from pathlib import Path

from bifrost.solution_descriptor import DESCRIPTOR_FILENAME, parse_descriptor
from shared.solution_git_stage import (
    MAX_DESCRIPTOR_BYTES,
    UPDATE_INPUT,
    GitInput,
    VersionResult,
)

from src.worker.remote.platform_stages import read_stage_input

MAX_GIT_FILE_BYTES = 64 * 1024 * 1024


def _git(*args: str) -> bytes:
    options = [
        "-c", "protocol.allow=never", "-c", "protocol.https.allow=always",
        "-c", "http.followRedirects=false", "-c", "credential.helper=",
        "-c", "fetch.unpackLimit=1", "-c", "maintenance.auto=false",
    ]
    if ca := os.environ.get("SSL_CERT_FILE"):
        options.extend(["-c", f"http.sslCAInfo={ca}"])
    return subprocess.check_output(
        ["git", *options, *args], stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _fetch(stage: GitInput, checkout: Path) -> tuple[bytes, str]:
    branch = [] if stage.ref is None else [f"--branch={stage.ref}"]
    _git("clone", "--depth=1", "--single-branch", "--no-tags", "--no-checkout",
         "--template=", *branch, "--", stage.repo, str(checkout))
    commit = _git("-C", str(checkout), "rev-parse", "HEAD").decode().strip()
    path = f"{stage.subpath}/{DESCRIPTOR_FILENAME}" if stage.subpath else DESCRIPTOR_FILENAME
    entry = _git("-C", str(checkout), "--literal-pathspecs", "ls-tree", "-z", commit, "--", path)
    if not entry.startswith((b"100644 blob ", b"100755 blob ")) or entry.count(b"\x00") != 1:
        raise ValueError("A regular Solution descriptor is required")
    object_id = entry.split(b"\t", 1)[0].split()[2].decode()
    size = int(_git("-C", str(checkout), "cat-file", "-s", object_id))
    if size > MAX_DESCRIPTOR_BYTES:
        raise ValueError("Solution descriptor exceeds its byte limit")
    return _git("-C", str(checkout), "cat-file", "blob", object_id), commit


def main(root: Path) -> None:
    # Keep the received Git pack on disk and cap individual pack/index files.
    # The launcher owns the wall clock, process group, and cgroup admission.
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_GIT_FILE_BYTES, MAX_GIT_FILE_BYTES))
    stage = UPDATE_INPUT.validate_json(read_stage_input())
    if isinstance(stage, GitInput):
        descriptor, commit = _fetch(stage, root / "git")
    else:
        descriptor, commit = stage.content.encode(), None
    result = VersionResult(version=parse_descriptor(descriptor.decode()).version, commit=commit)
    print(result.model_dump_json(), flush=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
