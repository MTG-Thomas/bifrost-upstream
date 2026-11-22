"""Docker conformance fixture. Replace the guest's workflow without an API."""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def _workflow(execution_id, worker_id, context):
    from cryptography.hazmat.bindings._rust import openssl

    assert openssl is not None and os.getuid() == 1001
    assert "BIFROST_WORKER_CONTROL_SECRET" not in os.environ
    for probe in (
        lambda: os.kill(1, 0),
        lambda: Path("/proc/1/environ").read_bytes(),
        lambda: Path("/tmp/bifrost/supervisor-fixture").read_bytes(),
    ):
        try:
            probe()
        except PermissionError:
            pass
        else:
            raise AssertionError("Guest could reach supervisor authority")
    try:
        socket.create_connection(("192.0.2.1", 443), timeout=1).close()
    except OSError:
        pass
    else:
        raise AssertionError("Guest reached an unrelated network")
    assert subprocess.check_output(["/bin/sh", "-c", "printf native"]) == b"native"
    descendant = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"], start_new_session=True)
    Path("/tmp/container-fixture-descendant").write_text(str(descendant.pid))
    if context["cancel"]:
        while True:
            time.sleep(1)
    return {"execution_id": execution_id, "success": True, "duration_ms": 1, "result": descendant.pid}


if os.getuid() == 1001:
    # Install the fixture after the real runtime's requirements and source
    # setup. This module is mounted for the Docker test only.
    from src.services.execution import template_process

    _run = template_process._run_forked_child

    def _run_fixture(*args, **kwargs):
        from src.services.execution import simple_worker

        simple_worker._execute_sync = _workflow
        _run(*args, **kwargs)

    template_process._run_forked_child = _run_fixture
