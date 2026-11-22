"""Source submitted to the real execution engine by the Docker fixture."""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def exercise(cancel: bool) -> int:
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
    marker = Path("/tmp/container-fixture-descendant")
    partial = marker.with_suffix(".tmp")
    partial.write_text(str(descendant.pid))
    partial.replace(marker)
    if cancel:
        while True:
            time.sleep(1)
    return descendant.pid
