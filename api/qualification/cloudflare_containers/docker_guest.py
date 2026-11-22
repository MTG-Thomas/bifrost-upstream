"""Direct, isolated image conformance for the dedicated Linux guest."""

import asyncio
import os
import sys
from base64 import b64encode
from pathlib import Path
from queue import Empty
from uuid import uuid4

from src.worker.remote.container_guest import ContainerGuest


async def exercise(cancel: bool) -> None:
    guest = ContainerGuest.start_image("https://ingress.example.test")
    assert os.getuid() == 1000 and os.getpid() == 1
    Path("/tmp/bifrost/supervisor-fixture").write_text("synthetic")
    try:
        await asyncio.to_thread(guest.wait_ready)
        pid, work, result = guest.take()
        execution = str(uuid4())
        script = Path(__file__).with_name("guest_workflow.py").read_text() + "\nreturn exercise(cancel)\n"
        work.put_nowait((execution, {
            "code": b64encode(script.encode()).decode("ascii"),
            "name": "container-guest-conformance",
            "engine_token": "synthetic-run-credential",
            "caller": {"user_id": str(uuid4()), "email": "fixture@example.test", "name": "Fixture"},
            "parameters": {"cancel": cancel},
            "transient": True,
            "no_cache": True,
        }))
        marker = Path("/tmp/container-fixture-descendant")
        async with asyncio.timeout(10):
            while not marker.is_file() or not marker.read_text():
                if guest.exited:
                    try:
                        outcome = result.get_nowait()
                    except (Empty, EOFError):
                        outcome = None
                    raise AssertionError(f"Guest exited before creating its detached descendant: {outcome!r}")
                await asyncio.sleep(0.05)
            descendant = int(marker.read_text())
            assert os.getpgid(descendant) == descendant != pid
            if not cancel:
                while True:
                    try:
                        outcome = result.get_nowait()
                        break
                    except Empty:
                        await asyncio.sleep(0.05)
                assert outcome["execution_id"] == execution and outcome["success"] is True, outcome
                assert outcome["status"] == "Success" and outcome["result"] == descendant
                assert outcome["duration_ms"] >= 0
        assert Path(f"/proc/{descendant}").is_dir()
        assert await guest.stop(grace=0.5 if cancel else 0)
        assert not Path(f"/proc/{pid}").exists()
        assert not Path(f"/proc/{descendant}").exists()
    finally:
        guest.close()
    print("Container guest cancellation passed" if cancel else "Container guest isolation passed")


if __name__ == "__main__":
    scenario = sys.argv[1]
    assert scenario in ("isolation", "cancel")
    asyncio.run(exercise(cancel=scenario == "cancel"))
