"""Direct, isolated image conformance for the dedicated Linux guest."""

import asyncio
import os
import sys
from pathlib import Path
from queue import Empty

from src.worker.remote.container_guest import ContainerGuest


async def exercise(cancel: bool) -> None:
    guest = ContainerGuest.start_image("https://ingress.example.test")
    assert os.getuid() == 1000 and os.getpid() == 1
    Path("/tmp/bifrost/supervisor-fixture").write_text("synthetic")
    try:
        await asyncio.to_thread(guest.wait_ready)
        pid, work, result = guest.take()
        work.put_nowait(("fixture", {"cancel": cancel}))
        marker = Path("/tmp/container-fixture-descendant")
        async with asyncio.timeout(10):
            while not marker.is_file() or not marker.read_text():
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
                assert outcome == {"execution_id": "fixture", "success": True, "duration_ms": 1, "result": descendant}
        assert Path(f"/proc/{descendant}").is_dir()
        assert await guest.stop(grace=0.5 if cancel else 0)
        assert not Path(f"/proc/{pid}").exists()
        assert not Path(f"/proc/{descendant}").exists()
    finally:
        guest.close()
    print("Container guest cancellation passed" if cancel else "Container guest isolation passed")


if __name__ == "__main__":
    asyncio.run(exercise(cancel=sys.argv[1] == "cancel"))
