"""Run the Linux guest and launcher inside the release image."""

import asyncio
import os
import sys
from base64 import b64encode
from pathlib import Path
from queue import Empty
from uuid import uuid4

from src.worker.remote.container_guest import ContainerGuest
from src.worker.remote.launcher import Launcher, LauncherIdentity, _receive_control_commands
from src.worker.remote.protocol import ControlEnvelope


def workflow_context(cancel: bool) -> dict:
    script = Path(__file__).with_name("guest_workflow.py").read_text() + "\nreturn exercise(cancel)\n"
    return {
        "code": b64encode(script.encode()).decode("ascii"),
        "name": "container-guest-conformance",
        "engine_token": "synthetic-run-credential",
        "caller": {"user_id": str(uuid4()), "email": "fixture@example.test", "name": "Fixture"},
        "parameters": {"cancel": cancel},
        "transient": True,
        "no_cache": True,
    }


class ControlSocket:
    def __init__(self):
        self.api = asyncio.Queue()
        self.launcher = asyncio.Queue()

    async def send(self, frame: str):
        await self.launcher.put(ControlEnvelope.from_json(frame).payload)

    async def recv(self):
        return await self.api.get()

    async def close(self, **_kwargs):
        pass

    def command(self, sequence: int, kind: str, payload: dict):
        self.api.put_nowait(ControlEnvelope(
            session_generation=1, sequence=sequence, kind=kind, payload=payload,
        ).to_json())

    async def event(self, name: str):
        while True:
            payload = await self.launcher.get()
            if payload.get("event") == name or payload.get("reply") == name:
                return payload


async def exercise_launcher() -> None:
    guest = ContainerGuest.start_image("https://ingress.example.test")
    assert os.getuid() == 1000 and os.getpid() == 1
    Path("/tmp/bifrost/supervisor-fixture").write_text("synthetic")
    socket = ControlSocket()
    launcher = Launcher(
        socket, identity=LauncherIdentity(
            launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
            hostname="fixture", form="external", resource_uid=uuid4().hex,
        ), secret="synthetic", containers=True, container_release="a" * 64,
        container_idle_seconds=120, container_guest=guest,
    )
    launcher.session_generation = 1
    receiver = asyncio.create_task(_receive_control_commands(launcher, socket, asyncio.Event()))
    monitor = None
    try:
        await asyncio.to_thread(guest.wait_ready)
        execution = str(uuid4())
        async with asyncio.timeout(10):
            socket.command(1, "command", {
                "id": "policy", "action": "configure",
                "payload": {"pressure_threshold": 0.98, "dangerous_memory_ratio": 0.99},
            })
            assert "error" not in await socket.event("policy")
            socket.command(2, "command", {
                "id": "run", "action": "workflow", "payload": {
                    "route_id": execution, "execution_id": execution, "context": workflow_context(False),
                },
            })
            reply = await socket.event("run")
            assert "result" in reply, reply
            pid = reply["result"]["pid"]
            monitor = launcher.task(launcher.monitor())
            launcher._monitor_task = monitor
            terminal = await socket.event("result")
            result = terminal["result"]
            assert result["execution_id"] == execution and result["success"] is True, result
            descendant = int(Path("/tmp/container-fixture-descendant").read_text())
            assert result["result"] == descendant
            assert not Path(f"/proc/{pid}").exists()
            assert not Path(f"/proc/{descendant}").exists()
            assert not launcher.children
            await socket.event("draining")
            assert not launcher.retired.is_set()
            socket.command(3, "command", {
                "id": "second", "action": "workflow", "payload": {"route_id": str(uuid4())},
            })
            assert (await socket.event("second"))["error_type"] == "MemoryError"
            socket.command(4, "ack", {"event_id": terminal["_event_id"]})
            await launcher.retired.wait()
            await receiver
    finally:
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
        await launcher.stop()
    print("Container launcher ran once, drained and retired")


async def exercise(cancel: bool) -> None:
    guest = ContainerGuest.start_image("https://ingress.example.test")
    assert os.getuid() == 1000 and os.getpid() == 1
    Path("/tmp/bifrost/supervisor-fixture").write_text("synthetic")
    try:
        await asyncio.to_thread(guest.wait_ready)
        pid, work, result = guest.take()
        execution = str(uuid4())
        work.put_nowait((execution, workflow_context(cancel)))
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
    assert scenario in ("isolation", "cancel", "launcher")
    asyncio.run(exercise_launcher() if scenario == "launcher" else exercise(cancel=scenario == "cancel"))
