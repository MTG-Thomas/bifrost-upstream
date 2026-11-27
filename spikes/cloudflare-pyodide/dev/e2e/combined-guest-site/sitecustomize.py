"""Inject one failure in the local rehearsal after signed hello and bootstrap."""

import os

if os.getpid() == 1 and (generation := os.environ.pop("REHEARSAL_FAIL_GUEST_AFTER_HELLO", "")) in ("1", "2"):
    from websockets.exceptions import ConnectionClosed

    from src.worker.remote.launcher import Launcher

    if __file__ == "/opt/bifrost-fault-hook/sitecustomize.py":
        print("bifrost-local-fault-hook: " + __file__, flush=True)

    hello = Launcher.hello
    saw_bootstrap_drop = False

    async def witness_bootstrap_drop(self, requirements):
        global saw_bootstrap_drop
        try:
            await hello(self, requirements)
        except ConnectionClosed:
            saw_bootstrap_drop = self.session_generation == 1
            raise

    async def fail_after_hello(self):
        assert self.session_generation == int(generation)
        assert generation == "1" or saw_bootstrap_drop
        await self.note_guest_stage("requirements")
        raise RuntimeError("rehearsal-secret-in-guest-exception")

    if generation == "2":
        Launcher.hello = witness_bootstrap_drop
    Launcher.start = fail_after_hello
