"""Inject one failure in the local rehearsal after signed hello and bootstrap."""

import os

if os.getpid() == 1 and (generation := os.environ.pop("REHEARSAL_FAIL_GUEST_AFTER_HELLO", "")) in ("1", "2"):
    from src.worker.remote.launcher import Launcher

    async def fail_after_hello(self):
        assert self.session_generation == int(generation)
        await self.note_guest_stage("requirements")
        raise RuntimeError("rehearsal-secret-in-guest-exception")

    Launcher.start = fail_after_hello
