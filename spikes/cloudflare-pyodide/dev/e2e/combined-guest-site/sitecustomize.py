"""Inject one failure in the local rehearsal after signed hello and bootstrap."""

import os

if os.getpid() == 1 and os.environ.pop("REHEARSAL_FAIL_GUEST_AFTER_HELLO", "") == "1":
    from src.worker.remote.launcher import Launcher

    async def fail_after_hello(self):
        assert self.session_generation == 1
        await self.note_guest_stage("requirements")
        raise RuntimeError("rehearsal-secret-in-guest-exception")

    Launcher.start = fail_after_hello
