"""Inject one failure in the local rehearsal after signed hello and bootstrap."""

import os
import sys
from functools import lru_cache
from pathlib import Path


def install_startup_fault(mode):
    from src.worker.remote import container_guest

    fault_dir = str(Path(__file__).parent)
    ca_file = Path("/workspace/rc06-fixture-ca")
    error = OSError("rehearsal-secret-in-guest-exception")

    def fail(*_args, **_kwargs):
        raise error

    if os.getpid() == 1:
        # Modify only the disposable guest's view. The supervisor still uses
        # the real injected CA for its signed control connection.
        if mode == "ca-unreadable":
            ca_file.touch(mode=0o600)
        if mode == "scratch-unwritable":
            os.chmod(container_guest.GUEST_SCRATCH, 0o500)
        if mode == "capabilities":
            import ctypes

            libc = ctypes.CDLL(None, use_errno=True)
            header = container_guest._CapabilityHeader(container_guest.CAPS_V3, 0)
            mask = (container_guest._CapabilitySet * 2)()
            assert libc.capget(ctypes.byref(header), ctypes.byref(mask)) == 0
            assert mask[0].permitted & container_guest.CAP_KILL
            mask[0].inheritable |= container_guest.CAP_KILL
            assert libc.capset(ctypes.byref(header), ctypes.byref(mask)) == 0
        popen = container_guest.subprocess.Popen

        def start_child(args, **kwargs):
            if args[1:3] == ["-m", container_guest.__name__]:
                env = kwargs["env"]
                env["PYTHONPATH"] = fault_dir + ":" + env["PYTHONPATH"]
                env["REHEARSAL_GUEST_INIT_FAULT"] = mode
                if mode == "identity":
                    kwargs["group"] = 1002
            return popen(args, **kwargs)

        container_guest.subprocess.Popen = start_child
        print("bifrost-local-fault-hook: " + __file__, flush=True)
        return

    if os.getuid() != 1001:
        return
    # The child runs `-m` as __main__. Reuse the module already loaded by
    # sitecustomize, so the patch affects that exact child entrypoint.
    if mode == "ca-unreadable":
        from src.worker.remote import guest_init

        guest_init.GUEST_CA = ca_file
    elif mode == "ready-exit":
        os._exit(7)
    elif mode == "ready-signal":
        os.kill(os.getpid(), 9)
    elif mode == "shm-unavailable":
        container_guest.multiprocessing.set_start_method = fail
    elif mode in ("runtime-import", "settings", "hook"):
        if mode == "settings":
            from src import config

            @lru_cache
            def bad_settings():
                fail()

            config.get_settings = bad_settings
        elif mode == "hook":
            from src.services.execution import virtual_import

            virtual_import.install_virtual_import_hook = fail
        else:
            sys.modules["src.services.execution.engine"] = None

    # Run the same production child entrypoint after its setup is patched.
    # Set argv only after Python has resolved the -m invocation.
    if mode == "prctl":
        import runpy

        container_guest._prctl = fail
        run_module = runpy._run_module_as_main

        def run_guest(name, *args, **kwargs):
            if name == container_guest.__name__:
                raise SystemExit(container_guest._guest_main(*(int(value) for value in sys.argv[1:])))
            return run_module(name, *args, **kwargs)

        runpy._run_module_as_main = run_guest


if init_fault := os.environ.pop("REHEARSAL_GUEST_INIT_FAULT", ""):
    install_startup_fault(init_fault)

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
