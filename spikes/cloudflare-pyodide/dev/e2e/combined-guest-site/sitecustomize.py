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
    import runpy

    run_module = runpy._run_module_as_main

    def run_guest(name, *args, **kwargs):
        if name != container_guest.__name__:
            return run_module(name, *args, **kwargs)
        # Once interpreter initialization finishes, patch the same module
        # whose guest entrypoint runs. Native imports must not run in sitecustomize.
        if mode == "ca-unreadable":
            from src.worker.remote import guest_init

            guest_init.GUEST_CA = ca_file
        elif mode == "ready-exit":
            os._exit(7)
        elif mode == "ready-signal":
            os.kill(os.getpid(), 9)
        elif mode == "prctl":
            container_guest._prctl = fail
        elif mode == "settings":
            from src import config

            get_settings = config.get_settings

            @lru_cache
            def bad_settings():
                if "BIFROST_SECRET_KEY" in os.environ:
                    fail()
                return get_settings()

            config.get_settings = bad_settings
        elif mode == "hook":
            from src.services.execution import virtual_import

            install = virtual_import.install_virtual_import_hook
            load_runtime = container_guest._load_workflow_runtime
            final_hook = False

            def install_or_fail():
                if final_hook:
                    fail()
                return install()

            def mark_hook(pipe, *, note_phase):
                def note(value):
                    nonlocal final_hook
                    final_hook = value == "hook"
                    note_phase(value)

                return load_runtime(pipe, note_phase=note)

            virtual_import.install_virtual_import_hook = install_or_fail
            container_guest._load_workflow_runtime = mark_hook
        elif mode == "runtime-import":
            sys.modules["src.services.execution.engine"] = None
        raise SystemExit(container_guest._guest_main(*(int(value) for value in sys.argv[1:])))

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
