"""Set up a local Container startup condition or inject one guest failure."""

import os
import sys
from functools import lru_cache
from pathlib import Path


def set_init_inheritable(value, container_guest):
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    header = container_guest._CapabilityHeader(container_guest.CAPS_V3, 0)
    masks = (container_guest._CapabilitySet * 2)()
    assert libc.capget(ctypes.byref(header), ctypes.byref(masks)) == 0
    for index, mask in enumerate(masks):
        mask.inheritable = (value >> (32 * index)) & 0xffffffff
    assert libc.capset(ctypes.byref(header), ctypes.byref(masks)) == 0
    assert int(container_guest.proc_status()["CapInh"], 16) == value


def deny_bounding_drop(partial, container_guest):
    import ctypes
    import errno

    class Comparison(ctypes.Structure):
        _fields_ = [
            ("arg", ctypes.c_uint), ("op", ctypes.c_int),
            ("datum_a", ctypes.c_uint64), ("datum_b", ctypes.c_uint64),
        ]

    seccomp = ctypes.CDLL("libseccomp.so.2")
    seccomp.seccomp_init.argtypes = [ctypes.c_uint32]
    seccomp.seccomp_init.restype = ctypes.c_void_p
    seccomp.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    seccomp.seccomp_rule_add_array.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint, ctypes.POINTER(Comparison),
    ]
    seccomp.seccomp_load.argtypes = [ctypes.c_void_p]
    seccomp.seccomp_release.argtypes = [ctypes.c_void_p]
    bounding = int(container_guest.proc_status()["CapBnd"], 16)
    bits = [bit for bit in range(bounding.bit_length()) if bounding & (1 << bit)]
    assert len(bits) > 1
    # Stack a narrower in-process filter over Docker's original seccomp policy.
    comparisons = [Comparison(0, 4, container_guest.PR_CAPBSET_DROP, 0)]  # SCMP_CMP_EQ
    if partial:
        comparisons.append(Comparison(1, 4, bits[1], 0))
    args = (Comparison * len(comparisons))(*comparisons)
    ctx = seccomp.seccomp_init(0x7fff0000)  # SCMP_ACT_ALLOW
    assert ctx
    try:
        assert seccomp.seccomp_rule_add_array(
            ctx, 0x00050000 | errno.EPERM, seccomp.seccomp_syscall_resolve_name(b"prctl"), len(args), args,
        ) == 0
        assert seccomp.seccomp_load(ctx) == 0
    finally:
        seccomp.seccomp_release(ctx)
    print(f"bifrost-local-seccomp-denied-bit: {bits[1 if partial else 0]}", flush=True)
    prctl = container_guest._prctl

    def witness_drop(option, value):
        prctl(option, value)
        if option == container_guest.PR_CAPBSET_DROP:
            print(f"bifrost-local-bounding-drop-succeeded: {value}", flush=True)

    container_guest._prctl = witness_drop


def install_startup_fault(mode):
    from src.worker.remote import container_guest

    fault_dir = str(Path(__file__).parent)
    ca_file = Path("/workspace/rc06-fixture-ca")
    error = OSError("rehearsal-secret-in-guest-exception")

    def fail(*_args, **_kwargs):
        raise error

    if os.getpid() == 1:
        if mode in ("supervisor-seccomp", "supervisor-seccomp-partial"):
            deny_bounding_drop(mode.endswith("-partial"), container_guest)
        if mode == "hosted-capabilities":
            bounding = int(container_guest.proc_status()["CapBnd"], 16)
            assert bounding
            set_init_inheritable(bounding, container_guest)
            print("bifrost-local-hosted-capabilities: PID 1 inheritable equals nonzero bounding", flush=True)
        # Modify only the disposable guest's view. The supervisor still uses
        # the real injected CA for its signed control connection.
        if mode == "ca-unreadable":
            ca_file.touch(mode=0o600)
        if mode == "scratch-unwritable":
            os.chmod(container_guest.GUEST_SCRATCH, 0o500)
        if mode == "capabilities":
            # Preserve one bounding bit so the fixture can inject an inheritable
            # capability immediately before the UID transition, after cleanup.
            prctl = container_guest._prctl

            def keep_kill(option, value):
                if (option, value) != (container_guest.PR_CAPBSET_DROP, container_guest.CAP_KILL.bit_length() - 1):
                    prctl(option, value)

            container_guest._prctl = keep_kill
        popen = container_guest.subprocess.Popen

        def start_child(args, **kwargs):
            if args[1:3] == ["-m", container_guest.__name__]:
                assert not mode.startswith("supervisor-seccomp"), "a denied bounding drop reached Popen"
                if mode == "capabilities":
                    assert int(container_guest.proc_status()["CapInh"], 16) == 0
                    set_init_inheritable(container_guest.CAP_KILL, container_guest)
                env = kwargs["env"]
                env["PYTHONPATH"] = fault_dir + ":" + env["PYTHONPATH"]
                if mode != "hosted-capabilities":
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
