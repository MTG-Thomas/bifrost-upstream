"""Start one unprivileged workflow guest in the Container's private PID namespace."""

from __future__ import annotations

import asyncio
import ctypes
import json
import multiprocessing
import os
import signal
import subprocess
import sys
from contextlib import suppress
from multiprocessing.connection import Connection
from pathlib import Path
from time import monotonic

from src.services.execution.container_result import ContainerResultReader, ContainerResultWriter
from src.services.execution.template_process import (
    TEMPLATE_START_TIMEOUT_SECONDS,
    _load_workflow_runtime,
    _run_forked_child,
    _SendQueue,
)
from src.worker.remote.guest_init import (
    FACTS,
    GUEST_CA,
    GUEST_SCRATCH,
    INIT_CODES,
    READY,
    GuestFacts,
    GuestInitError,
    GuestInitPhase,
    collect_guest_facts,
    proc_status,
    valid_guest_facts,
)

SUPERVISOR_UID = 1000
GUEST_UID = 1001
PR_SET_KEEPCAPS = 8
PR_SET_NO_NEW_PRIVS = 38
CAP_KILL = 1 << 5
CAPS_V3 = 0x20080522
FORCED_STOP_SECONDS = 2
FACTS_LIMIT = 1024


class _CapabilityHeader(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]


class _CapabilitySet(ctypes.Structure):
    _fields_ = [("effective", ctypes.c_uint32), ("permitted", ctypes.c_uint32), ("inheritable", ctypes.c_uint32)]


def _prctl(option: int, value: int) -> None:
    if ctypes.CDLL(None, use_errno=True).prctl(option, value, 0, 0, 0) != 0:
        raise RuntimeError("Container privilege setup failed")


def _drop_supervisor_privileges() -> None:
    if len(list(Path("/proc/self/task").iterdir())) != 1:
        raise RuntimeError("Container supervisor must be single-threaded before dropping privileges")
    _prctl(PR_SET_KEEPCAPS, 1)
    os.setgroups([])
    os.setresgid(SUPERVISOR_UID, SUPERVISOR_UID, SUPERVISOR_UID)
    os.setresuid(SUPERVISOR_UID, SUPERVISOR_UID, SUPERVISOR_UID)
    header = _CapabilityHeader(CAPS_V3, 0)
    masks = (_CapabilitySet * 2)(_CapabilitySet(CAP_KILL, CAP_KILL, 0), _CapabilitySet())
    if ctypes.CDLL(None, use_errno=True).capset(ctypes.byref(header), ctypes.byref(masks)) != 0:
        raise RuntimeError("Container supervisor could not restrict its signal capability")
    _prctl(PR_SET_KEEPCAPS, 0)
    _prctl(PR_SET_NO_NEW_PRIVS, 1)


class ContainerGuest:
    """The image init preopens one set of pipes and irreversibly separates UIDs."""

    def __init__(
        self, process: subprocess.Popen[bytes], ready: Connection, work: Connection, result: Connection,
    ) -> None:
        self.process = process
        self.ready = ready
        self.work = _SendQueue(work)
        self.result = ContainerResultReader(result)
        self.used = False
        self.startup_facts: dict[str, GuestFacts] = {}
        self._stop_lock = asyncio.Lock()

    @classmethod
    def start_image(cls, ingress: str) -> ContainerGuest:
        # PID 1 reaps this dedicated guest's adopted descendants. Fail closed
        # if a provider does not expose the image's private PID namespace.
        if os.getuid() != 0 or os.getpid() != 1:
            raise RuntimeError("Managed Container requires its image init and private PID namespace")
        ready, ready_send = multiprocessing.Pipe(duplex=False)
        work_recv, work = multiprocessing.Pipe(duplex=False)
        result, result_send = multiprocessing.Pipe(duplex=False)
        fds = (ready_send.fileno(), work_recv.fileno(), result_send.fileno())
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", __name__, *(str(fd) for fd in fds)],
                pass_fds=fds, user=GUEST_UID, group=GUEST_UID, extra_groups=[], start_new_session=True,
                cwd=GUEST_SCRATCH, stdin=subprocess.DEVNULL,
                env={
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "HOME": GUEST_SCRATCH, "TMPDIR": GUEST_SCRATCH,
                    "PYTHONPATH": "/app", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1",
                    "SSL_CERT_FILE": str(GUEST_CA),
                    "BIFROST_API_URL": ingress, "BIFROST_BAKED_WHEELS_ONLY": "1",
                    "BIFROST_VERSION": os.environ.get("BIFROST_VERSION", ""),
                },
            )
        finally:
            for pipe in (ready_send, work_recv, result_send):
                pipe.close()
        _drop_supervisor_privileges()
        return cls(process, ready, work, result)

    def wait_ready(self) -> None:
        deadline = monotonic() + TEMPLATE_START_TIMEOUT_SECONDS
        self.startup_facts["supervisor"] = collect_guest_facts(supervisor=True)
        with self.ready:
            os.set_blocking(self.ready.fileno(), False)
            code = self._read_ready_frame(1, deadline)
            if code == FACTS:
                try:
                    facts = json.loads(self._read_ready_frame(FACTS_LIMIT, deadline))
                except (ValueError, RecursionError):
                    raise GuestInitError("ready-unknown", self.process.poll()) from None
                if not valid_guest_facts(facts):
                    raise GuestInitError("ready-unknown", self.process.poll())
                self.startup_facts["child"] = facts
                code = self._read_ready_frame(1, deadline)
                if code == READY:
                    return
            reason = INIT_CODES.get(code)
            raise GuestInitError(
                reason or ("ready-length" if len(code) != 1 else "ready-unknown"),
                self._startup_returncode(deadline),
            )

    def _startup_returncode(self, deadline: float) -> int | None:
        # The kernel may deliver pipe EOF just before waitpid observes exit.
        try:
            return self.process.wait(timeout=min(0.1, max(0, deadline - monotonic())))
        except subprocess.TimeoutExpired:
            return None

    def _read_ready_frame(self, size: int, deadline: float) -> bytes:
        def read_exact(length: int) -> bytes:
            data = bytearray()
            while len(data) < length:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self.ready.poll(remaining):
                    raise GuestInitError("ready-timeout", self.process.poll())
                part = os.read(self.ready.fileno(), length - len(data))
                if not part:
                    raise EOFError
                data.extend(part)
            return bytes(data)

        try:
            # Connection.send_bytes uses a four-byte signed network-order
            # length for small frames. Ready messages never use extended lengths.
            length = int.from_bytes(read_exact(4), "big", signed=True)
            if not 0 <= length <= size:
                raise GuestInitError("ready-length", self.process.poll())
            return read_exact(length)
        except EOFError:
            returncode = self._startup_returncode(deadline)
            reason = "ready-eof" if returncode in (None, 0) else "ready-signal" if returncode < 0 else "ready-exit"
            raise GuestInitError(reason, returncode) from None
        except OSError:
            raise GuestInitError("ready-io", self.process.poll()) from None

    @property
    def exited(self) -> bool:
        return self.process.poll() is not None

    def take(self) -> tuple[int, _SendQueue, ContainerResultReader]:
        if self.used or self.exited:
            raise MemoryError("Container workflow guest is unavailable")
        self.used = True
        return self.process.pid, self.work, self.result

    def _signal_guest(self, signum: int) -> None:
        """Signal only this dedicated UID, even after a descendant leaves its process group."""
        for entry in Path("/proc").iterdir():
            if not entry.name.isdecimal():
                continue
            with suppress(FileNotFoundError, ProcessLookupError):
                pidfd = os.pidfd_open(int(entry.name))
                try:
                    fields = (entry / "status").read_text().splitlines()
                    uid = next((line.split()[1] for line in fields if line.startswith("Uid:")), None)
                    if uid == str(GUEST_UID):
                        signal.pidfd_send_signal(pidfd, signum)
                finally:
                    os.close(pidfd)

    def _reap_guest(self) -> bool:
        if self.process.poll() is not None:
            try:
                while os.waitpid(-1, os.WNOHANG)[0] > 0:
                    pass
            except ChildProcessError:
                return True
        return False

    async def stop(self, grace: float = 0) -> bool:
        """Return true only after the workflow process and all guest descendants are gone."""
        async with self._stop_lock:
            try:
                return await self._stop_and_reap(grace)
            except OSError:
                # Unavailable process evidence must stop the namespace and
                # leave settlement to the provider.
                return False

    async def _stop_and_reap(self, grace: float) -> bool:
        if grace:
            self._signal_guest(signal.SIGTERM)
            deadline = asyncio.get_running_loop().time() + grace
            while asyncio.get_running_loop().time() < deadline:
                if self._reap_guest():
                    return True
                await asyncio.sleep(0.05)
        deadline = asyncio.get_running_loop().time() + FORCED_STOP_SECONDS
        while True:
            if self._reap_guest():
                return True
            self._signal_guest(signal.SIGKILL)
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.05)

    def close(self) -> None:
        for pipe in (self.ready, self.work, self.result):
            pipe.close()


def _initialize_guest(ready: Connection) -> bool:
    phase: GuestInitPhase = "identity"
    facts_sent = False

    def note_phase(value: GuestInitPhase) -> None:
        nonlocal phase
        phase = value

    def send_facts(facts: GuestFacts) -> None:
        nonlocal facts_sent
        ready.send_bytes(FACTS)
        ready.send_bytes(json.dumps(facts, separators=(",", ":")).encode("ascii"))
        facts_sent = True

    try:
        if os.getresuid() != (GUEST_UID,) * 3 or os.getresgid() != (GUEST_UID,) * 3 or os.getgroups():
            raise RuntimeError("Container guest has the wrong identity")
        phase = "prctl"
        _prctl(PR_SET_NO_NEW_PRIVS, 1)
        phase = "capabilities"
        status = proc_status()
        if any(int(status[key], 16) for key in ("CapEff", "CapPrm", "CapAmb", "CapInh")):
            raise RuntimeError("Container guest retained capabilities")
        phase = "other"
        facts = collect_guest_facts(supervisor=False)
        send_facts(facts)
        phase = "scratch-unwritable"
        if not facts["scratch_writable"]:
            raise PermissionError("Container guest scratch is not writable")
        phase = "other"
        multiprocessing.set_start_method("spawn", force=True)
        phase = "ca-unreadable"
        if not facts["ca_readable"]:
            raise PermissionError("Container interception CA is not readable")
        phase = "runtime-import"
        _load_workflow_runtime(None, note_phase=note_phase)
    except Exception:
        # Never send an exception object or text from a guest startup path.
        with suppress(OSError):
            if not facts_sent:
                send_facts(collect_guest_facts(supervisor=False))
            ready.send_bytes(next(code for code, value in INIT_CODES.items() if value == phase))
        return False
    else:
        ready.send_bytes(READY)
        return True


def _guest_main(ready_fd: int, work_fd: int, result_fd: int) -> int:
    with Connection(ready_fd, readable=False) as ready:
        if not _initialize_guest(ready):
            return 1
    with Connection(work_fd, writable=False) as work, Connection(result_fd, readable=False) as result:
        _run_forked_child(work, ContainerResultWriter(result), "container-guest", False)
    return 0


if __name__ == "__main__":
    raise SystemExit(_guest_main(*(int(value) for value in sys.argv[1:])))
