"""Start one unprivileged workflow guest in the Container's private PID namespace."""

from __future__ import annotations

import asyncio
import ctypes
import multiprocessing
import os
import signal
import subprocess
import sys
from contextlib import suppress
from multiprocessing.connection import Connection
from pathlib import Path

from src.services.execution.container_result import ContainerResultReader, ContainerResultWriter
from src.services.execution.template_process import (
    TEMPLATE_START_TIMEOUT_SECONDS,
    _load_workflow_runtime,
    _run_forked_child,
    _SendQueue,
)

SUPERVISOR_UID = 1000
GUEST_UID = 1001
GUEST_SCRATCH = "/workspace/guest"
PR_SET_KEEPCAPS = 8
PR_SET_NO_NEW_PRIVS = 38
CAP_KILL = 1 << 5
CAPS_V3 = 0x20080522
READY = b"\x01"
FORCED_STOP_SECONDS = 2


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
                    "SSL_CERT_FILE": "/etc/cloudflare/certs/cloudflare-containers-ca.crt",
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
        with self.ready:
            if not self.ready.poll(TEMPLATE_START_TIMEOUT_SECONDS) or self.ready.recv_bytes(maxlength=1) != READY:
                raise RuntimeError("Container guest did not verify its startup")

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


def _guest_main(ready_fd: int, work_fd: int, result_fd: int) -> None:
    if os.getresuid() != (GUEST_UID,) * 3 or os.getresgid() != (GUEST_UID,) * 3 or os.getgroups():
        raise RuntimeError("Container guest has the wrong identity")
    _prctl(PR_SET_NO_NEW_PRIVS, 1)
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
    if any(int(status[key], 16) for key in ("CapEff", "CapPrm", "CapAmb", "CapInh")):
        raise RuntimeError("Container guest retained capabilities")
    multiprocessing.set_start_method("spawn", force=True)
    ready = Connection(ready_fd, readable=False)
    with ready:
        if _load_workflow_runtime(ready) is None:
            return
        ready.send_bytes(READY)
    with Connection(work_fd, writable=False) as work, Connection(result_fd, readable=False) as result:
        _run_forked_child(work, ContainerResultWriter(result), "container-guest", False)


if __name__ == "__main__":
    _guest_main(*(int(value) for value in sys.argv[1:]))
