"""Bounded, non-secret facts and readiness outcomes for the Linux guest."""

import os
import re
from contextlib import suppress
from pathlib import Path
from typing import Literal, get_args

GUEST_CA = Path("/etc/cloudflare/certs/cloudflare-containers-ca.crt")
GUEST_SCRATCH = "/workspace/guest"
SHM = Path("/dev/shm")
CAP_MASKS = ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")
GuestFact = bool | int | str | None
GuestFacts = dict[str, GuestFact]
GuestInitPhase = Literal[
    "identity", "prctl", "capabilities", "ca-unreadable", "shm-unavailable",
    "scratch-unwritable", "runtime-import", "settings", "hook", "other",
]
_SIZE_BUCKETS = ("empty", "under-4k", "under-64k", "under-1m", "under-64m", "under-1g", "under-4g", "4g-or-more")
READY = b"\x01"
FACTS = b"\x02"
INIT_CODES: dict[bytes, GuestInitPhase] = {
    bytes([value]): phase for value, phase in enumerate(get_args(GuestInitPhase), start=16)
}
INIT_REASONS = frozenset(INIT_CODES.values()) | {
    "ready-timeout", "ready-eof", "ready-length", "ready-unknown", "ready-io",
    "ready-exit", "ready-signal",
}


class GuestInitError(RuntimeError):
    def __init__(self, reason: str, returncode: int | None = None):
        super().__init__("Managed Container guest initialization failed")
        self.reason = reason
        self.returncode = returncode


def proc_status() -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in Path("/proc/self/status").read_text().splitlines():
        name, separator, value = line.partition(":")
        if separator:
            fields[name] = value.strip()
    return fields


def _size_bucket(size: int) -> str:
    if size == 0:
        return _SIZE_BUCKETS[0]
    return next(
        (bucket for limit, bucket in zip((2**12, 2**16, 2**20, 2**26, 2**30, 2**32), _SIZE_BUCKETS[1:]) if size < limit),
        _SIZE_BUCKETS[-1],
    )


def _memory_limit() -> str | None:
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        with suppress(OSError, ValueError):
            value = Path(path).read_text().strip()
            if value == "max" or int(value) >= 2**60:
                return "unlimited"
            return _size_bucket(int(value))
    return None


def collect_guest_facts(*, supervisor: bool) -> GuestFacts:
    """Probe only fixed paths, under the calling process's own identity."""
    status: dict[str, str] = {}
    with suppress(OSError, ValueError):
        status = proc_status()
    masks: GuestFacts = {}
    for name in CAP_MASKS:
        value = status.get(name, "")
        masks[name] = value.lower() if re.fullmatch(r"[0-9a-fA-F]{1,16}", value) else None
    kernel = re.match(r"\d{1,5}\.\d{1,5}(?!\d)", os.uname().release)
    shm_exists = SHM.is_dir()
    shm_size = None
    if shm_exists:
        with suppress(OSError):
            stat = SHM.statvfs()
            shm_size = _size_bucket(stat.f_blocks * stat.f_frsize)
    ca_exists = GUEST_CA.is_file()
    ca_size = None
    with suppress(OSError):
        ca_size = _size_bucket(GUEST_CA.stat().st_size)
    ca_readable = False
    if ca_exists:
        with suppress(OSError):
            with GUEST_CA.open("rb") as file:
                file.read(1)
            ca_readable = True
    scratch = Path(GUEST_SCRATCH)
    return {
        **masks, "NoNewPrivs": {"0": False, "1": True}.get(status.get("NoNewPrivs", "")),
        "uid": os.getuid(), "gid": os.getgid(), "groups": len(os.getgroups()),
        "shm_exists": shm_exists, "shm_size": shm_size,
        "ca_exists": ca_exists, "ca_readable": ca_readable, "ca_size": ca_size,
        "scratch_exists": scratch.is_dir(), "scratch_writable": os.access(scratch, os.W_OK | os.X_OK),
        "memory_limit": _memory_limit(), "cpu_count": os.cpu_count(),
        "kernel": kernel[0] if kernel else None,
        "pid_namespace": os.getpid() == 1 if supervisor else os.getppid() == 1,
    }


def valid_guest_facts(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    flags = ("shm_exists", "ca_exists", "ca_readable", "scratch_exists", "scratch_writable", "pid_namespace")
    numbers = ("uid", "gid", "groups", "cpu_count")
    sizes = ("shm_size", "ca_size")
    return (
        set(value) == {*CAP_MASKS, *flags, *numbers, *sizes, "memory_limit", "NoNewPrivs", "kernel"}
        and all(value[key] is None or (
            isinstance(value[key], str) and re.fullmatch(r"[a-f0-9]{1,16}", value[key])
        ) for key in CAP_MASKS)
        and all(type(value[key]) is bool for key in flags)
        and all(type(value[key]) is int and 0 <= value[key] < 2**32 for key in numbers if value[key] is not None)
        and all(value[key] is not None for key in ("uid", "gid", "groups"))
        and all(value[key] is None or value[key] in _SIZE_BUCKETS for key in sizes)
        and (value["memory_limit"] is None or value["memory_limit"] in (*_SIZE_BUCKETS, "unlimited"))
        and (value["NoNewPrivs"] is None or type(value["NoNewPrivs"]) is bool)
        and (value["kernel"] is None or (
            isinstance(value["kernel"], str) and re.fullmatch(r"[0-9]{1,5}\.[0-9]{1,5}", value["kernel"])
        ))
    )
