"""Supervise a single cloudflared process from a read-only token projection."""

import asyncio
import ctypes
import hashlib
import os
import re
import signal
import sys
from contextlib import suppress
from pathlib import Path

CLOUDFLARED = "/usr/local/bin/cloudflared"
TOKEN_PATH = Path("/run/bifrost-tunnel/token")
# Compose replaces token and kubelet replaces the ..data symlink.
DIRECTORY_CHANGES = 0x00000008 | 0x00000080 | 0x00000100 | 0x00000200


def token_fingerprint(path: Path) -> bytes | None:
    try:
        with path.open("rb") as handle:
            token = handle.read(4097)
    except FileNotFoundError:
        return None
    if not token:
        return None
    if len(token) > 4096 or re.fullmatch(rb"[A-Za-z0-9+/]+={0,2}", token) is None:
        raise ValueError("Invalid connector token projection")
    return hashlib.sha256(token).digest()


async def launch(path: Path) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        CLOUDFLARED, "tunnel", "--no-autoupdate", "--metrics", "127.0.0.1:20241",
        "run", "--token-file", str(path),
        env={"HOME": "/tmp", "PATH": "/usr/bin:/bin"},
        stdout=asyncio.subprocess.DEVNULL,
    )


async def stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), 10)
    except TimeoutError:
        with suppress(ProcessLookupError):
            process.kill()
        await process.wait()


async def supervise(path: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "Could not open connector projection watch")
    loop = asyncio.get_running_loop()
    changed = asyncio.Event()
    process = None
    active = None

    def mark_changed() -> None:
        with suppress(BlockingIOError):
            os.read(fd, 65536)
        changed.set()

    try:
        if libc.inotify_add_watch(fd, os.fsencode(path.parent), DIRECTORY_CHANGES) < 0:
            raise OSError(ctypes.get_errno(), "Could not watch connector projection")
        loop.add_reader(fd, mark_changed)
        while True:
            changed.clear()
            try:
                fingerprint = token_fingerprint(path)
            except (ValueError, OSError):
                print("Connector credential projection is unreadable", file=sys.stderr, flush=True)
                fingerprint = None
            if process is not None and fingerprint != active:
                await stop(process)
                process = None
            if process is None and fingerprint is not None:
                process = await launch(path)
                print("Started Cloudflare connector", flush=True)
            active = fingerprint
            if process is None:
                await changed.wait()
                continue
            change_event = asyncio.create_task(changed.wait())
            exited = asyncio.create_task(process.wait())
            try:
                await asyncio.wait((change_event, exited), return_when=asyncio.FIRST_COMPLETED)
            finally:
                change_event.cancel()
                exited.cancel()
                await asyncio.gather(change_event, exited, return_exceptions=True)
            if process.returncode is not None:
                print(f"Cloudflare connector exited ({process.returncode})", file=sys.stderr, flush=True)
                process = None
                await asyncio.sleep(2)
    finally:
        loop.remove_reader(fd)
        os.close(fd)
        if process is not None:
            await stop(process)


async def main() -> None:
    task = asyncio.create_task(supervise(TOKEN_PATH))
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    with suppress(asyncio.CancelledError):
        await task


if __name__ == "__main__":
    asyncio.run(main())
