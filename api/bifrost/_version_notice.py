"""Dedupe helper for the soft 'update available' notice (Gate 2).

When the CLI is contract-compatible but on a different build version than the
server, we show a one-line nudge, but only once per (API URL, server version),
so it doesn't fire on every command.

The marker lives in a per-user directory under the OS temp dir
(``bifrost-<uid>/version-notices`` on POSIX; ``bifrost/version-notices`` on
Windows, where TEMP is already user-scoped), keyed by a STABLE digest of the
URL. We deliberately avoid Python's built-in ``hash()`` here: it is
per-process randomized (``PYTHONHASHSEED``), so two CLI invocations would key
to different files and dedupe would never work. That directory is opened
without following a symlink, and the marker is replaced from a private sibling
file so a pre-existing symlink is replaced rather than followed.

All I/O is best-effort: a permission/collision/stale-marker problem can only
cause a missed-or-extra notice, never a wrong block. Callers must keep this
isolated from the hard-gate decision.
"""

from __future__ import annotations

import os
import stat
import tempfile
import zlib
from pathlib import Path


def _marker_directory() -> Path:
    """Return the per-user directory that holds version-notice markers.

    The shared temp root is writable by every local user, so a predictable
    filename there can be planted as a symlink before the CLI runs. POSIX
    keeps the marker under ``bifrost-<uid>``, the same private temp namespace
    workspace locks use. Windows TEMP is already scoped to the current user.
    """
    temp_root = Path(tempfile.gettempdir())
    if hasattr(os, "getuid"):
        return temp_root / f"bifrost-{os.getuid()}" / "version-notices"
    return temp_root / "bifrost" / "version-notices"


def _marker_path(api_url: str) -> Path:
    # Derive a stable, filesystem-safe per-URL key for the marker filename. We use
    # a non-cryptographic checksum (CRC32) deliberately: this is a dedupe key, not
    # security, and crypto hashes here trip CodeQL's sensitive-data-hashing taint
    # rule (the URL is tainted via credentials._resolve_url). CRC32 is fixed-length,
    # collision-resistant enough to distinguish API URLs, and unambiguously not a
    # security primitive.
    key = format(zlib.crc32(api_url.rstrip("/").encode()) & 0xFFFFFFFF, "08x")
    return _marker_directory() / f"bifrost-vnotice-{key}"


def _open_owned_directory(name: str, full_path: Path, *, dir_fd: int | None = None) -> int:
    """Open a directory without following a symlink, and require current ownership."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    if dir_fd is None:
        descriptor = os.open(name, flags)
    else:
        descriptor = os.open(name, flags, dir_fd=dir_fd)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode):
            raise NotADirectoryError(
                f"version notice directory is not a directory: {full_path}"
            )
        if info.st_uid != os.getuid():
            raise PermissionError(
                f"version notice directory is not owned by the current user: {full_path}"
            )
        os.fchmod(descriptor, 0o700)
        return descriptor
    except OSError:
        os.close(descriptor)
        raise


def _ensure_posix_directory(directory: Path) -> None:
    """Create ``directory`` so a planted per-user temp symlink is not followed.

    Another local user can create ``bifrost-<uid>`` in the shared temp root
    before this process does. Opening each level with ``O_NOFOLLOW`` from a
    directory descriptor rejects that swap before the marker is written.
    """
    user_directory = directory.parent
    try:
        user_directory.mkdir(mode=0o700)
    except FileExistsError:
        # The no-follow open below validates the existing entry before use.
        pass
    user_fd = _open_owned_directory(str(user_directory), user_directory)
    child_fd: int | None = None
    try:
        try:
            os.mkdir(directory.name, mode=0o700, dir_fd=user_fd)
        except FileExistsError:
            # The no-follow open below validates the existing entry before use.
            pass
        child_fd = _open_owned_directory(
            directory.name, directory, dir_fd=user_fd
        )
    finally:
        if child_fd is not None:
            os.close(child_fd)
        os.close(user_fd)


def _ensure_marker_directory(directory: Path) -> None:
    if os.name == "posix":
        _ensure_posix_directory(directory)
        return
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink() or directory.parent.is_symlink():
        raise OSError(f"version notice directory must not be a symlink: {directory}")
    directory.chmod(0o700)


def should_notify(api_url: str, server_version: str) -> bool:
    """True if we haven't already shown a notice for this (url, version)."""
    try:
        marker = _marker_path(api_url)
        if marker.exists() and marker.read_text(encoding="utf-8").strip() == server_version:
            return False
    except OSError:
        # Can't read the marker — fall through and notify (extra notice at worst).
        return True
    return True


def mark_notified(api_url: str, server_version: str) -> None:
    """Record that the notice for (url, version) has been shown."""
    temporary_marker: Path | None = None
    try:
        marker = _marker_path(api_url)
        _ensure_marker_directory(marker.parent)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=marker.parent,
            prefix=f".{marker.name}.",
            delete=False,
        ) as handle:
            temporary_marker = Path(handle.name)
            handle.write(server_version)
        # replace() renames onto the marker name. A symlink at that name is
        # unlinked, not followed, so the symlink target is left untouched.
        temporary_marker.replace(marker)
    except OSError:
        pass  # best-effort: failing to record just means the notice may re-show
    finally:
        if temporary_marker is not None:
            try:
                temporary_marker.unlink(missing_ok=True)
            except OSError:
                # Cleanup is best-effort after the notice write failed.
                # Leaving the private temp file is safer than raising here.
                pass
