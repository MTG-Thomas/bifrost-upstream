"""Bounded, recoverable values over python-keyring's Windows credential store.

Only the small root pointer is replaced at commit. A journal lets the next
operation finish cleanup after interruption, without deleting the active value.
All calls, including readers and index updates, hold the same Windows mutex.
"""

import base64
import ctypes
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


class CredentialStorageError(RuntimeError):
    """A deliberately value-free persistence error suitable for CLI output."""


@contextmanager
def windows_credential_lock():
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
    kernel.CreateMutexW.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.ReleaseMutex.argtypes = [wintypes.HANDLE]
    kernel.ReleaseMutex.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    identity = hashlib.sha256(str(Path.home()).casefold().encode()).hexdigest()
    handle = kernel.CreateMutexW(None, False, "Global\\BifrostCredentials-" + identity)
    if not handle:
        raise CredentialStorageError("Windows credential storage failed at lock creation")
    try:
        # WAIT_ABANDONED grants ownership too. Journals recover interrupted writes.
        result = kernel.WaitForSingleObject(handle, 30000)
        if result not in (0, 0x80):
            raise CredentialStorageError("Windows credential storage failed at lock acquisition")
        try:
            yield
        finally:
            if not kernel.ReleaseMutex(handle):
                raise CredentialStorageError("Windows credential storage failed at lock release")
    finally:
        kernel.CloseHandle(handle)


class WindowsKeyringValues:
    """Keyring interface; callers must serialize each complete backend operation.

    Base64 makes each part ASCII: 1280 characters occupy exactly the documented
    2560-byte WinCred blob maximum, regardless of the input's Unicode characters.
    Legacy raw values remain readable until their first successful replacement.
    """

    MARKER = "bifrost_wincred_v1"
    PART_CHARS = 1280
    MAX_PARTS = 4096

    def __init__(self, keyring):
        self._kr = keyring

    def _call(self, stage, method, *args):
        try:
            return getattr(self._kr, method)(*args)
        except Exception:
            raise CredentialStorageError(f"Windows credential storage failed at {stage}") from None

    def _namespace(self, service, username):
        digest = hashlib.sha256(username.encode("utf-8")).hexdigest()
        return f"{service}/wincred-v1/{digest}"

    def _fingerprint(self, raw):
        return hashlib.sha256(json.dumps(raw).encode("utf-8")).hexdigest()

    def _manifest(self, raw):
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if not isinstance(data, dict) or self.MARKER not in data:
            return None
        generation = data.get(self.MARKER)
        count = data.get("parts")
        digest = data.get("sha256")
        if (not isinstance(generation, str) or len(generation) != 32
                or any(c not in "0123456789abcdef" for c in generation)
                or type(count) is not int or not 1 <= count <= self.MAX_PARTS
                or not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)):
            raise CredentialStorageError("Windows credential storage failed at manifest validation")
        return data

    def _remove_parts(self, namespace, manifest):
        if manifest is None:
            return
        for index in range(manifest["parts"]):
            key = f"{manifest[self.MARKER]}:{index}"
            if self._call("cleanup read", "get_password", namespace, key) is not None:
                self._call("cleanup delete", "delete_password", namespace, key)

    def _recover(self, service, username):
        namespace = self._namespace(service, username)
        raw = self._call("journal read", "get_password", namespace, "journal")
        if raw is None:
            return
        try:
            journal = json.loads(raw)
            pending = self._manifest(json.dumps(journal["pending"]))
            previous = self._manifest(json.dumps(journal["previous"]))
            if pending is None:
                raise ValueError
        except (ValueError, TypeError, KeyError):
            raise CredentialStorageError("Windows credential storage failed at journal validation") from None
        root = self._call("recovery read", "get_password", service, username)
        committed = self._manifest(root) == pending
        if not committed and self._fingerprint(root) != journal.get("previous_fingerprint"):
            # An unexpected/misread root is neither a commit nor a rollback.
            # Keep both generations and the journal intact for safe diagnosis.
            raise CredentialStorageError("Windows credential storage failed at recovery state validation")
        self._remove_parts(namespace, previous if committed else pending)
        self._call("journal cleanup", "delete_password", namespace, "journal")

    def get_password(self, service, username):
        self._recover(service, username)
        raw = self._call("profile read", "get_password", service, username)
        manifest = self._manifest(raw)
        if manifest is None:
            return raw
        namespace = self._namespace(service, username)
        parts = []
        for index in range(manifest["parts"]):
            part = self._call("part read", "get_password", namespace,
                              f"{manifest[self.MARKER]}:{index}")
            if not isinstance(part, str) or len(part) > self.PART_CHARS or not part.isascii():
                raise CredentialStorageError("Windows credential storage failed at incomplete readback")
            parts.append(part)
        encoded = "".join(parts)
        if hashlib.sha256(encoded.encode("ascii")).hexdigest() != manifest["sha256"]:
            raise CredentialStorageError("Windows credential storage failed at integrity verification")
        try:
            return base64.b64decode(encoded, validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            raise CredentialStorageError("Windows credential storage failed at decoding") from None

    def prepare(self, value):
        # Validate every bound before touching even the journal.
        try:
            encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
        except UnicodeError:
            raise CredentialStorageError("Windows credential storage refused invalid Unicode before writing") from None
        parts = [encoded[i:i + self.PART_CHARS] for i in range(0, len(encoded), self.PART_CHARS)] or [""]
        if len(parts) > self.MAX_PARTS:
            raise CredentialStorageError("Windows credential storage refused an oversized record before writing")
        return encoded, parts

    def set_password(self, service, username, value):
        encoded, parts = self.prepare(value)
        self._recover(service, username)
        previous_raw = self._call("previous read", "get_password", service, username)
        pending = {self.MARKER: uuid4().hex, "parts": len(parts),
                   "sha256": hashlib.sha256(encoded.encode("ascii")).hexdigest()}
        journal = json.dumps({"pending": pending, "previous": self._manifest(previous_raw),
                              "previous_fingerprint": self._fingerprint(previous_raw)})
        namespace = self._namespace(service, username)
        self._call("journal write", "set_password", namespace, "journal", journal)
        if self._call("journal verification", "get_password", namespace, "journal") != journal:
            raise CredentialStorageError("Windows credential storage failed at journal verification")
        for index, part in enumerate(parts):
            key = f"{pending[self.MARKER]}:{index}"
            self._call("part write", "set_password", namespace, key, part)
            if self._call("part verification", "get_password", namespace, key) != part:
                raise CredentialStorageError("Windows credential storage failed at part verification")
        root = json.dumps(pending)
        self._call("commit", "set_password", service, username, root)
        if self._call("commit verification", "get_password", service, username) != root:
            raise CredentialStorageError("Windows credential storage failed at commit verification")
        self._recover(service, username)

    def delete_password(self, service, username):
        # An empty committed value is an atomic tombstone. It also makes cleanup
        # recoverable; removing the root first would lose its part references.
        self.set_password(service, username, "")
