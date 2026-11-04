"""Synthetic WinCred boundary tests. Never invoke a native credential store."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from bifrost._windows_credentials import CredentialStorageError, WindowsKeyringValues
from bifrost.credentials import KEYRING_SERVICE, Credentials, WindowsKeyringBackend


class MemoryWinCred:
    def __init__(self):
        self.data = {}
        self.writes = []
        self.mutations = 0
        self.interrupt_at = None
        self.after = False

    def _interrupt(self, after):
        if self.mutations == self.interrupt_at and self.after == after:
            raise SystemExit("simulated process interruption")

    def get_password(self, service, username):
        return self.data.get((service, username))

    def set_password(self, service, username, value):
        size = len(value.encode("utf-16-le"))
        if size > 2560:
            raise ValueError("synthetic CredWrite 1783")
        self.mutations += 1
        self._interrupt(False)
        self.data[service, username] = value
        self.writes.append(size)
        self._interrupt(True)

    def delete_password(self, service, username):
        self.mutations += 1
        self._interrupt(False)
        del self.data[service, username]
        self._interrupt(True)


def backend(store, lock=None):
    mutex = lock or threading.RLock()
    return WindowsKeyringBackend(_keyring=store, _lock=lambda: mutex)


def profile(url="https://example.test", token="synthetic-access"):
    return Credentials(url, token, "synthetic-refresh", "2099-01-01T00:00:00+00:00")


def assert_no_orphans(store):
    referenced = set()
    values = WindowsKeyringValues(store)
    for (service, username), raw in store.data.items():
        if service == KEYRING_SERVICE:
            manifest = values._manifest(raw)
            if manifest:
                namespace = values._namespace(service, username)
                referenced.update((namespace, f"{manifest[values.MARKER]}:{i}")
                                  for i in range(manifest["parts"]))
    assert all(key[0] == KEYRING_SERVICE or key in referenced for key in store.data)


def test_exact_reported_size_and_legacy_profiles():
    store = MemoryWinCred()
    old = profile("https://old.example.test")
    store.set_password(KEYRING_SERVICE, old.api_url, json.dumps(old.to_dict()))
    store.set_password(KEYRING_SERVICE, "__index__", json.dumps([old.api_url]))
    new = profile()
    extra = (3328 - len(json.dumps(new.to_dict()).encode("utf-16-le"))) // 2
    new = replace(new, access_token=new.access_token + "x" * extra)
    assert len(json.dumps(new.to_dict()).encode("utf-16-le")) == 3328
    with pytest.raises(ValueError, match="1783"):
        store.set_password(KEYRING_SERVICE, new.api_url, json.dumps(new.to_dict()))
    target = backend(store)
    target.save(new)
    assert target.get(new.api_url) == new
    assert target.get(old.api_url) == old
    assert target.list_urls() == sorted([old.api_url, new.api_url])
    assert max(store.writes) <= 2560
    target.clear(new.api_url)
    assert target.get(new.api_url) is None
    assert target.list_urls() == [old.api_url]
    assert_no_orphans(store)


@pytest.mark.parametrize("text", ["a" * 1280, "a" * 1281, "é" * 1280, "😀" * 1280, "汉字" * 1280])
def test_unicode_byte_bounds(text):
    store = MemoryWinCred()
    values = WindowsKeyringValues(store)
    values.set_password("bifrost", "unicode", text)
    assert values.get_password("bifrost", "unicode") == text
    assert max(store.writes) <= 2560


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("existing", [False, True])
def test_interruption_at_every_mutation_preserves_complete_profiles(after, existing):
    seed = MemoryWinCred()
    other = profile("https://other.example.test")
    previous = profile()
    updated = replace(previous, access_token="😀" * 2000, refresh_token="new-refresh")
    target = backend(seed)
    target.save(other)
    if existing:
        target.save(previous)
    baseline = dict(seed.data)
    seed.mutations = 0
    target.save(updated)
    for step in range(1, seed.mutations + 1):
        store = MemoryWinCred()
        store.data = dict(baseline)
        store.interrupt_at = step
        store.after = after
        target = backend(store)
        with pytest.raises(SystemExit):
            target.save(updated)
        store.interrupt_at = None
        # Simulate a new process; journals, not process memory, own recovery.
        target = backend(store)
        assert target.get(other.api_url) == other
        assert target.get(previous.api_url) in (previous if existing else None, updated)
        urls = target.list_urls()
        assert other.api_url in urls
        assert (previous.api_url in urls) == (target.get(previous.api_url) is not None)
        assert_no_orphans(store)


def test_concurrent_backends_preserve_index_and_complete_tuples():
    store = MemoryWinCred()
    lock = threading.RLock()
    profiles = [profile(f"https://instance-{i}.example.test", str(i) * 2000) for i in range(8)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda item: backend(store, lock).save(item), profiles))
    target = backend(store, lock)
    assert target.list_urls() == sorted(item.api_url for item in profiles)
    for item in profiles:
        assert target.get(item.api_url) == item
    assert_no_orphans(store)


def test_readback_failure_is_not_reported_as_success_and_error_has_no_values():
    class BadReadback(MemoryWinCred):
        def get_password(self, service, username):
            value = super().get_password(service, username)
            return "incorrect" if "/wincred-v1/" in service and username != "journal" and value else value

    with pytest.raises(CredentialStorageError, match="part verification") as error:
        backend(BadReadback()).save(profile(token="PRIVATE-SYNTHETIC"))
    assert "PRIVATE-SYNTHETIC" not in str(error.value)


def test_native_error_values_are_suppressed():
    class FailingStore(MemoryWinCred):
        def set_password(self, *args):
            raise RuntimeError("PRIVATE-SYNTHETIC")

    with pytest.raises(CredentialStorageError) as error:
        backend(FailingStore()).save(profile())
    assert "PRIVATE-SYNTHETIC" not in str(error.value)
    assert error.value.__suppress_context__


def test_windows_selection_never_falls_back_to_plaintext(monkeypatch):
    import keyring
    from bifrost import credentials

    monkeypatch.setattr(credentials.platform, "system", lambda: "Windows")
    monkeypatch.setattr(keyring, "get_keyring", lambda: MemoryWinCred())

    def denied(*args):
        raise RuntimeError("PRIVATE-SYNTHETIC")

    monkeypatch.setattr(keyring, "get_password", denied)
    with pytest.raises(CredentialStorageError, match="unavailable"):
        credentials._select_persistent_backend()


def test_invalid_index_is_not_replaced():
    store = MemoryWinCred()
    store.data[KEYRING_SERVICE, "__index__"] = '{"invalid":"synthetic"}'
    before = dict(store.data)
    with pytest.raises(CredentialStorageError, match="index validation"):
        backend(store).save(profile())
    assert store.data == before


def test_oversized_record_refused_before_any_mutation(monkeypatch):
    store = MemoryWinCred()
    monkeypatch.setattr(WindowsKeyringValues, "MAX_PARTS", 1)
    with pytest.raises(CredentialStorageError, match="before writing"):
        backend(store).save(profile(token="x" * 2000))
    assert store.data == {}


def test_missing_and_corrupt_parts_fail_closed():
    for replacement in (None, "YWJj"):
        store = MemoryWinCred()
        target = backend(store)
        target.save(profile(token="x" * 2000))
        namespace = WindowsKeyringValues(store)._namespace(KEYRING_SERVICE, profile().api_url)
        part_key = next(key for key in store.data if key[0] == namespace)
        if replacement is None:
            del store.data[part_key]
        else:
            store.data[part_key] = replacement
        with pytest.raises(CredentialStorageError):
            target.get(profile().api_url)


def test_same_url_concurrency_does_not_mix_token_generations():
    store = MemoryWinCred()
    lock = threading.RLock()
    profiles = [replace(profile(token=str(i) * 2000), refresh_token=f"refresh-{i}") for i in range(8)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda item: backend(store, lock).save(item), profiles))
    assert backend(store, lock).get(profile().api_url) in profiles
    assert_no_orphans(store)


@pytest.mark.parametrize("after", [False, True])
def test_interrupted_logout_never_resurrects_a_deleted_profile(after):
    seed = MemoryWinCred()
    target = backend(seed)
    target.save(profile())
    baseline = dict(seed.data)
    seed.mutations = 0
    target.clear(profile().api_url)
    for step in range(1, seed.mutations + 1):
        store = MemoryWinCred()
        store.data = dict(baseline)
        store.interrupt_at = step
        store.after = after
        with pytest.raises(SystemExit):
            backend(store).clear(profile().api_url)
        store.interrupt_at = None
        target = backend(store)
        assert target.get(profile().api_url) in (None, profile())
        target.clear(profile().api_url)
        assert target.get(profile().api_url) is None
        assert target.list_urls() == []
        assert_no_orphans(store)
