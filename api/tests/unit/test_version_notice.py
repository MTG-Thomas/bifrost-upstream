"""Unit tests for the CLI build-drift notice marker."""

from __future__ import annotations

import stat
import sys
from pathlib import Path

import pytest
from bifrost import _version_notice


def _isolate_temp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(_version_notice.tempfile, "gettempdir", lambda: str(temp_root))
    return temp_root


def test_version_notice_is_deduped_by_url_and_server_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_root = _isolate_temp(tmp_path, monkeypatch)
    api_url = "https://dev.bifrost.example.com/"

    assert _version_notice.should_notify(api_url, "2026.7.5") is True

    _version_notice.mark_notified(api_url, "2026.7.5")

    assert (
        _version_notice.should_notify("https://dev.bifrost.example.com", "2026.7.5")
        is False
    )
    assert _version_notice.should_notify(api_url, "2026.7.6") is True
    markers = list(temp_root.rglob("bifrost-vnotice-*"))
    assert len(markers) == 1
    # The shared temp root is writable by other local users. The marker must
    # live in the per-user child, not as a predictable direct entry.
    assert markers[0].parent != temp_root
    assert markers[0].parent.name == "version-notices"
    if sys.platform != "win32":
        assert stat.S_IMODE(markers[0].parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(markers[0].stat().st_mode) == 0o600


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX symlink semantics")
def test_version_notice_replaces_symlink_without_clobbering_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _isolate_temp(tmp_path, monkeypatch)
    api_url = "https://dev.bifrost.example.com"
    target = tmp_path / "victim"
    target.write_text("preserve me", encoding="utf-8")
    marker = _version_notice._marker_path(api_url)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.symlink_to(target)

    _version_notice.mark_notified(api_url, "2026.7.5")

    assert target.read_text(encoding="utf-8") == "preserve me"
    assert marker.is_symlink() is False
    assert marker.read_text(encoding="utf-8") == "2026.7.5"
    assert _version_notice.should_notify(api_url, "2026.7.5") is False


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX symlink semantics")
def test_version_notice_does_not_follow_user_directory_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-created per-user temp directory must not redirect the marker."""
    temp_root = _isolate_temp(tmp_path, monkeypatch)
    redirected = tmp_path / "redirected"
    redirected.mkdir()
    user_directory = _version_notice._marker_directory().parent
    user_directory.symlink_to(redirected, target_is_directory=True)

    _version_notice.mark_notified("https://dev.bifrost.example.com", "2026.7.5")

    assert list(redirected.iterdir()) == []
    assert list(temp_root.glob("bifrost-vnotice-*")) == []


def test_version_notice_read_and_write_failures_are_best_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenMarker:
        @property
        def parent(self) -> BrokenMarker:
            return self

        def mkdir(self, *args: object, **kwargs: object) -> None:
            raise OSError("cannot create marker directory")

        def exists(self) -> bool:
            return True

        def read_text(self, *args: object, **kwargs: object) -> str:
            raise OSError("cannot read marker")

        def write_text(self, *args: object, **kwargs: object) -> None:
            raise OSError("cannot write marker")

    monkeypatch.setattr(_version_notice, "_marker_path", lambda api_url: BrokenMarker())

    assert _version_notice.should_notify("https://dev.bifrost.example.com", "2026.7.5")
    _version_notice.mark_notified("https://dev.bifrost.example.com", "2026.7.5")
