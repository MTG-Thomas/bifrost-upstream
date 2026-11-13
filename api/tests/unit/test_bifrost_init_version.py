"""``import bifrost`` resolves its version without spawning git."""

from __future__ import annotations

import ast
import importlib.metadata
from pathlib import Path

import pytest

import bifrost

_INIT = Path(bifrost.__file__)


def test_compute_version_prefers_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_VERSION", "2026.7.5")
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *_args, **_kwargs: pytest.fail("subprocess invoked"),
    )
    monkeypatch.setattr(
        "importlib.metadata.version",
        lambda _name: pytest.fail("metadata lookup"),
    )

    assert bifrost._compute_version() == "2026.7.5"


def test_compute_version_uses_installed_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BIFROST_VERSION", raising=False)
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *_args, **_kwargs: "v9.9.9-dirty\n",
    )
    seen: list[str] = []

    def version(name: str) -> str:
        seen.append(name)
        return "1.2.3"

    monkeypatch.setattr("importlib.metadata.version", version)

    assert bifrost._compute_version() == "1.2.3"
    assert seen == ["bifrost"]


def test_compute_version_falls_back_when_distribution_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BIFROST_VERSION", raising=False)
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *_args, **_kwargs: "v9.9.9-dirty\n",
    )

    def missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr("importlib.metadata.version", missing)

    assert bifrost._compute_version() == "unknown"


def test_package_init_does_not_import_subprocess() -> None:
    tree = ast.parse(_INIT.read_text(encoding="utf-8"), filename=str(_INIT))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(
                alias.name for alias in node.names if alias.name == "subprocess"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "subprocess":
            imported.append("subprocess")
    assert imported == []
