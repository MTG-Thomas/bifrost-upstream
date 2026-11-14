"""Direct ``bifrost run`` output must not print registered secret values.

The unredacted printer text is checked first, then ``_run_direct`` output.
"""

from __future__ import annotations

import json

import pytest

from bifrost import cli
from bifrost._context import (
    clear_execution_context,
    get_execution_context,
    register_secret,
)

PLAIN = "cli-direct-plain-token"


class _Client:
    user = {"id": "user-1", "email": "dev@example.test", "name": "Dev"}
    organization = None


class _AuthenticatedClient:
    @staticmethod
    def get_instance(require_auth: bool = True) -> _Client:
        return _Client()


class _OfflineClient:
    @staticmethod
    def get_instance(require_auth: bool = True) -> _Client:
        raise RuntimeError("not logged in")


@pytest.fixture(autouse=True)
def _reset_execution_context():
    clear_execution_context()
    yield
    clear_execution_context()


class _Wrapped:
    def __str__(self) -> str:
        return f"wrapped {PLAIN}"


def test_run_direct_redacts_secrets_from_success_output(monkeypatch, capsys):
    captured: dict[str, object] = {}

    async def returns_secret():
        register_secret(PLAIN)
        value = {
            "token": PLAIN,
            "note": f"saw {PLAIN}",
            "custom": _Wrapped(),
            "count": 7,
            "enabled": True,
        }
        captured["value"] = value
        return value

    monkeypatch.setattr(cli, "BifrostClient", _AuthenticatedClient)

    assert cli._run_direct("returns_secret", {"returns_secret": returns_secret}, {}) == 0
    output = capsys.readouterr().out
    leaked = json.dumps(captured["value"], default=str)

    assert PLAIN in leaked
    assert PLAIN not in output
    parsed = json.loads(output)
    assert parsed["token"] == "[REDACTED]"
    assert parsed["note"] == "saw [REDACTED]"
    assert parsed["custom"] == "wrapped [REDACTED]"
    assert parsed["count"] == 7
    assert parsed["enabled"] is True
    assert PLAIN in get_execution_context()._collect_secret_values()

    assert (
        cli._run_direct(
            "returns_secret",
            {"returns_secret": returns_secret},
            {},
            verbose=True,
        )
        == 0
    )
    verbose_output = capsys.readouterr().out
    assert "Result:" in verbose_output
    assert PLAIN not in verbose_output


def test_run_direct_redacts_secrets_from_error_output(monkeypatch, capsys):
    captured: dict[str, BaseException] = {}

    async def raises_plain():
        register_secret(PLAIN)
        exc = RuntimeError(f"failed with {PLAIN}")
        captured["exc"] = exc
        raise exc

    monkeypatch.setattr(cli, "BifrostClient", _AuthenticatedClient)

    assert cli._run_direct("raises_plain", {"raises_plain": raises_plain}, {}) == 1
    error = capsys.readouterr().err
    assert PLAIN in f"Error executing workflow: {captured['exc']}"
    assert PLAIN not in error
    assert "[REDACTED]" in error
    assert error.startswith("Error executing workflow:")
    assert PLAIN in get_execution_context()._collect_secret_values()


def test_run_direct_redacts_without_installing_an_execution_context(monkeypatch, capsys):
    """Standalone runs keep working when login is unavailable.

    Secrets registered with no execution context are still scrubbed. This
    does not install a fallback context.
    """

    async def returns_secret():
        register_secret(PLAIN)
        return {"token": PLAIN}

    async def raises_secret():
        register_secret(PLAIN)
        raise RuntimeError(f"failed with {PLAIN}")

    monkeypatch.setattr(cli, "BifrostClient", _OfflineClient)

    assert cli._run_direct("returns_secret", {"returns_secret": returns_secret}, {}) == 0
    output = capsys.readouterr().out
    assert PLAIN not in output
    assert json.loads(output) == {"token": "[REDACTED]"}
    with pytest.raises(RuntimeError, match="No execution context found"):
        get_execution_context()

    assert cli._run_direct("raises_secret", {"raises_secret": raises_secret}, {}) == 1
    error = capsys.readouterr().err
    assert PLAIN not in error
    assert "[REDACTED]" in error
    with pytest.raises(RuntimeError, match="No execution context found"):
        get_execution_context()
