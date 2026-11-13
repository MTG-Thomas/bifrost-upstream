"""Direct ``bifrost run`` output must not print registered secret values.

The server scrubs those values before it stores a result or an error. The
CLI printer used to emit the raw JSON and ``Error executing workflow: {e}``.
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
QUOTED = 'cli-direct"quoted-token'
ESCAPED = 'direct-run"secret\\line\nvalue'


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
        register_secret(QUOTED)
        value = {
            "token": PLAIN,
            "note": f"saw {PLAIN}",
            "encoded": json.dumps(QUOTED),
            "shown": repr(QUOTED),
            QUOTED: "kept",
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
    assert json.dumps(QUOTED) in leaked
    assert PLAIN not in output
    assert QUOTED not in output
    assert json.dumps(QUOTED) not in output
    assert repr(QUOTED) not in output

    parsed = json.loads(output)
    assert parsed["token"] == "[REDACTED]"
    assert parsed["note"] == "saw [REDACTED]"
    assert parsed["encoded"] == "[REDACTED]"
    assert parsed["shown"] == "[REDACTED]"
    assert parsed["custom"] == "wrapped [REDACTED]"
    assert parsed["count"] == 7
    assert parsed["enabled"] is True
    assert parsed["[REDACTED_KEY_1]"] == "kept"
    assert QUOTED not in parsed
    context = get_execution_context()
    assert PLAIN in context._collect_secret_values()
    assert QUOTED in context._collect_secret_values()

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
    assert QUOTED not in verbose_output
    assert json.dumps(QUOTED) not in verbose_output


def test_run_direct_redacts_secrets_from_error_output(monkeypatch, capsys):
    captured: dict[str, BaseException] = {}

    async def raises_plain():
        register_secret(PLAIN)
        exc = RuntimeError(f"failed with {PLAIN}")
        captured["exc"] = exc
        raise exc

    async def raises_escaped():
        register_secret(ESCAPED)
        exc = KeyError(ESCAPED)
        captured["exc"] = exc
        raise exc

    monkeypatch.setattr(cli, "BifrostClient", _AuthenticatedClient)

    assert cli._run_direct("raises_plain", {"raises_plain": raises_plain}, {}) == 1
    plain_error = capsys.readouterr().err
    assert PLAIN in f"Error executing workflow: {captured['exc']}"
    assert PLAIN not in plain_error
    assert "[REDACTED]" in plain_error
    assert plain_error.startswith("Error executing workflow:")
    assert PLAIN in get_execution_context()._collect_secret_values()

    assert cli._run_direct("raises_escaped", {"raises_escaped": raises_escaped}, {}) == 1
    escaped_error = capsys.readouterr().err
    leaked = f"Error executing workflow: {captured['exc']}"
    assert ESCAPED not in leaked
    assert repr(ESCAPED) in leaked
    assert ESCAPED not in escaped_error
    assert repr(ESCAPED) not in escaped_error
    assert "[REDACTED]" in escaped_error
    assert ESCAPED in get_execution_context()._collect_secret_values()


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
