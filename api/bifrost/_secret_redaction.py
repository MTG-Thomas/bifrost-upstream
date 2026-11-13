"""Scrub registered secret substrings from execution output.

This is the single implementation behind ``src.core.secret_string.redact_secrets``.
It lives in the SDK package so the installed CLI can apply the same scrub
without importing server modules.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

REDACTED = "[REDACTED]"
_MIN_SECRET_LENGTH = 4


def redact_secrets(
    obj: Any,
    secret_values: set[str],
    *,
    redact_keys: bool = False,
) -> Any:
    """Deep-walk ``obj`` and replace secret substrings with ``[REDACTED]``.

    Secrets shorter than 4 characters are skipped. Longer secrets are applied
    first so a shorter registered value cannot split a longer one. Dict keys
    are left unchanged unless ``redact_keys`` is set, which the CLI printer
    uses because keys are part of the JSON it writes. A key that contains a
    secret is renamed to ``[REDACTED_KEY_N]`` so it cannot collide with another
    key. The input is not mutated.
    """
    effective = tuple(
        sorted(
            (secret for secret in secret_values if len(secret) >= _MIN_SECRET_LENGTH),
            key=len,
            reverse=True,
        )
    )
    if not effective:
        return obj
    return _redact_recursive(obj, effective, redact_keys=redact_keys)


def _replace_secrets(text: str, secrets: tuple[str, ...]) -> str:
    result = text
    for secret in secrets:
        result = result.replace(secret, REDACTED)
    return result


def _redact_mapping(
    obj: dict[Any, Any],
    secrets: tuple[str, ...],
    *,
    redact_keys: bool,
) -> dict[Any, Any]:
    if not redact_keys:
        return {
            key: _redact_recursive(value, secrets, redact_keys=False)
            for key, value in obj.items()
        }

    redacted: dict[Any, Any] = {}
    original_keys = set(obj)
    for key, value in obj.items():
        child = _redact_recursive(value, secrets, redact_keys=True)
        if not isinstance(key, str):
            redacted[key] = child
            continue
        scrubbed = _replace_secrets(key, secrets)
        if scrubbed == key:
            redacted[key] = child
            continue
        index = 1
        while True:
            candidate = f"[REDACTED_KEY_{index}]"
            if candidate not in original_keys and candidate not in redacted:
                redacted[candidate] = child
                break
            index += 1
    return redacted


def _redact_recursive(
    obj: Any,
    secrets: tuple[str, ...],
    *,
    redact_keys: bool,
) -> Any:
    if isinstance(obj, str):
        return _replace_secrets(obj, secrets)
    if isinstance(obj, dict):
        return _redact_mapping(obj, secrets, redact_keys=redact_keys)
    if isinstance(obj, (list, tuple)):
        redacted = [
            _redact_recursive(item, secrets, redact_keys=redact_keys) for item in obj
        ]
        return redacted if isinstance(obj, list) else tuple(redacted)
    if isinstance(obj, set):
        return {_redact_recursive(item, secrets, redact_keys=redact_keys) for item in obj}
    if isinstance(obj, BaseModel):
        return _redact_recursive(obj.model_dump(), secrets, redact_keys=redact_keys)
    return obj
