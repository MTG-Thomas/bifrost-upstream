"""Allowlisted diagnostics on the existing signed managed Container session."""

import hashlib
import hmac
import json
from typing import Any, Literal, get_args

from src.worker.remote.guest_init import INIT_REASONS, GuestFacts, GuestInitError, valid_guest_facts
from src.worker.remote.protocol import AuthenticationError

GuestStage = Literal[
    "hello", "bootstrap", "runtime-grant", "requirements", "guest-init", "inventory",
    "canary-command", "sdk-tls", "sdk-witness", "egress-probes", "ack", "ready",
]
_STAGES = get_args(GuestStage)
GUEST_DIAGNOSTIC_EVENTS = ("guest_stage", "guest_fatal")
_ERROR_REASONS = {
    "AuthenticationError": "session",
    "ProtocolError": "session",
    "ConnectionError": "transport",
    "ConnectionClosed": "transport",
    "ConnectionClosedError": "transport",
    "ConnectionClosedOK": "transport",
    "ConnectError": "transport",
    "SSLError": "tls",
    "SSLCertVerificationError": "tls",
    "gaierror": "dns",
    "TimeoutError": "timeout",
    "ConnectTimeout": "timeout",
    "ReadTimeout": "timeout",
    "HTTPStatusError": "http-status",
    "FileNotFoundError": "missing-file",
    "PermissionError": "permission",
    "OSError": "os-error",
    "MemoryError": "resource",
    "ValueError": "invalid-input",
    "KeyError": "invalid-input",
    "RuntimeError": "runtime",
    "SystemExit": "process-exit",
    "Exception": "unexpected",
}
_DOMAIN = b"bifrost.containers.guest-diagnostic.v1\0"


def guest_diagnostic_payload(
    stage: GuestStage, error: BaseException | None = None, *, startup_facts: dict[str, GuestFacts] | None = None,
) -> dict[str, Any]:
    facts = {"startup_facts": startup_facts} if startup_facts else {}
    if error is None:
        return {"event": "guest_stage", "stage": stage, **facts}
    if isinstance(error, GuestInitError):
        return {
            "event": "guest_fatal", "stage": stage, "exception_class": "GuestInitError",
            "reason": error.reason, "child_returncode": error.returncode, **facts,
        }
    exception_class = type(error).__name__
    # HTTP clients wrap the TLS/DNS cause. Walk a bounded chain of class
    # names only and prefer those two specific categories when present.
    cause = error.__cause__
    for _ in range(4):
        if cause is None:
            break
        name = type(cause).__name__
        if _ERROR_REASONS.get(name) in ("tls", "dns"):
            exception_class = name
            break
        cause = cause.__cause__
    if exception_class not in _ERROR_REASONS:
        exception_class = "Exception"
    return {
        "event": "guest_fatal", "stage": stage,
        "exception_class": exception_class, "reason": _ERROR_REASONS[exception_class], **facts,
    }


def guest_diagnostic_signature(secret: str, generation: int, sequence: int, payload: dict) -> str:
    body = json.dumps([generation, sequence, payload], sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), _DOMAIN + body, hashlib.sha256).hexdigest()


def verify_guest_diagnostic(secret: str, generation: int, sequence: int, payload: dict) -> dict[str, Any]:
    event, stage, signature = (payload.get(key) for key in ("event", "stage", "signature"))
    body = {key: value for key, value in payload.items() if key != "signature"}
    valid = event in GUEST_DIAGNOSTIC_EVENTS and stage in _STAGES
    fields = set(body)
    if "startup_facts" in fields:
        facts = body["startup_facts"]
        valid &= (
            stage == "guest-init" and isinstance(facts, dict)
            and set(facts) in ({"supervisor"}, {"child", "supervisor"})
            and all(valid_guest_facts(item) for item in facts.values())
        )
        fields.remove("startup_facts")
    if event == "guest_stage":
        valid &= fields == {"event", "stage"}
    elif event == "guest_fatal":
        name = body.get("exception_class")
        expected = {"event", "stage", "exception_class", "reason"}
        if name == "GuestInitError":
            reason = body.get("reason")
            code = body.get("child_returncode")
            valid &= (
                stage == "guest-init" and fields == expected | {"child_returncode"}
                and isinstance(reason, str) and reason in INIT_REASONS
                and (code is None or (type(code) is int and -64 <= code <= 255))
            )
        else:
            valid &= (
                fields == expected
                and isinstance(name, str) and name in _ERROR_REASONS and _ERROR_REASONS[name] == body["reason"]
            )
    if (
        not valid or not isinstance(signature, str) or len(signature) != 64 or not signature.isascii()
        or not hmac.compare_digest(guest_diagnostic_signature(secret, generation, sequence, body), signature)
    ):
        raise AuthenticationError("Invalid managed Container diagnostic")
    return body


def close_evidence(code: int, reason: str | None = None) -> dict[str, str | int]:
    # The close reason is peer-controlled text. Derive its category from the
    # standardized code without retaining any part of the text itself.
    if not 1000 <= code < 5000:
        code = 1006
    categories = {
        1000: "normal", 1001: "going-away", 1002: "protocol", 1003: "unsupported-data",
        1005: "no-status", 1006: "transport-lost", 1007: "invalid-payload", 1008: "policy",
        1009: "size-limit", 1010: "extension", 1011: "internal", 1012: "restart",
        1013: "unavailable", 1014: "gateway",
    }
    category = categories.get(code, "other")
    if code == 1011:
        if reason == "Container relay transport lost":
            category = "transport-lost"
        elif isinstance(reason, str) and reason.startswith("Container relay close fallback "):
            original = reason.removeprefix("Container relay close fallback ")
            if len(original) == 4 and original.isascii() and original.isdecimal():
                category = categories.get(int(original), "other")
    return {"close_code": code, "close_category": category}
