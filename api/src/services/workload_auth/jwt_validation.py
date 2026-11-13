"""Verify a subject JWT with PyJWT. Fail closed, and do not log the token."""

from __future__ import annotations

from typing import Any

import jwt
from jwt import PyJWK

from src.services.workload_auth.constants import ALLOWED_ALGS, CLOCK_SKEW_SECONDS

REASON_EXPIRED = "expired"
REASON_WRONG_AUDIENCE = "wrong_audience"
REASON_WRONG_ISSUER = "wrong_issuer"
REASON_INVALID = "invalid_subject_token"


class SubjectTokenRejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def verify_subject_jwt(
    token: str,
    *,
    keys: dict[str, Any],
    issuer: str,
    audiences: tuple[str, ...],
) -> dict[str, Any]:
    """Return verified claims, or raise ``SubjectTokenRejected``.

    A key that verifies the signature decides expiry, issuer, and audience.
    Other keys are not consulted after a verified-but-rejected token.
    """
    if not keys.get("keys"):
        raise SubjectTokenRejected(REASON_INVALID)
    try:
        header = jwt.get_unverified_header(token)
    except jwt.PyJWTError as exc:
        raise SubjectTokenRejected(REASON_INVALID) from exc
    alg = header.get("alg")
    if not isinstance(alg, str) or alg not in ALLOWED_ALGS:
        raise SubjectTokenRejected(REASON_INVALID)
    kid = header.get("kid")
    candidates = [
        key
        for key in keys["keys"]
        if isinstance(key, dict)
        and "d" not in key
        and "k" not in key
        and (kid is None or key.get("kid") == kid)
        and (key.get("alg") in (None, alg))
    ]
    if not candidates:
        raise SubjectTokenRejected(REASON_INVALID)
    for jwk in candidates:
        try:
            public_key = PyJWK.from_dict(jwk).key
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            continue
        try:
            claims = jwt.decode(
                token,
                public_key,
                algorithms=[alg],
                audience=list(audiences),
                issuer=issuer,
                leeway=CLOCK_SKEW_SECONDS,
                options={
                    "require": ["exp", "iat", "iss", "aud", "sub"],
                    "verify_signature": True,
                    "verify_aud": True,
                    "verify_iss": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_iat": True,
                },
            )
        except jwt.ExpiredSignatureError as exc:
            raise SubjectTokenRejected(REASON_EXPIRED) from exc
        except jwt.InvalidAudienceError as exc:
            raise SubjectTokenRejected(REASON_WRONG_AUDIENCE) from exc
        except jwt.InvalidIssuerError as exc:
            raise SubjectTokenRejected(REASON_WRONG_ISSUER) from exc
        except jwt.PyJWTError:
            continue
        if not isinstance(claims, dict):
            raise SubjectTokenRejected(REASON_INVALID)
        return claims
    raise SubjectTokenRejected(REASON_INVALID)


def unverified_issuer_and_subject(token: str) -> tuple[str | None, str | None]:
    """Read ``iss`` and ``sub`` without trusting the signature, for audit only."""
    try:
        claims = jwt.decode(
            token,
            key="",
            options={
                "verify_signature": False,
                "verify_aud": False,
                "verify_exp": False,
                "verify_iat": False,
                "verify_nbf": False,
                "verify_iss": False,
            },
        )
    except jwt.PyJWTError:
        return None, None
    issuer = claims.get("iss") if isinstance(claims, dict) else None
    subject = claims.get("sub") if isinstance(claims, dict) else None
    if not isinstance(issuer, str):
        issuer = None
    if not isinstance(subject, str):
        subject = None
    return (
        None if issuer is None else issuer[:512],
        None if subject is None else subject[:256],
    )
