"""Provider profiles. A new provider is a profile plus a thin client helper."""

from __future__ import annotations

import hmac
import re
from typing import Any

from src.services.workload_auth.constants import (
    AZURE_PROVIDER,
    GITHUB_ISSUER,
    GITHUB_JWKS_URI,
    GITHUB_PROVIDER,
    RESERVED_CONDITION_CLAIMS,
    SUBJECT_TOKEN_ID,
    SUBJECT_TOKEN_JWT,
)
from src.services.workload_auth.errors import RuleValidationError
from src.services.workload_auth.jwks import validate_jwks_url, validate_pinned_jwks
from src.services.workload_auth.types import ClaimCondition

_AZURE_V2 = re.compile(
    r"^https://login\.microsoftonline\.com/"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"/v2\.0$"
)
_AZURE_V1 = re.compile(
    r"^https://sts\.windows\.net/"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"/$"
)

_GITHUB_REQUIRED = ("repository", "sub")
_GITHUB_OPTIONAL = frozenset(
    {
        "ref",
        "job_workflow_ref",
        "environment",
        "repository_owner",
        "actor",
        "sha",
        "workflow_ref",
        "runner_environment",
        "event_name",
    }
)
_AZURE_REQUIRED = ("tid", "oid")
_AZURE_OPTIONAL = frozenset({"appid", "sub", "xms_mirid"})


def _condition_map(conditions: tuple[ClaimCondition, ...]) -> dict[str, str]:
    found: dict[str, str] = {}
    for condition in conditions:
        if condition.claim in found:
            raise RuleValidationError(
                f"Claim {condition.claim} may only be pinned once."
            )
        if condition.claim in RESERVED_CONDITION_CLAIMS:
            raise RuleValidationError(
                f"{condition.claim} is enforced separately and cannot be a condition."
            )
        if not condition.equals or condition.equals.strip() != condition.equals:
            raise RuleValidationError(
                "Claim conditions must be exact non-empty strings."
            )
        found[condition.claim] = condition.equals
    return found


def _require_claims(
    found: dict[str, str], required: tuple[str, ...], optional: frozenset[str]
) -> None:
    missing = [claim for claim in required if claim not in found]
    if missing:
        raise RuleValidationError(
            "This provider requires conditions for: " + ", ".join(missing) + "."
        )
    unknown = sorted(set(found) - set(required) - optional)
    if unknown:
        raise RuleValidationError(
            "Unsupported claim conditions: " + ", ".join(unknown) + "."
        )


def _keys(
    *,
    canonical_jwks: str,
    jwks_uri: str | None,
    pinned_jwks: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if (jwks_uri is None) == (pinned_jwks is None):
        raise RuleValidationError("Set either a JWKS URL or pinned keys, not both.")
    if pinned_jwks is not None:
        return validate_pinned_jwks(pinned_jwks)
    if jwks_uri is None or jwks_uri != canonical_jwks:
        raise RuleValidationError(
            "JWKS URL must be the provider's canonical discovery URL."
        )
    validate_jwks_url(jwks_uri)
    return None


class ProviderProfile:
    def __init__(
        self,
        name: str,
        subject_token_type: str,
        *,
        required: tuple[str, ...],
        optional: frozenset[str],
    ) -> None:
        self.name = name
        self.subject_token_type = subject_token_type
        self.required = required
        self.optional = optional

    def canonical_jwks(self, issuer: str) -> str:
        raise NotImplementedError

    def validate_issuer(self, issuer: str) -> str:
        raise NotImplementedError

    def token_ok(self, claims: dict[str, Any]) -> bool:
        raise NotImplementedError

    def validate_rule(
        self,
        *,
        issuer: str,
        conditions: tuple[ClaimCondition, ...],
        jwks_uri: str | None,
        pinned_jwks: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        self.validate_issuer(issuer)
        found = _condition_map(conditions)
        _require_claims(found, self.required, self.optional)
        self._validate_conditions(issuer, found)
        return _keys(
            canonical_jwks=self.canonical_jwks(issuer),
            jwks_uri=jwks_uri,
            pinned_jwks=pinned_jwks,
        )

    def _validate_conditions(self, issuer: str, found: dict[str, str]) -> None:
        return None


class GitHubActionsProfile(ProviderProfile):
    def __init__(self) -> None:
        super().__init__(
            GITHUB_PROVIDER,
            SUBJECT_TOKEN_ID,
            required=_GITHUB_REQUIRED,
            optional=_GITHUB_OPTIONAL,
        )

    def canonical_jwks(self, issuer: str) -> str:
        return GITHUB_JWKS_URI

    def validate_issuer(self, issuer: str) -> str:
        if issuer != GITHUB_ISSUER:
            raise RuleValidationError(
                "GitHub Actions issuer must be https://token.actions.githubusercontent.com."
            )
        return issuer

    def token_ok(self, claims: dict[str, Any]) -> bool:
        repository = claims.get("repository")
        subject = claims.get("sub")
        if not isinstance(repository, str) or not isinstance(subject, str):
            return False
        return subject.startswith(f"repo:{repository}:")


class AzureManagedIdentityProfile(ProviderProfile):
    def __init__(self) -> None:
        super().__init__(
            AZURE_PROVIDER,
            SUBJECT_TOKEN_JWT,
            required=_AZURE_REQUIRED,
            optional=_AZURE_OPTIONAL,
        )

    def tenant(self, issuer: str) -> str:
        for pattern in (_AZURE_V2, _AZURE_V1):
            match = pattern.fullmatch(issuer)
            if match is not None:
                return match.group(1)
        raise RuleValidationError(
            "Azure issuer must be the v2 login.microsoftonline.com issuer or the v1 sts.windows.net issuer."
        )

    def canonical_jwks(self, issuer: str) -> str:
        tenant = self.tenant(issuer)
        if _AZURE_V2.fullmatch(issuer):
            return f"https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys"
        return f"https://login.microsoftonline.com/{tenant}/discovery/keys"

    def validate_issuer(self, issuer: str) -> str:
        self.tenant(issuer)
        return issuer

    def _validate_conditions(self, issuer: str, found: dict[str, str]) -> None:
        tenant = self.tenant(issuer)
        if len(found["tid"]) != len(tenant) or not hmac.compare_digest(
            found["tid"], tenant
        ):
            raise RuleValidationError(
                "The tid condition must equal the tenant in the issuer."
            )

    def token_ok(self, claims: dict[str, Any]) -> bool:
        try:
            tenant = self.tenant(str(claims.get("iss") or ""))
        except RuleValidationError:
            return False
        tid = claims.get("tid")
        if (
            not isinstance(tid, str)
            or len(tid) != len(tenant)
            or not hmac.compare_digest(tid, tenant)
        ):
            return False
        version = claims.get("ver")
        if version is not None and version not in {"1.0", "2.0"}:
            return False
        return isinstance(claims.get("oid"), str) and bool(claims.get("oid"))


PROFILES: dict[str, ProviderProfile] = {
    GITHUB_PROVIDER: GitHubActionsProfile(),
    AZURE_PROVIDER: AzureManagedIdentityProfile(),
}


def profile_for(name: str) -> ProviderProfile:
    try:
        return PROFILES[name]
    except KeyError as exc:
        raise RuleValidationError("Unknown workload provider.") from exc
