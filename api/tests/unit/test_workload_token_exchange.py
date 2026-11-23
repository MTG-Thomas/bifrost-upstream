"""Provider-neutral workload token exchange, with locally signed subject tokens."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest
from bifrost import workload_login
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from src.services.authorization import runtime_auth, tokens
from src.services.authorization.tokens import (
    WORKLOAD_CREDENTIAL_KIND,
    authenticate_workload_token,
    parse_human_claims,
    parse_workload_claims,
)
from src.services.workload_auth.constants import (
    AZURE_PROVIDER,
    GITHUB_ISSUER,
    GITHUB_JWKS_URI,
    GITHUB_PROVIDER,
    SUBJECT_TOKEN_ID,
    SUBJECT_TOKEN_JWT,
    TOKEN_EXCHANGE_GRANT,
    WORKLOAD_SCOPE,
)
from src.services.workload_auth.constants import (
    WORKLOAD_CREDENTIAL_KIND as CONSTANT_KIND,
)
from src.services.workload_auth.errors import (
    JwksFetchError,
    RuleValidationError,
    WorkloadAuthError,
)
from src.services.workload_auth.exchange import reject_request_shape, select_rule
from src.services.workload_auth.jwks import (
    clear_jwks_cache,
    fetch_jwks,
    validate_jwks_url,
    validate_pinned_jwks,
)
from src.services.workload_auth.jwt_validation import (
    SubjectTokenRejected,
    verify_subject_jwt,
)
from src.services.workload_auth.profiles import profile_for
from src.services.workload_auth.types import ClaimCondition, TrustRule

_TENANT = "11111111-1111-1111-1111-111111111111"
_AZURE_ISSUER = f"https://sts.windows.net/{_TENANT}/"
_AZURE_JWKS = f"https://login.microsoftonline.com/{_TENANT}/discovery/keys"
_NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    clear_jwks_cache()


def _private_and_jwks() -> tuple[object, dict]:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(private.public_key()))
    jwk.update({"kid": "test", "alg": "RS256", "use": "sig"})
    return private, {"keys": [jwk]}


def _sign(private: object, claims: dict) -> str:
    return jwt.encode(claims, private, algorithm="RS256", headers={"kid": "test"})  # type: ignore[arg-type]


def _github_claims(**overrides: object) -> dict:
    claims: dict[str, object] = {
        "iss": GITHUB_ISSUER,
        "aud": "https://bifrost.example",
        "sub": "repo:acme/widgets:ref:refs/heads/main",
        "repository": "acme/widgets",
        "ref": "refs/heads/main",
        "iat": int(_NOW.timestamp()),
        "exp": int((_NOW + timedelta(minutes=5)).timestamp()),
    }
    claims.update(overrides)
    return claims


def _azure_claims(**overrides: object) -> dict:
    claims: dict[str, object] = {
        "iss": _AZURE_ISSUER,
        "aud": "api://bifrost-workload",
        "sub": "22222222-2222-2222-2222-222222222222",
        "tid": _TENANT,
        "oid": "22222222-2222-2222-2222-222222222222",
        "appid": "33333333-3333-3333-3333-333333333333",
        "ver": "1.0",
        "iat": int(_NOW.timestamp()),
        "exp": int((_NOW + timedelta(minutes=10)).timestamp()),
    }
    claims.update(overrides)
    return claims


def _rule(
    provider: str,
    issuer: str,
    token_type: str,
    jwks: dict,
    conditions: tuple[ClaimCondition, ...],
    **overrides: object,
) -> TrustRule:
    audiences = (
        ("https://bifrost.example",) if provider == GITHUB_PROVIDER else ("api://bifrost-workload",)
    )
    values: dict[str, object] = {
        "id": uuid4(),
        "name": "ci",
        "provider": provider,
        "enabled": True,
        "issuer": issuer,
        "subject_token_type": token_type,
        "audiences": audiences,
        "jwks_uri": None,
        "pinned_jwks": jwks,
        "conditions": conditions,
        "identity_id": uuid4(),
        "token_lifetime_seconds": 900,
    }
    values.update(overrides)
    return TrustRule(**values)  # type: ignore[arg-type]


def _github_rule(jwks: dict, **overrides: object) -> TrustRule:
    return _rule(
        GITHUB_PROVIDER,
        GITHUB_ISSUER,
        SUBJECT_TOKEN_ID,
        jwks,
        (
            ClaimCondition("repository", "acme/widgets"),
            ClaimCondition("sub", "repo:acme/widgets:ref:refs/heads/main"),
        ),
        **overrides,
    )


def _azure_rule(jwks: dict, **overrides: object) -> TrustRule:
    return _rule(
        AZURE_PROVIDER,
        _AZURE_ISSUER,
        SUBJECT_TOKEN_JWT,
        jwks,
        (
            ClaimCondition("tid", _TENANT),
            ClaimCondition("oid", "22222222-2222-2222-2222-222222222222"),
        ),
        **overrides,
    )


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return _NOW if tz is None else _NOW.astimezone(tz)


@pytest.fixture
def frozen_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jwt.api_jwt, "datetime", _FrozenDateTime)


def test_credential_kind_constant_matches_the_minter() -> None:
    assert CONSTANT_KIND == WORKLOAD_CREDENTIAL_KIND == "workload"
    assert workload_login.TOKEN_EXCHANGE_GRANT == TOKEN_EXCHANGE_GRANT
    assert workload_login.SUBJECT_TOKEN_ID == SUBJECT_TOKEN_ID
    assert workload_login.SUBJECT_TOKEN_JWT == SUBJECT_TOKEN_JWT
    assert workload_login.WORKLOAD_SCOPE == WORKLOAD_SCOPE


def test_workload_payload_is_not_a_human_or_runtime_credential() -> None:
    user_id = uuid4()
    payload = {
        "iss": "bifrost",
        "aud": "bifrost",
        "type": "access",
        "ver": 1,
        "iat": 1,
        "exp": 2,
        "jti": "jti",
        "av": "1",
        "sub": str(user_id),
        "credential_kind": "workload",
        "usr": {
            "id": str(user_id),
            "email": "nightly@identities.bifrost.internal",
            "kind": "custom",
            "org": str(uuid4()),
            "ext": False,
            "prov": False,
            "active": True,
            "base": {"id": str(uuid4()), "perm": ["workflows.read"]},
            "roles": [],
        },
    }
    claims = parse_workload_claims(payload)
    assert claims is not None
    assert claims.usr.kind == "custom"
    assert parse_human_claims(payload) is None
    assert runtime_auth.is_runtime_credential_payload(payload) is False
    payload["usr"]["kind"] = None
    assert parse_workload_claims(payload) is None


@pytest.mark.asyncio
async def test_workload_epoch_mismatch_does_not_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = uuid4()
    payload = {
        "iss": "bifrost",
        "aud": "bifrost",
        "type": "access",
        "ver": 1,
        "iat": 1,
        "exp": 2,
        "jti": "jti",
        "av": "7",
        "sub": str(user_id),
        "credential_kind": "workload",
        "usr": {
            "id": str(user_id),
            "email": "nightly@identities.bifrost.internal",
            "kind": "custom",
            "org": None,
            "ext": False,
            "prov": False,
            "active": True,
            "base": {"id": str(uuid4()), "perm": []},
            "roles": [],
        },
    }
    claims = parse_workload_claims(payload)
    assert claims is not None

    async def current(db: object) -> int:
        return 7

    monkeypatch.setattr(tokens, "read_epoch", current)
    principal = await authenticate_workload_token(object(), claims)  # type: ignore[arg-type]
    assert principal is not None
    assert principal.user_id == user_id

    async def moved(db: object) -> int:
        return 8

    monkeypatch.setattr(tokens, "read_epoch", moved)
    assert await authenticate_workload_token(object(), claims) is None  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_github_and_azure_exchange_and_replay(frozen_time: None) -> None:
    private, jwks = _private_and_jwks()
    github = _sign(private, _github_claims())
    azure = _sign(private, _azure_claims())
    github_rule = _github_rule(jwks)
    azure_rule = _azure_rule(jwks)
    selected = await select_rule([github_rule, azure_rule], github, SUBJECT_TOKEN_ID)
    assert isinstance(selected, tuple)
    assert selected[0].id == github_rule.id
    again = await select_rule([github_rule], github, SUBJECT_TOKEN_ID)
    assert isinstance(again, tuple)
    azure_selected = await select_rule(
        [github_rule, azure_rule], azure, SUBJECT_TOKEN_JWT
    )
    assert isinstance(azure_selected, tuple)
    assert azure_selected[0].id == azure_rule.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda claims: claims.update(iss="https://evil.example"), "wrong_issuer"),
        (lambda claims: claims.update(aud="https://other.example"), "wrong_audience"),
        (
            lambda claims: claims.update(
                iat=int((_NOW - timedelta(minutes=10)).timestamp()),
                exp=int((_NOW - timedelta(minutes=5)).timestamp()),
            ),
            "expired",
        ),
        (lambda claims: claims.update(repository="acme/other"), "claim_mismatch"),
        (lambda claims: claims.pop("repository"), "claim_mismatch"),
    ],
)
async def test_github_token_failures(frozen_time: None, mutate, reason: str) -> None:
    private, jwks = _private_and_jwks()
    claims = _github_claims()
    mutate(claims)
    failure = await select_rule(
        [_github_rule(jwks)], _sign(private, claims), SUBJECT_TOKEN_ID
    )
    assert not isinstance(failure, tuple)
    assert failure.reason == reason


@pytest.mark.asyncio
async def test_expired_within_skew_is_accepted(frozen_time: None) -> None:
    private, jwks = _private_and_jwks()
    issued_at = int((_NOW - timedelta(seconds=30)).timestamp())
    token = _sign(private, _github_claims(iat=issued_at, exp=issued_at))
    selected = await select_rule([_github_rule(jwks)], token, SUBJECT_TOKEN_ID)
    assert isinstance(selected, tuple)


@pytest.mark.asyncio
async def test_disabled_rule_and_ambiguous_rules_fail_closed(frozen_time: None) -> None:
    private, jwks = _private_and_jwks()
    token = _sign(private, _github_claims())
    disabled = await select_rule(
        [_github_rule(jwks, enabled=False)], token, SUBJECT_TOKEN_ID
    )
    assert not isinstance(disabled, tuple)
    assert disabled.reason == "invalid_subject_token"
    ambiguous = await select_rule(
        [_github_rule(jwks), _github_rule(jwks, name="other", identity_id=uuid4())],
        token,
        SUBJECT_TOKEN_ID,
    )
    assert not isinstance(ambiguous, tuple)
    assert ambiguous.reason == "ambiguous_rule"


@pytest.mark.asyncio
async def test_environment_condition_rejects_a_token_without_the_claim(
    frozen_time: None,
) -> None:
    private, jwks = _private_and_jwks()
    rule = _rule(
        GITHUB_PROVIDER,
        GITHUB_ISSUER,
        SUBJECT_TOKEN_ID,
        jwks,
        (
            ClaimCondition("repository", "acme/widgets"),
            ClaimCondition("sub", "repo:acme/widgets:environment:production"),
            ClaimCondition("environment", "production"),
        ),
    )
    token = _sign(
        private, _github_claims(sub="repo:acme/widgets:environment:production")
    )
    failure = await select_rule([rule], token, SUBJECT_TOKEN_ID)
    assert not isinstance(failure, tuple)
    assert failure.reason == "claim_mismatch"


@pytest.mark.asyncio
async def test_azure_rejects_tid_that_disagrees_with_the_issuer(
    frozen_time: None,
) -> None:
    private, jwks = _private_and_jwks()
    token = _sign(private, _azure_claims(tid="99999999-9999-9999-9999-999999999999"))
    failure = await select_rule([_azure_rule(jwks)], token, SUBJECT_TOKEN_JWT)
    assert not isinstance(failure, tuple)
    assert failure.reason == "claim_mismatch"


def test_unknown_subject_token_type_is_invalid_request() -> None:
    with pytest.raises(WorkloadAuthError) as caught:
        reject_request_shape(
            subject_token="a.b.c",
            subject_token_type="urn:ietf:params:oauth:token-type:saml2",
            requested_token_type=None,
            scope=None,
            actor_token=None,
        )
    assert caught.value.error == "invalid_request"
    assert caught.value.reason == "unsupported_subject_token_type"


def test_actor_token_and_bad_scope_and_target_are_rejected() -> None:
    with pytest.raises(WorkloadAuthError) as actor:
        reject_request_shape(
            subject_token="a.b.c",
            subject_token_type=SUBJECT_TOKEN_ID,
            requested_token_type=None,
            scope=None,
            actor_token="delegated",
        )
    assert actor.value.error == "invalid_request"
    with pytest.raises(WorkloadAuthError) as scope:
        reject_request_shape(
            subject_token="a.b.c",
            subject_token_type=SUBJECT_TOKEN_ID,
            requested_token_type=None,
            scope="mcp:access",
            actor_token=None,
        )
    assert scope.value.error == "invalid_scope"
    with pytest.raises(WorkloadAuthError) as target:
        reject_request_shape(
            subject_token="a.b.c",
            subject_token_type=SUBJECT_TOKEN_JWT,
            requested_token_type="urn:ietf:params:oauth:token-type:refresh_token",
            scope=WORKLOAD_SCOPE,
            actor_token=None,
        )
    assert target.value.error == "invalid_target"


def test_hmac_subject_tokens_are_rejected(frozen_time: None) -> None:
    _private, jwks = _private_and_jwks()
    token = jwt.encode(_github_claims(), "secret", algorithm="HS256")
    with pytest.raises(SubjectTokenRejected) as caught:
        verify_subject_jwt(
            token,
            keys=jwks,
            issuer=GITHUB_ISSUER,
            audiences=("https://bifrost.example",),
        )
    assert caught.value.reason == "invalid_subject_token"


def test_profiles_require_their_claim_conditions_and_canonical_jwks() -> None:
    github = profile_for(GITHUB_PROVIDER)
    with pytest.raises(RuleValidationError):
        github.validate_rule(
            issuer=GITHUB_ISSUER,
            conditions=(ClaimCondition("repository", "acme/widgets"),),
            jwks_uri=GITHUB_JWKS_URI,
            pinned_jwks=None,
        )
    with pytest.raises(RuleValidationError):
        github.validate_rule(
            issuer=GITHUB_ISSUER,
            conditions=(
                ClaimCondition("repository", "acme/widgets"),
                ClaimCondition("sub", "repo:acme/widgets:ref:refs/heads/main"),
            ),
            jwks_uri="https://attacker.example/jwks",
            pinned_jwks=None,
        )
    azure = profile_for(AZURE_PROVIDER)
    with pytest.raises(RuleValidationError):
        azure.validate_rule(
            issuer=_AZURE_ISSUER,
            conditions=(
                ClaimCondition("tid", "99999999-9999-9999-9999-999999999999"),
                ClaimCondition("oid", "22222222-2222-2222-2222-222222222222"),
            ),
            jwks_uri=_AZURE_JWKS,
            pinned_jwks=None,
        )
    azure.validate_rule(
        issuer=_AZURE_ISSUER,
        conditions=(
            ClaimCondition("tid", _TENANT),
            ClaimCondition("oid", "22222222-2222-2222-2222-222222222222"),
            ClaimCondition("appid", "33333333-3333-3333-3333-333333333333"),
        ),
        jwks_uri=_AZURE_JWKS,
        pinned_jwks=None,
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://token.actions.githubusercontent.com/.well-known/jwks",
        "https://user:pass@login.microsoftonline.com/keys",
        "https://login.microsoftonline.com:8443/keys",
        "https://localhost/keys",
        "https://127.0.0.1/keys",
        "https://10.1.1.1/keys",
        "https://169.254.169.254/metadata",
    ],
)
def test_jwks_urls_that_are_not_public_https_are_rejected(url: str) -> None:
    with pytest.raises(JwksFetchError):
        validate_jwks_url(url)


@pytest.mark.asyncio
async def test_jwks_fetch_rejects_private_answers_and_pinned_keys_need_no_network(
    frozen_time: None,
) -> None:
    async def getter(url: str) -> bytes:
        raise AssertionError(url)

    with pytest.raises(JwksFetchError):
        await fetch_jwks(
            GITHUB_JWKS_URI, resolver=lambda host: ["10.0.0.8"], getter=getter
        )
    private, jwks = _private_and_jwks()
    selected = await select_rule(
        [_github_rule(jwks)], _sign(private, _github_claims()), SUBJECT_TOKEN_ID
    )
    assert isinstance(selected, tuple)


def test_pinned_private_keys_are_rejected() -> None:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(private))
    jwk["kid"] = "test"
    with pytest.raises(RuleValidationError):
        validate_pinned_jwks({"keys": [jwk]})
