"""HTTP token exchange for GitHub Actions and Azure managed-identity tokens.

Subject tokens are signed with a key generated in the test. Nothing is fetched
from GitHub or Microsoft.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import select
from src.models.orm.audit import AuditLog
from src.services.workload_auth.constants import (
    GITHUB_ISSUER,
    SUBJECT_TOKEN_ID,
    SUBJECT_TOKEN_JWT,
    TOKEN_EXCHANGE_GRANT,
)

_TENANT = "11111111-1111-1111-1111-111111111111"
_OID = "22222222-2222-2222-2222-222222222222"
_AUDIENCE = "api://bifrost-workload-test"


def _keys() -> tuple[object, dict]:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(private.public_key()))
    jwk.update({"kid": "e2e", "alg": "RS256", "use": "sig"})
    return private, {"keys": [jwk]}


def _sign(private: object, claims: dict) -> str:
    return jwt.encode(claims, private, algorithm="RS256", headers={"kid": "e2e"})  # type: ignore[arg-type]


def _exchange(e2e_client, token: str, token_type: str):
    return e2e_client.post(
        "/token",
        data={
            "grant_type": TOKEN_EXCHANGE_GRANT,
            "subject_token": token,
            "subject_token_type": token_type,
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": "bifrost:api",
        },
    )


@pytest.mark.asyncio
async def test_workload_exchange_issues_an_identity_token(
    e2e_client, platform_admin, org1_user, db_session
) -> None:
    private, jwks = _keys()
    created = e2e_client.post(
        "/api/identities",
        headers=platform_admin.headers,
        json={"name": f"Workload {uuid4().hex[:8]}", "organization_id": None},
    )
    assert created.status_code == 201, created.text
    identity_id = created.json()["id"]
    now = datetime.now(UTC)
    github_sub = "repo:acme/widgets:ref:refs/heads/main"
    github_claims = {
        "iss": GITHUB_ISSUER,
        "aud": _AUDIENCE,
        "sub": github_sub,
        "repository": "acme/widgets",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
    }
    azure_issuer = f"https://login.microsoftonline.com/{_TENANT}/v2.0"
    azure_claims = {
        "iss": azure_issuer,
        "aud": _AUDIENCE,
        "sub": _OID,
        "tid": _TENANT,
        "oid": _OID,
        "appid": "33333333-3333-3333-3333-333333333333",
        "ver": "2.0",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
    }
    rules = []
    specs = (
        (
            f"github-{uuid4().hex[:8]}",
            "github_actions",
            GITHUB_ISSUER,
            SUBJECT_TOKEN_ID,
            [
                {"claim": "repository", "equals": "acme/widgets"},
                {"claim": "sub", "equals": github_sub},
            ],
        ),
        (
            f"azure-{uuid4().hex[:8]}",
            "azure_managed_identity",
            azure_issuer,
            SUBJECT_TOKEN_JWT,
            [
                {"claim": "tid", "equals": _TENANT},
                {"claim": "oid", "equals": _OID},
            ],
        ),
    )
    for name, provider, issuer, token_type, conditions in specs:
        response = e2e_client.post(
            "/api/settings/workload-trust",
            headers=platform_admin.headers,
            json={
                "name": name,
                "provider": provider,
                "issuer": issuer,
                "audiences": [_AUDIENCE],
                "pinned_jwks": jwks,
                "claim_conditions": conditions,
                "identity_id": identity_id,
                "token_lifetime_seconds": 300,
            },
        )
        assert response.status_code == 201, response.text
        assert response.json()["subject_token_type"] == token_type
        rules.append(response.json())

    denied = e2e_client.post(
        "/api/settings/workload-trust",
        headers=org1_user.headers,
        json={
            "name": f"denied-{uuid4().hex[:8]}",
            "provider": "github_actions",
            "issuer": GITHUB_ISSUER,
            "audiences": [_AUDIENCE],
            "pinned_jwks": jwks,
            "claim_conditions": [
                {"claim": "repository", "equals": "acme/widgets"},
                {"claim": "sub", "equals": github_sub},
            ],
            "identity_id": identity_id,
        },
    )
    assert denied.status_code == 403, denied.text

    human_target = e2e_client.post(
        "/api/settings/workload-trust",
        headers=platform_admin.headers,
        json={
            "name": f"human-{uuid4().hex[:8]}",
            "provider": "github_actions",
            "issuer": GITHUB_ISSUER,
            "audiences": [_AUDIENCE],
            "pinned_jwks": jwks,
            "claim_conditions": [
                {"claim": "repository", "equals": "acme/widgets"},
                {"claim": "sub", "equals": github_sub},
            ],
            "identity_id": str(platform_admin.user_id),
        },
    )
    assert human_target.status_code == 400, human_target.text

    github_token = _sign(private, github_claims)
    first = _exchange(e2e_client, github_token, SUBJECT_TOKEN_ID)
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["token_type"] == "Bearer"
    assert body["issued_token_type"] == "urn:ietf:params:oauth:token-type:access_token"
    assert body["scope"] == "bifrost:api"
    assert "refresh_token" not in body
    assert body["access_token"] != github_token
    assert body["expires_in"] <= 300
    assert _exchange(e2e_client, github_token, SUBJECT_TOKEN_ID).status_code == 200

    me = e2e_client.get(
        "/auth/me", headers={"Authorization": f"Bearer {body['access_token']}"}
    )
    assert me.status_code == 200, me.text
    assert me.json()["id"] == identity_id
    forbidden = e2e_client.get(
        "/api/settings/workload-trust",
        headers={"Authorization": f"Bearer {body['access_token']}"},
    )
    assert forbidden.status_code == 403, forbidden.text

    azure = _exchange(e2e_client, _sign(private, azure_claims), SUBJECT_TOKEN_JWT)
    assert azure.status_code == 200, azure.text

    wrong_audience = _exchange(
        e2e_client,
        _sign(private, {**github_claims, "aud": "https://other.example"}),
        SUBJECT_TOKEN_ID,
    )
    assert wrong_audience.status_code == 400
    assert wrong_audience.json()["error"] == "invalid_grant"

    unknown = _exchange(
        e2e_client, github_token, "urn:ietf:params:oauth:token-type:saml2"
    )
    assert unknown.status_code == 400
    assert unknown.json()["error"] == "invalid_request"

    disabled = e2e_client.patch(
        f"/api/settings/workload-trust/{rules[0]['id']}",
        headers=platform_admin.headers,
        json={"enabled": False},
    )
    assert disabled.status_code == 200, disabled.text
    refused = _exchange(e2e_client, github_token, SUBJECT_TOKEN_ID)
    assert refused.status_code == 400
    assert refused.json()["error"] == "invalid_grant"

    rows = (
        (
            await db_session.execute(
                select(AuditLog).where(
                    AuditLog.action == "auth.workload_exchange.success"
                )
            )
        )
        .scalars()
        .all()
    )
    assert rows
    assert all("subject_token" not in (row.details or {}) for row in rows)
    failures = (
        (
            await db_session.execute(
                select(AuditLog).where(
                    AuditLog.action == "auth.workload_exchange.failed"
                )
            )
        )
        .scalars()
        .all()
    )
    assert failures

    for rule in rules:
        deleted = e2e_client.delete(
            f"/api/settings/workload-trust/{rule['id']}",
            headers=platform_admin.headers,
        )
        assert deleted.status_code == 204, deleted.text
    removed = e2e_client.delete(
        f"/api/identities/{identity_id}", headers=platform_admin.headers
    )
    assert removed.status_code == 204, removed.text
