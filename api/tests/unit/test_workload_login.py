"""The CLI only fetches a provider token and posts it to the token endpoint."""

from __future__ import annotations

import httpx
import pytest
from bifrost import cli, workload_login
from bifrost.workload_login import (
    SUBJECT_TOKEN_ID,
    SUBJECT_TOKEN_JWT,
    WorkloadLoginError,
    exchange_subject_token,
    export_github_env,
    fetch_azure_managed_identity_token,
    fetch_github_actions_token,
)


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_github_helper_requests_the_actions_audience() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["authorization"] = request.headers["Authorization"]
        return httpx.Response(200, json={"value": "header.payload.sig"})

    async with _client(handler) as client:
        token = await fetch_github_actions_token(
            "api://bifrost",
            env={
                "ACTIONS_ID_TOKEN_REQUEST_URL": "https://actions.example/token?run=1",
                "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
            },
            client=client,
        )
    assert token == "header.payload.sig"
    assert seen["authorization"] == "Bearer request-token"
    assert "audience=api%3A%2F%2Fbifrost" in seen["url"]


@pytest.mark.asyncio
async def test_azure_helper_uses_the_identity_endpoint_or_imds() -> None:
    def app_service(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-IDENTITY-HEADER"] == "header-secret"
        assert "resource=api%3A%2F%2Fbifrost" in str(request.url)
        return httpx.Response(200, json={"value": "azure.jwt.token"})

    async with _client(app_service) as client:
        token = await fetch_azure_managed_identity_token(
            "api://bifrost",
            env={
                "IDENTITY_ENDPOINT": "http://127.0.0.1:42356/msi/token",
                "IDENTITY_HEADER": "header-secret",
            },
            client=client,
        )
    assert token == "azure.jwt.token"

    def imds(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "169.254.169.254"
        assert request.headers["Metadata"] == "true"
        return httpx.Response(200, json={"value": "imds.jwt.token"})

    async with _client(imds) as client:
        token = await fetch_azure_managed_identity_token(
            "api://bifrost", env={}, client=client
        )
    assert token == "imds.jwt.token"


@pytest.mark.asyncio
async def test_exchange_posts_the_rfc_form_and_rejects_a_refresh_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content.decode()
        assert (
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"
            in body
        )
        assert "subject_token=subject.jwt.value" in body
        assert "refresh_token" not in body
        return httpx.Response(
            200,
            json={
                "access_token": "bifrost.jwt.value",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "token_type": "Bearer",
                "expires_in": 900,
                "scope": "bifrost:api",
            },
        )

    async with _client(handler) as client:
        payload = await exchange_subject_token(
            "https://bifrost.example",
            "subject.jwt.value",
            SUBJECT_TOKEN_ID,
            client=client,
        )
    assert payload["access_token"] != "subject.jwt.value"

    def with_refresh(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "x", "refresh_token": "y"})

    async with _client(with_refresh) as client:
        with pytest.raises(WorkloadLoginError):
            await exchange_subject_token(
                "https://bifrost.example", "a.b.c", SUBJECT_TOKEN_JWT, client=client
            )


def test_github_env_export_appends_the_access_token(tmp_path, monkeypatch) -> None:
    path = tmp_path / "github.env"
    path.write_text("EXISTING=1\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_ENV", str(path))
    assert export_github_env("https://bifrost.example", "access.jwt.value") is True
    text = path.read_text(encoding="utf-8")
    assert "BIFROST_ACCESS_TOKEN=access.jwt.value\n" in text
    assert "BIFROST_REFRESH_TOKEN" not in text


def test_login_workload_requires_audience_and_stores_access_only(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    assert cli.handle_login(["--workload", "github"]) == 1
    assert "--audience" in capsys.readouterr().err
    saved: dict[str, str] = {}

    def save_credentials(**kwargs):
        saved.update(kwargs)

    async def fake_github(audience: str, **kwargs):
        assert audience == "api://bifrost"
        return "subject.jwt.value"

    async def fake_exchange(
        api_url: str, subject_token: str, subject_token_type: str, **kwargs
    ):
        assert api_url == "https://bifrost.example"
        assert subject_token == "subject.jwt.value"
        assert subject_token_type == SUBJECT_TOKEN_ID
        return {"access_token": "bifrost.jwt.value", "expires_in": 120}

    monkeypatch.setattr(cli, "fetch_github_actions_token", fake_github)
    monkeypatch.setattr(cli, "exchange_subject_token", fake_exchange)
    monkeypatch.setattr(workload_login, "fetch_github_actions_token", fake_github)
    monkeypatch.setattr(cli.credentials, "save_credentials", save_credentials)
    rc = cli.handle_login(
        [
            "--workload",
            "github",
            "--audience",
            "api://bifrost",
            "--url",
            "https://bifrost.example",
        ]
    )
    assert rc == 0
    assert saved["access_token"] == "bifrost.jwt.value"
    assert saved["refresh_token"] == ""
    assert "bifrost.jwt.value" not in capsys.readouterr().out
