"""Thin helpers that obtain a provider token and exchange it at BiFrost.

The URNs match ``src.services.workload_auth.constants``. A unit test keeps
them equal. This module does not validate the provider token.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import httpx

TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
SUBJECT_TOKEN_ID = "urn:ietf:params:oauth:token-type:id_token"
SUBJECT_TOKEN_JWT = "urn:ietf:params:oauth:token-type:jwt"
ISSUED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"
WORKLOAD_SCOPE = "bifrost:api"

_AZURE_IMDS = "http://169.254.169.254/metadata/identity/oauth2/token"


class WorkloadLoginError(Exception):
    """The provider or the BiFrost token endpoint refused the exchange."""


def _token_value(payload: Any, source: str) -> str:
    if not isinstance(payload, dict):
        raise WorkloadLoginError(f"{source} did not return a token.")
    value = payload.get("value")
    if not isinstance(value, str) or not value or "\n" in value:
        raise WorkloadLoginError(f"{source} did not return a token.")
    return value


async def fetch_github_actions_token(
    audience: str,
    *,
    env: Mapping[str, str] | None = None,
    client: httpx.AsyncClient | None = None,
) -> str:
    """Request a GitHub Actions OIDC token for ``audience``."""
    source = os.environ if env is None else env
    request_url = source.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
    request_token = source.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
    if not request_url or not request_token:
        raise WorkloadLoginError(
            "GitHub Actions OIDC is unavailable. Set id-token: write and run inside a job."
        )
    separator = "&" if "?" in request_url else "?"
    url = f"{request_url}{separator}audience={quote(audience, safe='')}"
    headers = {"Authorization": f"Bearer {request_token}", "Accept": "application/json"}
    if client is None:
        async with httpx.AsyncClient(
            timeout=10.0, follow_redirects=False, trust_env=False
        ) as owned:
            response = await owned.get(url, headers=headers)
    else:
        response = await client.get(url, headers=headers)
    if response.status_code != 200:
        raise WorkloadLoginError("GitHub Actions refused the OIDC request.")
    return _token_value(response.json(), "GitHub Actions")


async def fetch_azure_managed_identity_token(
    audience: str,
    *,
    env: Mapping[str, str] | None = None,
    client: httpx.AsyncClient | None = None,
) -> str:
    """Request an Azure managed-identity access token for ``audience`` (the resource)."""
    source = os.environ if env is None else env
    identity_endpoint = source.get("IDENTITY_ENDPOINT", "")
    identity_header = source.get("IDENTITY_HEADER", "")
    resource = quote(audience, safe="")
    if identity_endpoint and identity_header:
        separator = "&" if "?" in identity_endpoint else "?"
        url = (
            f"{identity_endpoint}{separator}api-version=2019-08-01&resource={resource}"
        )
        headers = {"X-IDENTITY-HEADER": identity_header, "Metadata": "true"}
    else:
        url = f"{_AZURE_IMDS}?api-version=2018-02-01&resource={resource}"
        headers = {"Metadata": "true"}
    if client is None:
        async with httpx.AsyncClient(
            timeout=10.0, follow_redirects=False, trust_env=False
        ) as owned:
            response = await owned.get(url, headers=headers)
    else:
        response = await client.get(url, headers=headers)
    if response.status_code != 200:
        raise WorkloadLoginError("Azure managed identity refused the token request.")
    return _token_value(response.json(), "Azure managed identity")


async def exchange_subject_token(
    api_url: str,
    subject_token: str,
    subject_token_type: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """POST the subject token to the BiFrost token endpoint."""
    body = {
        "grant_type": TOKEN_EXCHANGE_GRANT,
        "subject_token": subject_token,
        "subject_token_type": subject_token_type,
        "requested_token_type": ISSUED_TOKEN_TYPE,
        "scope": WORKLOAD_SCOPE,
    }
    url = f"{api_url.rstrip('/')}/token"

    async def _post(http: httpx.AsyncClient) -> httpx.Response:
        return await http.post(url, data=body)

    if client is None:
        async with httpx.AsyncClient(
            timeout=30.0, follow_redirects=False, trust_env=False
        ) as owned:
            response = await _post(owned)
    else:
        response = await _post(client)
    try:
        payload = response.json()
    except ValueError as exc:
        raise WorkloadLoginError("BiFrost token endpoint did not return JSON.") from exc
    if response.status_code != 200:
        description = (
            payload.get("error_description") if isinstance(payload, dict) else None
        )
        message = (
            description
            if isinstance(description, str) and description
            else "Token exchange failed."
        )
        raise WorkloadLoginError(message)
    access = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(access, str) or not access or payload.get("refresh_token"):
        raise WorkloadLoginError(
            "BiFrost token endpoint returned an unexpected response."
        )
    return payload


def export_github_env(api_url: str, access_token: str) -> bool:
    """Append the access token for later steps when ``GITHUB_ENV`` is set."""
    path = os.environ.get("GITHUB_ENV")
    if not path:
        return False
    if any(char in value for value in (api_url, access_token) for char in "\n\r"):
        raise WorkloadLoginError("Refusing to export a multiline credential.")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"BIFROST_API_URL={api_url}\n")
        handle.write(f"BIFROST_ACCESS_TOKEN={access_token}\n")
    return True
