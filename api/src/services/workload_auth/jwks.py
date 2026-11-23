"""JWKS documents: pinned public keys, and a fail-closed HTTPS fetch."""

from __future__ import annotations

import ipaddress
import json
import socket
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any
from urllib.parse import urlsplit

import httpx
import jwt
from jwt import PyJWK

from src.services.workload_auth.constants import (
    ALLOWED_ALGS,
    JWKS_CACHE_SECONDS,
    JWKS_MAX_BYTES,
    JWKS_TIMEOUT_SECONDS,
    MAX_PINNED_KEYS,
)
from src.services.workload_auth.errors import JwksFetchError, RuleValidationError

_cache: dict[str, tuple[float, dict[str, Any]]] = {}

JsonGetter = Callable[[str], Awaitable[bytes]]
HostResolver = Callable[[str], list[str]]


def clear_jwks_cache() -> None:
    _cache.clear()


def validate_pinned_jwks(document: object) -> dict[str, Any]:
    """Accept a JWKS of public verification keys and reject anything else."""
    if not isinstance(document, dict):
        raise RuleValidationError("Pinned JWKS must be a JSON object.")
    keys = document.get("keys")
    if not isinstance(keys, list) or not 1 <= len(keys) <= MAX_PINNED_KEYS:
        raise RuleValidationError("Pinned JWKS must contain 1 to 8 keys.")
    public: list[dict[str, Any]] = []
    for key in keys:
        if not isinstance(key, dict):
            raise RuleValidationError("Each pinned JWKS key must be an object.")
        if "d" in key or "k" in key:
            raise RuleValidationError("Pinned JWKS must contain public keys only.")
        if key.get("kty") not in {"RSA", "EC", "OKP"}:
            raise RuleValidationError("Pinned JWKS keys must be RSA, EC, or OKP.")
        alg = key.get("alg")
        if alg is not None and alg not in ALLOWED_ALGS:
            raise RuleValidationError("Pinned JWKS key uses an unsupported algorithm.")
        try:
            PyJWK.from_dict(key)
        except (jwt.PyJWTError, ValueError, TypeError, KeyError) as exc:
            raise RuleValidationError(
                "Pinned JWKS key is not a usable public key."
            ) from exc
        public.append(dict(key))
    return {"keys": public}


def validate_jwks_url(url: str) -> str:
    """Return the hostname when ``url`` is an acceptable JWKS address."""
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise JwksFetchError("JWKS URL must use HTTPS.")
    if parsed.username or parsed.password:
        raise JwksFetchError("JWKS URL must not include credentials.")
    if parsed.port not in (None, 443):
        raise JwksFetchError("JWKS URL must use port 443.")
    host = parsed.hostname
    if not host or host == "localhost" or host.endswith(".local"):
        raise JwksFetchError("JWKS host is not allowed.")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return host
    _require_public(address)
    return host


def _require_public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        _require_public(mapped)
        return
    if not address.is_global:
        raise JwksFetchError("JWKS address is not a public address.")


def default_resolve(host: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise JwksFetchError("JWKS host did not resolve.") from exc
    addresses = sorted({str(item[4][0]) for item in infos})
    if not addresses:
        raise JwksFetchError("JWKS host did not resolve.")
    return addresses


def _require_public_answers(addresses: Iterable[str]) -> None:
    seen = list(addresses)
    if not seen:
        raise JwksFetchError("JWKS host did not resolve.")
    for raw in seen:
        try:
            parsed = ipaddress.ip_address(raw)
        except ValueError as exc:
            raise JwksFetchError("JWKS host resolved to an invalid address.") from exc
        _require_public(parsed)


def _document_from_body(body: bytes) -> dict[str, Any]:
    if len(body) > JWKS_MAX_BYTES:
        raise JwksFetchError("JWKS document is too large.")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise JwksFetchError("JWKS document is not JSON.") from exc
    try:
        return validate_pinned_jwks(parsed)
    except RuleValidationError as exc:
        raise JwksFetchError("JWKS document has no usable public keys.") from exc


async def _http_get(url: str) -> bytes:
    async with httpx.AsyncClient(
        timeout=JWKS_TIMEOUT_SECONDS,
        follow_redirects=False,
        trust_env=False,
    ) as client:
        try:
            response = await client.get(url)
        except httpx.HTTPError as exc:
            raise JwksFetchError("JWKS fetch failed.") from exc
    if response.is_redirect or response.status_code != 200:
        raise JwksFetchError("JWKS fetch failed.")
    return response.content


async def fetch_jwks(
    url: str,
    *,
    resolver: HostResolver | None = None,
    getter: JsonGetter | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Fetch and cache one JWKS document. Never follows redirects or proxy env."""
    host = validate_jwks_url(url)
    clock = time.monotonic() if now is None else now
    cached = _cache.get(url)
    if cached is not None and cached[0] > clock:
        return cached[1]
    try:
        ipaddress.ip_address(host)
    except ValueError:
        _require_public_answers((resolver or default_resolve)(host))
    body = await (getter or _http_get)(url)
    document = _document_from_body(body)
    _cache[url] = (clock + JWKS_CACHE_SECONDS, document)
    return document
