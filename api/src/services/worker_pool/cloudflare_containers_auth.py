"""Sign Container control requests with incarnation-scoped keys."""

import json
import secrets
from hashlib import sha256
from hmac import digest
from typing import Literal

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from src.services.worker_pool.external_client import CAPACITY_REQUEST_SECONDS, ExternalCapacityClient

CERTIFICATE_DOMAIN = b"bifrost.containers.incarnation.v1\0"
REQUEST_DOMAIN = b"bifrost.containers.request.v1\0"
INCARNATION_KEY_DOMAIN = b"bifrost.containers.incarnation-key.v1\0"
CERTIFICATE_LIFETIME_MS = 60_000


def _key(secret: str, domain: bytes) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(digest(secret.encode("ascii"), domain, "sha256"))


def container_scope_public_key(scope_secret: str) -> str:
    return _key(scope_secret, CERTIFICATE_DOMAIN).public_key().public_bytes_raw().hex()


def sign_container_request(
    scope_secret: str, *, path: str, pin: dict[str, str | int],
    body: bytes, now_ms: int,
) -> dict[str, str]:
    """Certify the public incarnation key and sign the exact request bytes."""
    scope_key = _key(scope_secret, CERTIFICATE_DOMAIN)
    incarnation_key = _key(scope_secret, INCARNATION_KEY_DOMAIN + str(pin["uid"]).encode("ascii"))
    public = incarnation_key.public_key().public_bytes_raw().hex()
    expires = now_ms + CERTIFICATE_LIFETIME_MS
    nonce = secrets.token_hex(16)
    certificate = json.dumps(
        [sorted(pin.items()), public, expires],
        separators=(",", ":"),
    ).encode("ascii")
    request = json.dumps([path, now_ms, expires, nonce, sha256(body).hexdigest()], separators=(",", ":")).encode("ascii")
    return {
        "Content-Type": "application/json",
        "X-Bifrost-Incarnation-Key": public,
        "X-Bifrost-Incarnation-Certificate": scope_key.sign(CERTIFICATE_DOMAIN + certificate).hex(),
        "X-Bifrost-Incarnation-Expires": str(expires),
        "X-Bifrost-Request-Time": str(now_ms),
        "X-Bifrost-Request-Nonce": nonce,
        "X-Bifrost-Request-Signature": incarnation_key.sign(REQUEST_DOMAIN + request).hex(),
    }


async def request_container_control(
    http: httpx.AsyncClient, endpoint: str, scope_secret: str, control_key: str, *,
    operation: Literal["start", "stop", "egress-probe", "void", "observe-start", "close-absent", "close", "retire"],
    pin: dict[str, str | int], now_ms: int, payload: dict[str, str | int],
) -> bytes:
    origin = ExternalCapacityClient.check_configuration(endpoint, scope_secret)
    if httpx.URL(origin).path != "/":
        raise ValueError("Container control requires the verified Worker origin")
    path = f"/v1/{operation}"
    document: dict[str, str | int | dict[str, str | int]] = {**payload, "pin": pin}
    if operation in ("start", "egress-probe"):
        document["control_key"] = control_key
    body = json.dumps(document, separators=(",", ":")).encode()
    async with http.stream(
        "POST", origin + path, content=body,
        headers=sign_container_request(scope_secret, path=path, pin=pin, body=body, now_ms=now_ms),
        timeout=CAPACITY_REQUEST_SECONDS, follow_redirects=False,
    ) as response:
        response.raise_for_status()
        receipt = bytearray()
        async for chunk in response.aiter_bytes():
            if len(receipt) + len(chunk) > 8192:
                raise ValueError("Container control receipt exceeds the protocol limit")
            receipt.extend(chunk)
    return bytes(receipt)
