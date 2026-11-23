"""Measure the Container network boundary from the trusted supervisor."""

import asyncio
import errno
from ipaddress import ip_address
from ssl import create_default_context
from uuid import UUID

import httpx

CONTAINER_RELAY_REJECTION = "x-bifrost-container-relay-rejection"
_RELAY_FORWARDED = "x-bifrost-container-relay-forwarded"
CONTAINER_EGRESS_DENIALS = frozenset({"alternate-origin", "wrong-host", "raw-tcp", "disallowed-route", "redirect"})
# The exact TLS intercept can accept a TCP handshake at 443 before seeing
# SNI. Probe another Cloudflare edge port and verify it is reachable from
# Bifrost before asking the guest to connect.
# https://developers.cloudflare.com/fundamentals/reference/network-ports/
CONTAINER_RAW_PROBE_PORT = 8443
CONTAINER_ALTERNATE_PROBE_PORT = 2053
_CONNECT_DENIALS = frozenset({
    errno.EACCES, errno.ECONNREFUSED, errno.ECONNRESET, errno.EHOSTUNREACH,
    errno.ENETUNREACH, errno.EPERM, errno.ETIMEDOUT,
})


async def run_container_egress_checks(
    origin: str, alternate: str, address: str, worker_id: UUID, generation: int, proof: str,
) -> list[str]:
    """Return only denials observed at the boundary. Send no credentials to probes on other origins."""
    ingress, alternate_url = httpx.URL(origin), httpx.URL(alternate)
    if (
        ingress.scheme != "https" or alternate_url.scheme != "https" or ingress.host == alternate_url.host
        or alternate_url.port is not None or alternate_url.path != "/" or alternate_url.query
        or alternate_url.fragment or alternate_url.userinfo
    ):
        raise ValueError("The Container egress challenge lacks distinct HTTPS endpoints")
    ip_address(address)
    route = f"{origin}/api/sdk/capacity-canary/{worker_id}"
    auth = {"Authorization": f"Bearer {proof}", "X-Bifrost-Canary-Generation": str(generation)}
    tls = create_default_context(cafile="/etc/cloudflare/certs/cloudflare-containers-ca.crt")
    async with httpx.AsyncClient(verify=tls, trust_env=False, follow_redirects=False) as client:
        def require_probe_response(response: httpx.Response) -> None:
            if response.status_code >= 500 or response.status_code in (408, 429):
                response.raise_for_status()

        async def denied(url: str, reason: str, headers: dict[str, str] | None = None) -> bool:
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code == 403 and response.headers.get(CONTAINER_RELAY_REJECTION) == reason:
                    return True
                if response.headers.get(_RELAY_FORWARDED) == "1":
                    return False
                require_probe_response(response)
                return False

        async def redirect_denied() -> bool:
            async with client.stream("GET", route, headers={**auth, "X-Bifrost-Canary-Redirect": "1"}) as response:
                # The dedicated ingress can suppress the redirect before it
                # reaches the relay. Both guards return their own marker.
                if "location" not in response.headers and (
                    (response.status_code == 403 and response.headers.get(CONTAINER_RELAY_REJECTION) == "redirect")
                    or (response.status_code == 404 and response.headers.get("x-bifrost-worker-ingress") == "v1; denied")
                ):
                    return True
                if response.headers.get(_RELAY_FORWARDED) == "1":
                    return False
                require_probe_response(response)
                return False

        async def connect_denied(target: str, port: int) -> bool:
            # A successful TCP connection is enough to fail the challenge.
            # TLS trust failures must never stand in for a network denial.
            try:
                async with asyncio.timeout(5):
                    _, writer = await asyncio.open_connection(target, port)
            except TimeoutError:
                return True
            except OSError as exc:
                if exc.errno not in _CONNECT_DENIALS:
                    raise
                return True
            writer.close()
            await writer.wait_closed()
            return False

        async with client.stream("GET", route, headers=auth) as response:
            if response.status_code != 204 or response.headers.get("x-bifrost-worker-ingress") != "v1; allowed":
                raise RuntimeError("The Container SDK canary did not reach verified worker ingress")
        results = await asyncio.gather(
            # The API has already resolved the alternate name and reached
            # both sockets. Guest DNS is intentionally restricted by policy.
            connect_denied(address, CONTAINER_ALTERNATE_PROBE_PORT), connect_denied(address, CONTAINER_RAW_PROBE_PORT),
            denied(route, "origin", {"Host": "wrong-host.invalid"}),
            denied(f"{origin}/__bifrost_canary_denied__", "route"),
            redirect_denied(),
            return_exceptions=True,
        )
    if False not in results:
        for result in results:
            if isinstance(result, BaseException):
                raise result
    return [
        name for name, observed in zip(
            ("alternate-origin", "raw-tcp", "wrong-host", "disallowed-route", "redirect"), results, strict=True,
        ) if observed is True
    ]
