"""Measure the Container network boundary from the trusted supervisor."""

import asyncio
from ipaddress import ip_address
from ssl import create_default_context
from uuid import UUID

import httpx

CONTAINER_RELAY_REJECTION = "x-bifrost-container-relay-rejection"
CONTAINER_EGRESS_DENIALS = frozenset({"alternate-origin", "wrong-host", "raw-tcp", "disallowed-route", "redirect"})
# The exact TLS intercept can accept a TCP handshake at 443 before seeing
# SNI. Probe another Cloudflare edge port and verify it is reachable from
# Bifrost before asking the guest to connect.
# https://developers.cloudflare.com/fundamentals/reference/network-ports/
CONTAINER_RAW_PROBE_PORT = 8443


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
        async def denied(url: str, reason: str, headers: dict[str, str] | None = None) -> bool:
            async with client.stream("GET", url, headers=headers) as response:
                return response.status_code == 403 and response.headers.get(CONTAINER_RELAY_REJECTION) == reason

        async def redirect_denied() -> bool:
            async with client.stream("GET", route, headers={**auth, "X-Bifrost-Canary-Redirect": "1"}) as response:
                # The dedicated ingress can suppress the redirect before it
                # reaches the relay. Both guards return their own marker.
                return "location" not in response.headers and (
                    (response.status_code == 403 and response.headers.get(CONTAINER_RELAY_REJECTION) == "redirect")
                    or (response.status_code == 404 and response.headers.get("x-bifrost-worker-ingress") == "v1; denied")
                )

        async def connect_denied(target: str, port: int) -> bool:
            # A successful TCP connection is enough to fail the challenge.
            # TLS trust failures must never stand in for a network denial.
            try:
                async with asyncio.timeout(5):
                    _, writer = await asyncio.open_connection(target, port)
            except (OSError, TimeoutError):
                return True
            writer.close()
            await writer.wait_closed()
            return False

        async with client.stream("GET", route, headers=auth) as response:
            if response.status_code != 204 or response.headers.get("x-bifrost-worker-ingress") != "v1; allowed":
                raise RuntimeError("The Container SDK canary did not reach verified worker ingress")
        results = await asyncio.gather(
            connect_denied(alternate_url.host, 443), connect_denied(address, CONTAINER_RAW_PROBE_PORT),
            denied(route, "origin", {"Host": "wrong-host.invalid"}),
            denied(f"{origin}/__bifrost_canary_denied__", "route"),
            redirect_denied(),
        )
    return [
        name for name, observed in zip(
            ("alternate-origin", "raw-tcp", "wrong-host", "disallowed-route", "redirect"), results, strict=True,
        ) if observed
    ]
