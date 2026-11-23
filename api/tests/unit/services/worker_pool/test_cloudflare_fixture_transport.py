"""The test-only Cloudflare substitute must fail closed on a missing mock."""

import httpcore
import httpx
import pytest
from scripts.test_cloudflare_fixture import block_cloudflare_network


@pytest.mark.asyncio
async def test_test_fixture_blocks_hosted_endpoints_before_network_io(monkeypatch):
    def unexpected_io(*_args, **_kwargs):
        pytest.fail("Cloudflare request reached the connection pool")

    monkeypatch.setattr(httpcore.ConnectionPool, "handle_request", unexpected_io)
    monkeypatch.setattr(httpcore.AsyncConnectionPool, "handle_async_request", unexpected_io)
    block_cloudflare_network(monkeypatch.setattr)

    with (
        httpx.Client(trust_env=False) as client,
        pytest.raises(RuntimeError, match="Hosted Cloudflare requests are blocked"),
    ):
        client.get("https://api.cloudflare.com/client/v4")

    async with httpx.AsyncClient(trust_env=False) as client:
        with pytest.raises(RuntimeError, match="Hosted Cloudflare requests are blocked"):
            await client.get("https://test.example.workers.dev./v1/profile")

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(204))) as client:
        assert (await client.get("https://api.cloudflare.com/client/v4")).status_code == 204
