from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from src.jobs.platform import cloudflare_enable
from src.jobs.platform.base import PlatformJobFailure, PlatformJobRequiresAction
from src.models.orm.cloudflare_capacity import CloudflareOwnedResource
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnershipResource,
)
from src.services.worker_pool.external_client import (
    CapacityProtocolCheck,
    CapacityProtocolError,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "disposition", "resource", "check", "observed", "expected"),
    [
        (
            ObservationSettling("private-provider-value", OwnershipResource.DURABLE_OBJECT_NAMESPACE,
                                ObservationCheck.NAMESPACE_EXPORTS_PENDING),
            PlatformJobFailure, "durable_object_namespace", "namespace_exports",
            "missing_expected_classes", "exact_export_set",
        ),
        (
            ObservationPending("private-provider-value", OwnershipResource.DURABLE_OBJECT_NAMESPACE,
                               ObservationCheck.NAMESPACE_EXPORTS_UNEXPECTED),
            PlatformJobRequiresAction, "durable_object_namespace", "namespace_exports",
            "unexpected_classes", "exact_export_set",
        ),
        (
            CapacityProtocolError("private-provider-value", CapacityProtocolCheck.RESOURCE_IDENTITY),
            PlatformJobRequiresAction, "canary_launcher", "resource_identity", "different", "exact_reservation",
        ),
        (
            httpx.HTTPStatusError(
                "private-provider-value",
                request=httpx.Request("GET", "https://private-provider.test/v1/capabilities"),
                response=httpx.Response(404),
            ),
            PlatformJobRequiresAction, "workers_scope", "capabilities_request", "rejected", "successful_response",
        ),
        (
            httpx.HTTPStatusError(
                "private-provider-value",
                request=httpx.Request("POST", "https://private-provider.test/v1/workers/fixture/prepare"),
                response=httpx.Response(400),
            ),
            PlatformJobRequiresAction, "canary_launcher", "prepare_request", "rejected", "successful_response",
        ),
        (
            httpx.HTTPStatusError(
                "private-provider-value",
                request=httpx.Request("GET", "https://private-provider.test/v1/unexpected"),
                response=httpx.Response(400),
            ),
            PlatformJobRequiresAction, "capacity_provider", "protocol", "invalid", "valid",
        ),
    ],
)
async def test_enable_returns_only_categories_and_retries_only_settling_reads(
    monkeypatch, error, disposition, resource, check, observed, expected,
) -> None:
    monkeypatch.setattr(cloudflare_enable, "load_workers_release", lambda: object())
    monkeypatch.setattr(cloudflare_enable, "_settle_enable", AsyncMock(side_effect=error))
    context = cloudflare_enable.PlatformJobContext(uuid4(), uuid4(), None, "actor", "example@example.test", "Example")
    monkeypatch.setattr(cloudflare_enable.PlatformJobContext, "report", AsyncMock())
    payload = cloudflare_enable.CloudflareEnablePayload(installation_id=uuid4(), revision=1)

    with pytest.raises(disposition) as failure:
        await cloudflare_enable.CLOUDFLARE_ENABLE_DEFINITION.handler(context, payload)

    assert failure.value.result == {"code": "CF_DEPLOYMENT_UNVERIFIED", "details": {
        "resource_class": resource, "check": check,
        "observed_category": observed, "expected_category": expected,
    }}
    if isinstance(failure.value, PlatformJobFailure):
        assert failure.value.retryable
        assert failure.value.code == "CF_DEPLOYMENT_UNVERIFIED"


@pytest.mark.asyncio
async def test_creating_script_with_recorded_version_cannot_retry_an_absent_readback() -> None:
    resource = CloudflareOwnedResource(
        installation_id=uuid4(), account_id="a" * 32, scope="workers", kind="worker_script",
        target_name="bifrost-fixture", effect_id=uuid4(), desired_revision=1, artifact_digest="b" * 64,
        state="creating", provider_id=str(uuid4()), provider_version=str(uuid4()),
        provider_details={"upload_acknowledged": True},
    )
    adapter = AsyncMock()
    adapter.observe.return_value = None

    with pytest.raises(ObservationPending) as failure:
        await cloudflare_enable._install_script(adapter, resource, token=SecretStr("synthetic-token"))

    assert not isinstance(failure.value, ObservationSettling)
    assert failure.value.check is ObservationCheck.SCRIPT_RECORDED_ABSENT
    adapter.configure_endpoint.assert_not_awaited()
