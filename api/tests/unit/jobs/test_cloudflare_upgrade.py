from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.jobs.platform import cloudflare_enable
from src.jobs.platform.base import PlatformJobCancelled, PlatformJobContext
from src.jobs.platform.cloudflare_capacity import _replacement_revision
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.worker_pool.cloudflare_release import WorkersReleaseOrder


def test_replacement_requires_a_new_artifact_or_cutover_marker() -> None:
    order = WorkersReleaseOrder("1.3.0", 100)
    row = CloudflareInstallation(
        desired_revision=4, deployed_revision=3, admitted_revision=None, drain_required=False,
        policy={"bounds_release": "active"}, workers_release_version=order.version,
        workers_release_sequence=order.sequence, workers_release_digest="active",
    )
    assert _replacement_revision(row, "active", order) is None
    row.drain_required = True
    assert _replacement_revision(row, "active", order) == 5
    row.drain_required = False
    assert _replacement_revision(row, "hotfix", WorkersReleaseOrder("1.2.5", 101)) is None
    assert _replacement_revision(row, "conflict", WorkersReleaseOrder("1.3.0", 102)) is None
    assert _replacement_revision(row, "active", WorkersReleaseOrder("1.3.0", 102)) is None
    assert _replacement_revision(row, "new", WorkersReleaseOrder("1.4.0", 1)) == 5


def test_semver_precedes_build_sequence() -> None:
    assert WorkersReleaseOrder("1.3.0-rc.2", 100).key < WorkersReleaseOrder("1.3.0-rc.10", 1).key
    assert WorkersReleaseOrder("1.3.0-rc.10", 100).key < WorkersReleaseOrder("1.3.0", 1).key
    assert WorkersReleaseOrder("1.3.0+metadata", 2).precedence == WorkersReleaseOrder("1.3.0", 1).precedence


@pytest.mark.asyncio
async def test_stale_upgrade_lease_cannot_inspect_or_mutate_provider(monkeypatch) -> None:
    database = SimpleNamespace(get=AsyncMock())

    @asynccontextmanager
    async def context():
        yield database

    hold = AsyncMock(side_effect=PlatformJobCancelled)
    monkeypatch.setattr(cloudflare_enable, "get_db_context", context)
    monkeypatch.setattr(cloudflare_enable, "_hold_installation_lease", hold)
    run = PlatformJobContext(uuid4(), uuid4(), None, "system", "", "Bifrost")
    payload = cloudflare_enable.CloudflareEnablePayload(installation_id=uuid4(), revision=5)
    adapter = AsyncMock()

    with pytest.raises(PlatformJobCancelled):
        await cloudflare_enable._drain_previous_revision(run, payload, adapter, upgrading=True)

    hold.assert_awaited_once_with(database, run, payload.installation_id)
    database.get.assert_not_awaited()
    assert adapter.mock_calls == []
