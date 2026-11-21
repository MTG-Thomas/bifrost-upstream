from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.jobs.platform import cloudflare_enable
from src.jobs.platform.base import PlatformJobCancelled, PlatformJobContext
from src.jobs.platform.cloudflare_capacity import _replacement_revision
from src.models.orm.cloudflare_capacity import CloudflareInstallation


@pytest.mark.parametrize(("package", "pending", "admitted", "repair", "expected"), [
    ("new", None, 4, False, 5),
    ("old", None, 4, True, None),
    (None, None, 4, True, None),
    ("new", "new", None, True, None),
    ("newer", "new", None, False, None),
    ("old", None, None, True, 5),
    ("old", None, None, False, None),
])
def test_replacement_revision(package, pending, admitted, repair, expected) -> None:
    row = CloudflareInstallation(
        desired_revision=4, deployed_revision=4, admitted_revision=admitted,
        upgrade_release=pending, policy={"bounds_release": "old"},
    )
    assert _replacement_revision(row, package, repair=repair) == expected


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
