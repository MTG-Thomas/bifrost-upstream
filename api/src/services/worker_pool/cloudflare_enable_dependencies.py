"""Provider evidence required before enabling a Workers deployment."""

from collections.abc import Mapping
from typing import Protocol

from src.models.orm.cloudflare_capacity import CloudflareInstallation


class WorkersPaidUnverified(Exception):
    """An independently authorized check could not verify Workers Paid."""


class WorkersUsageUnverified(Exception):
    """The deployment does not have a verifiable usage sample."""


class CheckWorkersPaid(Protocol):
    async def __call__(self, account_id: str) -> None:
        """Verify the current Workers Paid subscription for this account."""
        ...


class RefreshWorkersUsage(Protocol):
    async def __call__(
        self, installation: CloudflareInstallation, *, revision: int,
        scripts: Mapping[str, Mapping[str, str]],
    ) -> None:
        """Commit usage evidence for these exact script and namespace identities."""
        ...


async def require_workers_paid(account_id: str) -> None:
    """Fail closed until the qualified account-reader adapter is installed."""
    raise WorkersPaidUnverified


async def refresh_workers_usage(
    installation: CloudflareInstallation, *, revision: int,
    scripts: Mapping[str, Mapping[str, str]],
) -> None:
    """Fail closed until the qualified attributable-usage adapter is installed."""
    raise WorkersUsageUnverified
