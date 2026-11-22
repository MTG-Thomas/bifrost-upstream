"""Internal policy and gross reservation pricing for the Linux scope."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    DateTime,
    Interval,
    Numeric,
    cast,
    exists,
    extract,
    func,
    literal,
    select,
)
from sqlalchemy.orm import aliased

from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareWorkerReservation,
)
from src.services.worker_pool.cloudflare_containers_release import ContainerRates

CONTAINERS_SCOPE = "containers"


def container_identity_matches(worker_id, release: str | None):
    """Keep the legacy Linux control credential away from managed incarnations."""
    pin = CloudflareWorkerReservation
    managed = exists().where(pin.worker_id == worker_id, pin.container_reserved_at.is_not(None))
    if release is None:
        return ~managed
    return managed.where(
        pin.scope == CONTAINERS_SCOPE, pin.container_release == release, pin.container_stopped_at.is_(None),
        container_deadline_sql() > func.clock_timestamp(),
    )


class ContainersPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    instance_type: Literal["standard-1"] = "standard-1"
    installation_instances: int = Field(default=2, ge=1, le=2)
    tenant_instances: Literal[1] = 1
    workflow_slots: Literal[1] = 1
    idle_seconds: int = Field(default=120, ge=1, le=120)
    # Required on explicit enablement. The pilot did not set defaults for
    # execution duration, ingress byte budgets or queue envelopes.
    start_timeout_seconds: int = Field(ge=1, le=3600)
    lifetime_seconds: int = Field(ge=121, le=86400)
    queue_wait_seconds: int = Field(ge=1, le=3600)
    queue_depth: int = Field(ge=1, le=1000)
    ingress_bytes: int = Field(ge=1, le=10**9)
    outbound_bytes: int = Field(ge=1, le=10**9)


def container_envelope(policy: ContainersPolicy, rates: ContainerRates) -> dict[str, str | int]:
    return {
        "version": rates.version,
        "seconds": policy.start_timeout_seconds + policy.lifetime_seconds + rates.cleanup_seconds,
        "second_usd": rates.instance_second_usd,
        "fixed_usd": str(rates.control_usd + Decimal(rates.outbound_gb_usd) * policy.outbound_bytes / 10**9),
        "control_usd": str(rates.control_usd),
        "outbound_gb_usd": rates.outbound_gb_usd,
        "outbound_bytes": policy.outbound_bytes,
    }


def fleet_envelope(policy: ContainersPolicy, rates: ContainerRates, month_seconds: int) -> Decimal:
    # Use full utilization plus one overlapping horizon across the boundary.
    # For fixed reserves, count repeated starts even if each stops immediately.
    envelope = container_envelope(policy, rates)
    horizon = int(envelope["seconds"])
    fixed = Decimal(envelope["fixed_usd"])
    return policy.installation_instances * (
        Decimal(rates.instance_second_usd) * (month_seconds + horizon) + fixed * (month_seconds + 1)
    )


def container_account_usage(now):
    """Cap unsettled exposure at the deadline and price witnessed runtime."""
    reservation = CloudflareWorkerReservation
    owner = aliased(CloudflareInstallation)
    period = func.date_trunc("month", now, "UTC")
    end = func.timezone("UTC", func.timezone("UTC", period) + literal("1 month").cast(Interval))
    envelope = reservation.container_envelope
    reserve_end = container_deadline_sql()
    charged_end = func.coalesce(reservation.container_stopped_at, reserve_end)
    charged_start = func.coalesce(
        cast(envelope["observed_started_at"].as_string(), DateTime(timezone=True)), reservation.container_reserved_at,
    )
    seconds = func.greatest(0, extract(
        "epoch", func.least(end, charged_end) - func.greatest(period, charged_start),
    ))
    return select(func.coalesce(func.sum(
        seconds * envelope["second_usd"].as_string().cast(Numeric(30, 12))
        + func.coalesce(
            envelope["settled_fixed_usd"].as_string().cast(Numeric(30, 12)),
            envelope["fixed_usd"].as_string().cast(Numeric(30, 12)),
        ),
    ), 0)).select_from(reservation).join(
        owner, owner.id == reservation.installation_id,
    ).where(
        owner.account_id == CloudflareInstallation.account_id,
        reservation.container_reserved_at.is_not(None),
        reservation.container_reserved_at < end,
        charged_start < end,
        charged_end > period,
    ).correlate(CloudflareInstallation).scalar_subquery()


def container_deadline_sql():
    reservation = CloudflareWorkerReservation
    return reservation.container_reserved_at + func.make_interval(
        0, 0, 0, 0, 0, 0, reservation.container_envelope["seconds"].as_integer(),
    )


def container_reserve_cost(
    envelope: dict[str, str | int], now: datetime, start: datetime, end: datetime,
) -> Decimal:
    until = now + timedelta(seconds=int(envelope["seconds"]))
    if now >= end or until <= start:
        return Decimal(0)
    elapsed = min(until, end) - max(start, now)
    seconds = Decimal(elapsed.days * 86400 + elapsed.seconds) + Decimal(elapsed.microseconds) / 10**6
    return seconds * Decimal(envelope["second_usd"]) + Decimal(envelope["fixed_usd"])
