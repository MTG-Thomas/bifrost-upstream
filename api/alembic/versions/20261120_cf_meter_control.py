"""Retain pre-dispatch tenant host reservations for receipt rate v4.

The new rate separates cadence-priced control traffic and observed workload
counts. Existing incarnations retain their deployed rate and bounds.
"""

import sqlalchemy as sa
from alembic import op

from shared.migration_policy import refuse_populated_downgrade

revision = "20261120_cf_meter_control"
down_revision = "20261120_cf_launcher_meter"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("tenant_host_seconds", "tenant_dispatches"):
        op.add_column("cloudflare_usage_activity", sa.Column(name, sa.BigInteger(), nullable=False, server_default="0"))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_usage_activity")
    for name in ("tenant_dispatches", "tenant_host_seconds"):
        op.drop_column("cloudflare_usage_activity", name)
