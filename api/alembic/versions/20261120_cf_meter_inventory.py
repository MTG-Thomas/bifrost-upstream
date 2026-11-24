"""Store inventory-rated reservations and capacity request units independently of prior rates."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261120_cf_meter_inventory"
down_revision = "20261120_cf_meter_control"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cloudflare_capacity_scopes",
        sa.Column("capacity_cpu_ms", sa.Integer(), nullable=False, server_default="30000"),
    )
    for name in ("inventory_tenant_seconds", "inventory_dispatches", "inventory_warm_pings", "capacity_requests", "capacity_cpu_ms"):
        op.add_column("cloudflare_usage_activity", sa.Column(name, sa.BigInteger(), nullable=False, server_default="0"))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_usage_activity", "cloudflare_capacity_scopes")
    for name in ("capacity_cpu_ms", "capacity_requests", "inventory_warm_pings", "inventory_dispatches", "inventory_tenant_seconds"):
        op.drop_column("cloudflare_usage_activity", name)
    op.drop_column("cloudflare_capacity_scopes", "capacity_cpu_ms")
