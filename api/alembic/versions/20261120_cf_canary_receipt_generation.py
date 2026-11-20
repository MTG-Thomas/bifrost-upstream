"""Fence scoped SDK receipts and ACKs to the same launcher session.

Historical receipts keep their timestamps. They cannot authorize an ACK
because their generation was never recorded.

Existing policies retain workers-paid-2026-11-20. The calibrated rate
version uses the legacy estimate for the calendar month in which an
existing operator applies a newly qualified release.
"""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_canary_gen"
down_revision = "20261120_cf_usage_only_envelope"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "cloudflare_worker_reservations"
    op.add_column(table, sa.Column("canary_sdk_generation", sa.Integer()))
    op.create_check_constraint(
        "ck_cf_canary_sdk_generation", table,
        "canary_sdk_generation IS NULL OR (canary_sdk_generation > 0 AND canary_sdk_at IS NOT NULL)",
    )


def downgrade() -> None:
    table = "cloudflare_worker_reservations"
    op.drop_constraint("ck_cf_canary_sdk_generation", table, type_="check")
    op.drop_column(table, "canary_sdk_generation")
