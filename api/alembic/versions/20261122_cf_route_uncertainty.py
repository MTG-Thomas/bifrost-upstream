"""Keep attempt uncertainty on the foreground booking."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261122_cf_route_uncertainty"
down_revision = "20261122_cf_workers_deadline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_dispatch_meters", sa.Column(
        "attempt_uncertain", sa.Boolean(), nullable=False, server_default=sa.false(),
    ))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_dispatch_meters")
    op.drop_column("cloudflare_dispatch_meters", "attempt_uncertain")
