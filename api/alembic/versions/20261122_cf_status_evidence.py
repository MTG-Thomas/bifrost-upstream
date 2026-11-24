"""Record redacted Cloudflare deployment evidence."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261122_cf_status_evidence"
down_revision = "20261122_cf_route_uncertainty"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column("deployment_evidence", JSONB()))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_installations")
    op.drop_column("cloudflare_installations", "deployment_evidence")
