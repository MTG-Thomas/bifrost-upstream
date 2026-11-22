"""Store pending reviews alongside, and outside the history of, grants."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_pending_reviews"
down_revision = "20261121_cf_containers"
branch_labels = None
depends_on = None

_TABLE = "cloudflare_workflow_approvals"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("is_review", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.alter_column(_TABLE, "accepted_by", nullable=True)
    op.create_check_constraint("ck_cloudflare_review_inactive", _TABLE, "NOT (is_review AND is_current)")
    op.create_index("uq_cloudflare_pending_review", _TABLE, ["workflow_id"], unique=True, postgresql_where=sa.text("is_review"))


def downgrade() -> None:
    op.execute(sa.text(f"DELETE FROM {_TABLE} WHERE is_review"))
    op.drop_index("uq_cloudflare_pending_review", table_name=_TABLE)
    op.drop_constraint("ck_cloudflare_review_inactive", _TABLE, type_="check")
    op.alter_column(_TABLE, "accepted_by", nullable=False)
    op.drop_column(_TABLE, "is_review")
