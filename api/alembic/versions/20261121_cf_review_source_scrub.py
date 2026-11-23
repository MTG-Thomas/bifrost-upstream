"""Remove source text and temporary diagnostics from stored reviews."""

from alembic import op

revision = "20261121_cf_review_source_scrub"
down_revision = "20261121_cf_pending_reviews"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE cloudflare_workflow_approvals
        SET declaration = declaration - 'sources'
        WHERE is_review AND declaration ? 'sources'
    """)
    op.execute("""
        DELETE FROM cloudflare_workflow_approvals
        WHERE is_review AND declaration->>'status' = 'temporarily_unavailable'
    """)


def downgrade() -> None:
    # Reviews can be regenerated from the current source.
    pass
