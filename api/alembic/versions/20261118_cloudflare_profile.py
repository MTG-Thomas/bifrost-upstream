"""Record each executor's runtime profile and a dispatch's Cloudflare eligibility.

Additive only. Every existing executor keeps the Linux process runtime, and
every existing or ordinarily staged dispatch carries no Cloudflare marker, so
it stays Linux-only.

Revision ID: 20261118_cloudflare_profile
Revises: 20261117_integ_secret_encrypt
Create Date: 2026-11-18
"""

from alembic import op
import sqlalchemy as sa


revision = "20261118_cloudflare_profile"
down_revision = "20261117_integ_secret_encrypt"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "execution_workers",
        sa.Column("runtime_profile", sa.String(64), nullable=False,
                  server_default=sa.text("'linux-process-v1'")),
    )
    op.add_column(
        "work_dispatches",
        sa.Column("cloudflare_revision", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("work_dispatches", "cloudflare_revision")
    op.drop_column("execution_workers", "runtime_profile")
