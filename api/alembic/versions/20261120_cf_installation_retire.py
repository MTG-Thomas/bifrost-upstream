"""Retain retired installations and bind the current account token."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_installation_retire"
down_revision = "20261120_cf_capacity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "cloudflare_installations"
    op.add_column(table, sa.Column("retired_at", sa.DateTime(timezone=True)))
    op.add_column(table, sa.Column("api_token_purpose", sa.String(16), nullable=False, server_default="account_token"))
    op.add_column(table, sa.Column("api_token_scope", sa.String(64), nullable=False, server_default=""))
    op.add_column(table, sa.Column("api_token_version", sa.Integer()))
    op.execute("""
        UPDATE cloudflare_installations AS installation
        SET api_token_version = 1
        WHERE EXISTS (
            SELECT 1 FROM cloudflare_capacity_credentials AS credential
            WHERE credential.installation_id = installation.id
              AND credential.purpose = 'account_token' AND credential.scope = ''
              AND credential.version = 1 AND credential.ciphertext IS NOT NULL
        )
    """)
    op.create_check_constraint("ck_cf_account_token_binding", table, "api_token_purpose = 'account_token' AND api_token_scope = ''")
    op.create_check_constraint(
        "ck_cf_retired_installation", table,
        "retired_at IS NULL OR (desired_state = 'disconnected' AND api_token_version IS NULL)",
    )
    op.create_foreign_key(
        "fk_cf_active_account_token", table, "cloudflare_capacity_credentials",
        ["id", "api_token_purpose", "api_token_scope", "api_token_version"],
        ["installation_id", "purpose", "scope", "version"],
        deferrable=True, initially="DEFERRED",
    )
    op.create_index("uq_cf_active_installation", table, ["singleton"], unique=True, postgresql_where=sa.text("retired_at IS NULL"))
    op.drop_constraint("cloudflare_installations_singleton_key", table, type_="unique")


def downgrade() -> None:
    table = "cloudflare_installations"
    # Downgrade refuses a second installation because the old schema cannot
    # retain more than one receipt.
    op.create_unique_constraint("cloudflare_installations_singleton_key", table, ["singleton"])
    op.drop_index("uq_cf_active_installation", table_name=table)
    op.drop_constraint("fk_cf_active_account_token", table, type_="foreignkey")
    op.drop_constraint("ck_cf_retired_installation", table, type_="check")
    op.drop_constraint("ck_cf_account_token_binding", table, type_="check")
    for column in ("api_token_version", "api_token_scope", "api_token_purpose", "retired_at"):
        op.drop_column(table, column)
