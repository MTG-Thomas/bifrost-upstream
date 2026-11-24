"""Join Cloudflare capacity and identity provisioning migration heads."""

revision = "20261123_merge_cf_identity"
down_revision = ("20261117_idp_access_pending", "20261123_cf_merge_scope_heads")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
