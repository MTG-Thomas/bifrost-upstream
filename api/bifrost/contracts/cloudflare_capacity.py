"""CLI mirror of the Cloudflare capacity action request."""

from uuid import UUID

from pydantic import BaseModel, Field


class CloudflareCapacityAction(BaseModel):
    installation_id: UUID
    expected_revision: int = Field(gt=0)
