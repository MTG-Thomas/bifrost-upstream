"""CLI mirror of the Cloudflare capacity action request."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CloudflareCapacityAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    expected_revision: int = Field(gt=0)
