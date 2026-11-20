"""CLI mirror of the Cloudflare capacity action request."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class CloudflareCapacityAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    expected_revision: int = Field(gt=0)


class CloudflareAccountTokenRotation(CloudflareCapacityAction):
    api_token: SecretStr = Field(min_length=16, max_length=512, json_schema_extra={"writeOnly": True})
