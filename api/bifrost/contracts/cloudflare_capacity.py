"""CLI mirror of the Cloudflare capacity action request."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class CloudflareCapacityAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    expected_revision: int = Field(gt=0)


class CloudflareReconcileAction(CloudflareCapacityAction):
    reset_release_fence: bool = Field(default=False, description="Explicitly accept a conflicting or older packaged Workers release.")


class CloudflareAccountTokenRotation(CloudflareCapacityAction):
    model_config = ConfigDict(hide_input_in_errors=True)

    api_token: SecretStr = Field(min_length=16, max_length=512, json_schema_extra={"writeOnly": True})
