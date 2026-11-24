"""CLI mirror of the Cloudflare capacity action request."""

from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class CloudflareCapacityAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    expected_revision: int = Field(gt=0)


class CloudflareReconcileAction(CloudflareCapacityAction):
    reset_release_fence: bool = Field(default=False, description="Explicitly accept a conflicting or older packaged Workers release.")


class CloudflarePolicyFence(CloudflareCapacityAction):
    expected_policy_version: int = Field(gt=0, description="Policy version reviewed in Cloudflare Capacity status.")


class CloudflareAccountTokenRotation(CloudflareCapacityAction):
    model_config = ConfigDict(hide_input_in_errors=True)

    api_token: SecretStr = Field(min_length=16, max_length=512, json_schema_extra={"writeOnly": True})


class CloudflarePreflightRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    api_token: SecretStr | None = None
    credential_ref: UUID | None = None
    account_id: str | None = None
    zone_id: str | None = None
    reachability_mode: Literal["existing_https_endpoint", "managed_tunnel"] | None = None
    worker_ingress_origin: str | None = None
    policy: dict[str, Any] | None = None
    containers: bool | None = None
    container_instance_hours: int | None = Field(default=None, ge=1, le=1488)
    container_qualification_canaries: int | None = Field(default=None, ge=0, le=1000)

    def request_body(self) -> dict[str, Any]:
        body = self.model_dump(mode="json", exclude_none=True, exclude={"api_token"})
        if self.api_token is not None:
            body["api_token"] = self.api_token.get_secret_value()
        return body


class CloudflareActivateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    api_token: SecretStr | None = None
    credential_ref: UUID | None = None
    preflight_receipt: str
    consent_version: str
    consent: Literal[True]
    monthly_limit_usd: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
