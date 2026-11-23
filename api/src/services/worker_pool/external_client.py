"""Authenticated, idempotent physical-capacity protocol; never a job transport."""

from collections.abc import Awaitable, Callable
from datetime import datetime
from enum import Enum
from uuid import UUID

import httpx
from pydantic import ValidationError
from shared.models import (
    WorkerCapacityProfile,
    WorkerCapacityReservation,
    WorkerCapacityResource,
)

CAPACITY_REQUEST_SECONDS = 10


class CapacityProtocolCheck(Enum):
    UNCLASSIFIED = ("capacity_provider", "protocol", "invalid", "valid")
    PROFILE = ("workers_scope", "capabilities_shape", "invalid", "valid_profile")
    RESOURCE_SHAPE = ("canary_launcher", "resource_shape", "invalid", "valid_resource")
    RESOURCE_IDENTITY = ("canary_launcher", "resource_identity", "different", "exact_reservation")
    PREPARE = ("canary_launcher", "prepare_receipt", "wrong_state_or_release", "prepared_matching_release")
    START = ("canary_launcher", "start_receipt", "wrong_state", "starting_or_running")
    REMOVE = ("canary_launcher", "delete_receipt", "wrong_state", "deleted")
    COMPACT = ("canary_launcher", "receipt_compaction", "unconfirmed", "compacted")
    INGRESS_ORIGIN = ("workers_scope", "canary_origin", "different", "approved_origin")
    PROFILE_CONTRACT = ("workers_scope", "capabilities_contract", "different", "installed_release_policy")
    PROFILE_HTTP = ("workers_scope", "capabilities_request", "rejected", "successful_response")
    PREPARE_HTTP = ("canary_launcher", "prepare_request", "rejected", "successful_response")
    START_HTTP = ("canary_launcher", "start_request", "rejected", "successful_response")


class CapacityProtocolError(RuntimeError):
    """Provider response cannot be bound to the requested worker resource."""

    def __init__(self, message: str, check: CapacityProtocolCheck = CapacityProtocolCheck.UNCLASSIFIED) -> None:
        super().__init__(message)
        self.check = check

    def as_result(self) -> dict[str, str]:
        resource, check, observed, expected = self.check.value
        return {
            "resource_class": resource,
            "check": check,
            "observed_category": observed,
            "expected_category": expected,
        }


class CapacityProviderUnavailable(RuntimeError):
    """A policy edit cannot verify the configured provider's current limits."""


class ExternalCapacityClient:
    """No implicit retries or redirects after an uncertain provider mutation.

    Every mutation addresses the same canonical UUID. The reconciler reads that
    resource before deciding what remains, and starts it only after its UID has
    been persisted. A provider must make preparation/start/delete idempotent and
    must never restart an incarnation that has already stopped.
    """

    def __init__(
        self, endpoint: str, token: str, *, http: httpx.AsyncClient,
        reserve_request: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.endpoint = self.check_configuration(endpoint, token)
        self._token = token
        self.http = http
        self._reserve_request = reserve_request

    @staticmethod
    def check_configuration(endpoint: str, token: str) -> str:
        try:
            url = httpx.URL(endpoint)
        except httpx.InvalidURL:
            raise ValueError("Capacity provider requires a valid HTTPS endpoint") from None
        if (
            url.scheme != "https" or not url.host or url.username or url.password
            or url.query or url.fragment
        ):
            raise ValueError("Capacity provider requires an HTTPS origin/path without credentials or query")
        if len(token) < 32 or len(token) > 512 or not token.isascii() or any(char.isspace() for char in token):
            raise ValueError("Capacity provider requires a dedicated bearer token")
        return str(url).rstrip("/")

    async def _request(self, method: str, path: str, *, scope: str | None = None, body=None) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self._token}"}
        if scope is not None:
            headers["X-Bifrost-Capacity-Scope"] = scope
        if self._reserve_request is not None:
            await self._reserve_request()
        return await self.http.request(
            method, f"{self.endpoint}/v1/{path}", headers=headers, json=body,
            timeout=CAPACITY_REQUEST_SECONDS, follow_redirects=False,
        )

    async def profile(self) -> WorkerCapacityProfile:
        response = await self._request("GET", "capabilities")
        response.raise_for_status()
        try:
            return WorkerCapacityProfile.model_validate_json(response.content)
        except ValidationError:
            raise CapacityProtocolError("Invalid capacity provider capabilities", CapacityProtocolCheck.PROFILE) from None

    @staticmethod
    def _resource(response: httpx.Response, worker_id: UUID, scope: str, uid: str | None = None) -> WorkerCapacityResource:
        response.raise_for_status()
        try:
            resource = WorkerCapacityResource.model_validate_json(response.content)
        except ValidationError:
            raise CapacityProtocolError(
                "Invalid capacity provider resource response", CapacityProtocolCheck.RESOURCE_SHAPE,
            ) from None
        if resource.worker_id != worker_id or resource.scope != scope or (uid is not None and resource.resource_uid != uid):
            raise CapacityProtocolError(
                "Capacity resource identity does not match its reservation", CapacityProtocolCheck.RESOURCE_IDENTITY,
            )
        return resource

    async def get(self, worker_id: UUID, scope: str) -> WorkerCapacityResource | None:
        response = await self._request("GET", f"workers/{worker_id}", scope=scope)
        if response.status_code == 404:
            return None
        return self._resource(response, worker_id, scope)

    async def prepare(
        self, reservation: WorkerCapacityReservation, scope: str, *,
        managed_identity: tuple[str, str] | None = None,
        managed_deadline: datetime | None = None,
    ) -> WorkerCapacityResource:
        body = reservation.model_dump(mode="json")
        uid = None
        if managed_identity is not None:
            uid, key = managed_identity
            body.update(resource_uid=uid, control_key=key)
            if managed_deadline is not None:
                body["deadline_ms"] = int(managed_deadline.timestamp() * 1000)
        response = await self._request(
            "PUT", f"workers/{reservation.worker_id}", scope=scope,
            body=body,
        )
        resource = self._resource(response, reservation.worker_id, scope, uid)
        if resource.state != "prepared" or resource.worker_version != reservation.worker_version:
            raise CapacityProtocolError(
                "A new capacity reservation must be prepared before starting", CapacityProtocolCheck.PREPARE,
            )
        return resource

    async def start(self, worker_id: UUID, scope: str, uid: str) -> WorkerCapacityResource:
        response = await self._request(
            "POST", f"workers/{worker_id}/start", scope=scope, body={"resource_uid": uid},
        )
        resource = self._resource(response, worker_id, scope, uid)
        if resource.state not in ("starting", "running"):
            raise CapacityProtocolError("Provider did not start the bound capacity resource", CapacityProtocolCheck.START)
        return resource

    async def remove(self, worker_id: UUID, scope: str, uid: str) -> WorkerCapacityResource:
        response = await self._request(
            "DELETE", f"workers/{worker_id}", scope=scope, body={"resource_uid": uid},
        )
        resource = self._resource(response, worker_id, scope, uid)
        if resource.state != "deleted":
            raise CapacityProtocolError("Provider did not delete the bound capacity resource", CapacityProtocolCheck.REMOVE)
        return resource

    async def acknowledge_receipt(self, worker_id: UUID, scope: str, uid: str) -> None:
        response = await self._request(
            "DELETE", f"workers/{worker_id}", scope=scope, body={"resource_uid": uid, "receipt_ack": True},
        )
        resource = self._resource(response, worker_id, scope, uid)
        if resource.state != "deleted" or resource.usage is not None:
            raise CapacityProtocolError(
                "Capacity provider did not compact its deletion receipt", CapacityProtocolCheck.COMPACT,
            )
