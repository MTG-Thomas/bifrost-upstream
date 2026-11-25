"""Cloudflare Workers provisioning and retirement on the qualified REST path."""

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

import httpx
from pydantic import SecretStr
from shared.models import CloudflareWorkersPolicy

from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnershipCheck,
    OwnershipConflict,
    OwnershipResource,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
    observe_owned,
)
from src.services.worker_pool.cloudflare_release import WorkerRole, WorkersRelease

API = "https://api.cloudflare.com/client/v4"
OWNER_BINDING = "BIFROST_CAPACITY_OWNER"
_ACCOUNT_ID = re.compile(r"[a-f0-9]{32}")
_CF_UUID = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")


class CloudflareManagementDenied(Exception):
    """The provider refused the scoped management credential."""


class WorkersPaidUnverified(Exception):
    """The account lacks the active Workers Paid entitlement witness."""


class WorkersWriteUnverified(Exception):
    """The candidate could not prove it can write the installed Workers."""


class CloudflareRestUnavailable(Exception):
    """A bounded REST operation can be retried under its journal fence."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _script_path(target: ResourceTarget) -> str:
    container_script = (
        target.scope == "containers"
        and re.fullmatch(rf"bifrost-{target.installation_id.hex}-r[1-9][0-9]*-containers", target.name)
    )
    if (
        target.kind != "worker_script" or (target.scope != "workers" and not container_script)
        or not _ACCOUNT_ID.fullmatch(target.account_id)
        or not re.fullmatch(r"[a-z0-9-]{1,63}", target.name)
    ):
        raise OwnershipConflict(
            "Cloudflare resource is outside the Workers cleanup contract",
            OwnershipResource.WORKER_SCRIPT, OwnershipCheck.TARGET_CONTRACT,
        )
    return f"workers/scripts/{target.name}"


def worker_owner_value(intent: ResourceIntent) -> str:
    return f"{intent.target.installation_id}:{intent.creation_effect_id}:{intent.desired_revision}:{intent.artifact_digest}"


class _RestOperation(Enum):
    ACCOUNT = (OwnershipResource.ACCOUNT, "account_read")
    ENTITLEMENTS = (OwnershipResource.ACCOUNT, "entitlements_read")
    SCRIPT_SETTINGS = (OwnershipResource.WORKER_SCRIPT, "script_settings_read")
    SCRIPT_DEPLOYMENTS = (OwnershipResource.WORKER_SCRIPT, "script_deployments_read")
    SCRIPT_UPLOAD = (OwnershipResource.WORKER_SCRIPT, "script_upload")
    SCRIPT_DELETE = (OwnershipResource.WORKER_SCRIPT, "script_delete")
    SCRIPT_SUBDOMAIN = (OwnershipResource.WORKERS_ENDPOINT, "script_subdomain")
    ACCOUNT_SUBDOMAIN = (OwnershipResource.WORKERS_ENDPOINT, "account_subdomain")
    NAMESPACES = (OwnershipResource.DURABLE_OBJECT_NAMESPACE, "namespace_inventory")
    ZONE = (OwnershipResource.DNS_ZONE, "zone_read")
    DNS = (OwnershipResource.DNS_RECORD, "dns_request")
    TUNNEL = (OwnershipResource.TUNNEL, "tunnel_request")
    TUNNEL_INGRESS = (OwnershipResource.TUNNEL_INGRESS, "tunnel_configuration")
    CONNECTOR = (OwnershipResource.CONNECTOR, "connector_inventory")
    CONNECTOR_TOKEN = (OwnershipResource.CONNECTOR_CREDENTIAL, "connector_token")
    PRIVATE_ROUTES = (OwnershipResource.PRIVATE_ROUTE, "private_route_inventory")
    CONTAINER_APPLICATION = (OwnershipResource.CONTAINER_APPLICATION, "container_application_request")


def _rest_operation(method: str, path: str) -> _RestOperation:
    if "/containers/applications" in path:
        return _RestOperation.CONTAINER_APPLICATION
    if "/workers/scripts/" in path:
        if path.endswith("/subdomain"):
            return _RestOperation.SCRIPT_SUBDOMAIN
        if path.endswith("/deployments"):
            return _RestOperation.SCRIPT_DEPLOYMENTS
        if path.endswith("/settings"):
            return _RestOperation.SCRIPT_SETTINGS
        return _RestOperation.SCRIPT_UPLOAD if method == "PUT" else _RestOperation.SCRIPT_DELETE
    if "/workers/durable_objects/" in path:
        return _RestOperation.NAMESPACES
    if path.endswith("/workers/subdomain"):
        return _RestOperation.ACCOUNT_SUBDOMAIN
    if "/dns_records" in path:
        return _RestOperation.DNS
    if "/configurations" in path:
        return _RestOperation.TUNNEL_INGRESS
    if "/connections" in path:
        return _RestOperation.CONNECTOR
    if "/cfd_tunnel" in path:
        return _RestOperation.CONNECTOR_TOKEN if path.endswith("/token") else _RestOperation.TUNNEL
    if "/teamnet/routes" in path:
        return _RestOperation.PRIVATE_ROUTES
    if path.startswith("zones/"):
        return _RestOperation.ZONE
    return _RestOperation.ENTITLEMENTS if path.endswith("/entitlements") else _RestOperation.ACCOUNT


class CloudflareRestObservation(ObservationPending):
    def __init__(
        self, message: str, operation: _RestOperation, check: ObservationCheck, *, status_code: int | None = None,
    ) -> None:
        super().__init__(message, operation.value[0], check)
        self.operation = operation
        self.status_code = status_code

    def as_result(self) -> dict[str, str]:
        return {**super().as_result(), "check": self.operation.value[1]}


class WorkersRestCleanup:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def advisory_workers_requests(
        self, account_id: str, api_token: SecretStr, scripts: set[str], start: datetime, end: datetime,
    ) -> Decimal | None:
        """Return sampled Workers counts only. Analytics never qualifies admission."""
        if not scripts or len(scripts) > 100 or start >= end:
            return None
        try:
            async with asyncio.timeout(4):
                response = await self._http.post(
                    f"{API}/graphql", headers={"Authorization": f"Bearer {api_token.get_secret_value()}"},
                    json={
                        "query": """query Advisory($account: string!, $scripts: [string!]!, $start: Time!, $end: Time!) {
                          viewer { accounts(filter: {accountTag: $account}) {
                            workersInvocationsAdaptive(limit: 100,
                              filter: {scriptName_in: $scripts, datetime_geq: $start, datetime_lt: $end}) {
                              dimensions { scriptName } sum { requests }
                            }
                          } }
                        }""",
                        "variables": {"account": account_id, "scripts": sorted(scripts),
                                      "start": start.isoformat(), "end": end.isoformat()},
                    },
                )
            if not response.is_success:
                return None
            body = response.json()
            if body.get("errors"):
                return None
            accounts = body["data"]["viewer"]["accounts"]
            if len(accounts) != 1:
                return None
            rows = accounts[0]["workersInvocationsAdaptive"]
            if not rows:
                return None
            seen = set()
            total = Decimal(0)
            for row in rows:
                name = row["dimensions"]["scriptName"]
                requests = row["sum"]["requests"]
                if name not in scripts or name in seen or type(requests) not in (float, int):
                    return None
                count = Decimal(str(requests))
                if not count.is_finite() or count < 0:
                    return None
                seen.add(name)
                total += count
            return total
        except (httpx.HTTPError, TimeoutError, AttributeError, IndexError, KeyError, TypeError, ValueError):
            return None

    async def _request(
        self, token: SecretStr, account: str, method: str, path: str, *,
        allow_missing: bool = False, params: dict[str, int | str] | None = None,
        files: dict[str, tuple[str, str | bytes, str]] | None = None,
        json_body: dict[str, Any] | None = None,
        include_http_status: bool = False,
    ) -> dict[str, Any] | None:
        resource_path = f"accounts/{account}" + (f"/{path}" if path else "")
        return await self._request_resource(
            token, method, resource_path, allow_missing=allow_missing, params=params, files=files, json_body=json_body,
            include_http_status=include_http_status,
        )

    async def _request_resource(
        self, token: SecretStr, method: str, path: str, *,
        allow_missing: bool = False, params: dict[str, int | str] | None = None,
        files: dict[str, tuple[str, str | bytes, str]] | None = None,
        json_body: dict[str, Any] | None = None,
        include_http_status: bool = False,
    ) -> dict[str, Any] | None:
        try:
            response = await self._http.request(
                method, f"{API}/{path}", params=params,
                headers={"Authorization": f"Bearer {token.get_secret_value()}"},
                files=files, json=json_body,
            )
        except httpx.HTTPError:
            raise CloudflareRestUnavailable("Cloudflare REST request could not be observed") from None
        if response.status_code in (401, 403):
            raise CloudflareManagementDenied
        if response.status_code == 404 and allow_missing:
            return None
        if response.status_code in (408, 429) or response.status_code >= 500:
            raise CloudflareRestUnavailable(
                "Cloudflare REST is temporarily unavailable", status_code=response.status_code,
            )
        if not response.is_success:
            raise CloudflareRestObservation(
                "Cloudflare REST request did not succeed", _rest_operation(method, path),
                ObservationCheck.REST_REJECTED, status_code=response.status_code,
            )
        if method == "DELETE" and response.status_code in (200, 204) and not response.content:
            # Worker delete can acknowledge with an empty body. The caller
            # still verifies script and namespace absence independently.
            return {"success": True, "result": None}
        try:
            payload = response.json()
        except ValueError:
            raise CloudflareRestObservation(
                "Cloudflare REST returned an invalid response", _rest_operation(method, path), ObservationCheck.REST_JSON,
            ) from None
        if not isinstance(payload, dict) or payload.get("success") is not True or "result" not in payload:
            raise CloudflareRestObservation(
                "Cloudflare REST returned an unconfirmed result", _rest_operation(method, path), ObservationCheck.REST_RESULT,
            )
        if include_http_status:
            payload["_http_status"] = response.status_code
        return payload

    async def verify_account(self, account_id: str, candidate: SecretStr) -> None:
        """Bind an authenticated read to the explicit account."""
        value = candidate.get_secret_value()
        if not _ACCOUNT_ID.fullmatch(account_id) or not re.fullmatch(r"[\x21-\x7e]{16,512}", value):
            raise CloudflareManagementDenied
        account = await self._request(candidate, account_id, "GET", "", allow_missing=True)
        record = account["result"] if account is not None else None
        if not isinstance(record, dict) or record.get("id") != account_id:
            raise CloudflareManagementDenied

    async def discovery_inventory(
        self, candidate: SecretStr, account_id: str | None = None,
    ) -> list[dict[str, Any]] | None:
        """List accounts, or active zones for one selected account."""
        if not re.fullmatch(r"[\x21-\x7e]{16,512}", candidate.get_secret_value()):
            raise CloudflareManagementDenied
        path = "accounts" if account_id is None else "zones"
        filters: dict[str, int | str] = {} if account_id is None else {"account.id": account_id, "status": "active"}
        entries: list[dict[str, Any]] = []
        for page in range(1, 21):
            try:
                payload = await self._request_resource(
                    candidate, "GET", path, params={**filters, "per_page": 50, "page": page},
                )
            except CloudflareManagementDenied:
                return None
            items = payload["result"] if payload else None
            if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
                raise CloudflareRestUnavailable("Cloudflare returned an invalid discovery inventory")
            info = payload.get("result_info") if payload else None
            total_pages = info.get("total_pages") if isinstance(info, dict) else None
            entries.extend(items)
            last_page = page >= total_pages if type(total_pages) is int else len(items) < 50
            if last_page:
                return entries
        raise CloudflareRestUnavailable("Cloudflare discovery inventory exceeds the supported size")

    async def verify_workers_paid(self, account_id: str, candidate: SecretStr) -> None:
        await self.verify_account(account_id, candidate)
        response = await self._request(candidate, account_id, "GET", "entitlements")
        entries = response["result"] if response is not None else None
        for feature in ("workers.enabled", "containers.enabled"):
            active = [
                entry.get("allocation") for entry in (entries if isinstance(entries, list) else ())
                if isinstance(entry, dict) and entry.get("id") == feature and entry.get("deleted_date") == ""
            ]
            if not active or any(
                not isinstance(allocation, dict) or allocation.get("type") != "bool" or allocation.get("value") is not True
                for allocation in active
            ):
                raise WorkersPaidUnverified(
                    f"Cloudflare did not report an active {feature} entitlement. "
                    "Bifrost requires both active Workers and Containers entitlements to verify Workers Paid.",
                )

    async def verify_workers_write(self, api_token: SecretStr, expected: ResourceObservation) -> None:
        """Reassert unchanged endpoint settings on an exactly owned Worker."""
        intent = expected.intent
        account = intent.target.account_id
        path = f"{_script_path(intent.target)}/subdomain"
        response = await self._request(api_token, account, "GET", path)
        before = response["result"] if response is not None else None
        if not isinstance(before, dict) or type(before.get("enabled")) is not bool or before.get("previews_enabled") is not False:
            raise WorkersWriteUnverified("Cloudflare did not confirm the owned Worker's existing endpoint settings.")
        settings = {"enabled": before["enabled"], "previews_enabled": False}
        # Cloudflare has no conditional mutation. Recheck the deployment just
        # before reasserting the observed values, then verify settings and owner.
        if await observe_owned(self, api_token=api_token, intent=intent, recorded=expected) is None:
            raise WorkersWriteUnverified("The owned Worker vanished before the write proof.")
        try:
            await self._request(api_token, account, "POST", path, json_body=settings)
        except CloudflareManagementDenied:
            raise WorkersWriteUnverified(
                "The replacement token can read an owned Worker but could not update its endpoint. "
                "Check Workers Scripts Write for the selected account.",
            ) from None
        response = await self._request(api_token, account, "GET", path)
        after = response["result"] if response is not None else None
        if not isinstance(after, dict) or any(after.get(key) is not value for key, value in settings.items()):
            raise WorkersWriteUnverified("Cloudflare did not confirm unchanged endpoint settings after the write.")
        if await observe_owned(self, api_token=api_token, intent=intent, recorded=expected) is None:
            raise WorkersWriteUnverified("The owned Worker vanished after the write proof.")

    async def observe(
        self, *, api_token: SecretStr, intent: ResourceIntent,
    ) -> ResourceObservation | None:
        target = intent.target
        path = _script_path(target)
        settings = await self._request(api_token, target.account_id, "GET", f"{path}/settings", allow_missing=True)
        if settings is None:
            return None
        record = settings["result"]
        bindings = record.get("bindings") if isinstance(record, dict) else None
        if not isinstance(bindings, list) or any(not isinstance(binding, dict) for binding in bindings):
            raise ObservationPending(
                "Cloudflare Worker binding readback is unavailable",
                OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_BINDINGS,
            )
        owners = [binding for binding in bindings if binding.get("name") == OWNER_BINDING]
        if len(owners) != 1 or owners[0].get("type") != "plain_text" or owners[0].get("text") != worker_owner_value(intent):
            raise OwnershipConflict(
                "Cloudflare Worker has a different or missing owner",
                OwnershipResource.WORKER_SCRIPT, OwnershipCheck.FOREIGN_OCCUPANT,
            )
        deployments = await self._request(
            api_token, target.account_id, "GET", f"{path}/deployments", params={"page": 1, "per_page": 1},
        )
        result = deployments["result"] if deployments is not None else None
        history = result.get("deployments") if isinstance(result, dict) else None
        if history == []:
            raise ObservationSettling(
                "Cloudflare has not listed the exactly owned Worker's deployment",
                OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_DEPLOYMENT_PENDING,
            )
        current = history[0] if isinstance(history, list) and history else None
        versions = current.get("versions") if isinstance(current, dict) else None
        deployment_id = current.get("id") if isinstance(current, dict) else None
        if (
            not isinstance(deployment_id, str) or not _CF_UUID.fullmatch(deployment_id)
            or not isinstance(versions, list) or len(versions) != 1
            or not isinstance(versions[0], dict) or versions[0].get("percentage") != 100
            or not isinstance(versions[0].get("version_id"), str)
            or not _CF_UUID.fullmatch(versions[0]["version_id"])
        ):
            raise OwnershipConflict(
                "Cloudflare Worker does not have one exact active version",
                OwnershipResource.WORKER_SCRIPT, OwnershipCheck.ACTIVE_VERSION,
            )
        return ResourceObservation(intent, deployment_id, versions[0]["version_id"])

    async def delete(self, *, api_token: SecretStr, expected: ResourceObservation) -> None:
        # observe_owned ends with active-version readback. Translate only this
        # exact receipt to the provider's unconditional DELETE by script name.
        # Cloudflare offers no conditional delete. An account-side writer can
        # race the last read, an accepted residual risk for owned script names.
        if await observe_owned(self, api_token=api_token, intent=expected.intent, recorded=expected) is not None:
            target = expected.intent.target
            await self._request(
                api_token, target.account_id, "DELETE", _script_path(target),
                allow_missing=True, params={"force": "true"},
            )

    async def _script_namespaces(
        self, token: SecretStr, target: ResourceTarget, *, namespace_ids: frozenset[str] = frozenset(),
    ) -> list[dict[str, Any]]:
        """Exhaust account pagination before trusting any script namespace inventory."""
        _script_path(target)
        seen: set[str] = set()
        owned: list[dict[str, Any]] = []
        total = None
        for page in range(1, 101):
            response = await self._request(
                token, target.account_id, "GET", "workers/durable_objects/namespaces",
                params={"page": page, "per_page": 100},
            )
            rows = response["result"] if response is not None else None
            info = response.get("result_info", {}) if response is not None else {}
            if (
                not isinstance(rows, list) or not isinstance(info, dict)
                or info.get("page", page) != page or info.get("count", len(rows)) != len(rows)
            ):
                raise ObservationPending(
                    "Cloudflare namespace inventory is incomplete",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_PAGE,
                )
            for row in rows:
                if (
                    not isinstance(row, dict) or not isinstance(row.get("id"), str)
                    or not row["id"]
                ):
                    raise ObservationPending(
                        "Cloudflare namespace inventory has invalid identity",
                        OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_IDENTITY,
                    )
                if row["id"] in seen:
                    raise ObservationSettling(
                        "Cloudflare namespace inventory repeated an identity across the snapshot",
                        OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_PAGINATION,
                    )
                seen.add(row["id"])
                # The account inventory can include namespaces with no script
                # attribution. Only an exact script or journal receipt binds one.
                if row["id"] in namespace_ids or row.get("script") == target.name:
                    owned.append(row)
            reported_total = info.get("total_count", total)
            pages = info.get("total_pages")
            if (
                reported_total is not None
                and (type(reported_total) is not int or reported_total < 0)
            ) or (pages is not None and (type(pages) is not int or pages < 0)):
                raise ObservationPending(
                    "Cloudflare namespace inventory has malformed pagination",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_PAGE,
                )
            if (
                reported_total is not None and (reported_total < len(seen) or total not in (None, reported_total))
            ) or (pages is not None and page > pages and rows):
                raise ObservationSettling(
                    "Cloudflare namespace inventory totals changed",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_PAGINATION,
                )
            total = reported_total
            more = (pages is not None and page < pages) or (total is not None and len(seen) < total)
            if not more and (pages is not None or total is not None or len(rows) < 100):
                if total is None or total == len(seen):
                    return owned
                raise ObservationSettling(
                    "Cloudflare namespace inventory ended before its advertised total",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_PAGINATION,
                )
            if not rows:
                raise ObservationSettling(
                    "Cloudflare namespace inventory ended before its advertised total",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_PAGINATION,
                )
        raise ObservationPending(
            "Cloudflare namespace inventory is incomplete",
            OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_LIMIT,
        )

    async def verify_absent(
        self, *, api_token: SecretStr, target: ResourceTarget, namespace_ids: frozenset[str] = frozenset(),
    ) -> None:
        path = _script_path(target)
        if await self._script_namespaces(api_token, target, namespace_ids=namespace_ids):
            raise DeletionSettling("Cloudflare Worker's namespace is still present")
        if await self._request(
            api_token, target.account_id, "GET", f"{path}/settings", allow_missing=True,
        ) is not None:
            raise OwnershipConflict(
                "Cloudflare Worker reappeared during absence verification",
                OwnershipResource.WORKER_SCRIPT, OwnershipCheck.REAPPEARED,
            )


@dataclass(frozen=True, slots=True)
class WorkersDeployment:
    """One release bound to an installation's public ingress and policy."""

    release: WorkersRelease
    worker_ingress_origin: str
    launcher_name: str
    capacity_name: str
    policy: CloudflareWorkersPolicy
    gateway_name: str


@dataclass(frozen=True, slots=True)
class ConfiguredWorkersScript:
    endpoint: str | None
    namespaces: dict[str, str]


class ScriptRestProvisioner(WorkersRestCleanup):
    async def _upload_script(
        self, api_token: SecretStr, intent: ResourceIntent, files: dict[str, tuple[str, str | bytes, str]],
    ) -> None:
        path = _script_path(intent.target)
        # Check immediately before the upload. REST has no conditional create,
        # so the caller must retain its effect receipt if confirmation is lost.
        if await observe_owned(self, api_token=api_token, intent=intent, recorded=None) is not None:
            raise OwnershipConflict(
                "Cloudflare Worker already exists at the intended target",
                OwnershipResource.WORKER_SCRIPT, OwnershipCheck.TARGET_OCCUPIED,
            )
        try:
            async with asyncio.timeout(30):
                payload = await self._request(api_token, intent.target.account_id, "PUT", path, files=files)
        except TimeoutError:
            raise CloudflareRestUnavailable("Cloudflare upload deadline elapsed") from None
        worker = payload["result"] if payload is not None else None
        if not isinstance(worker, dict) or worker.get("id") != intent.target.name:
            raise ObservationPending(
                "Cloudflare upload returned an unconfirmed script identity",
                OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_UPLOAD_RESULT,
            )

    async def _configure_script_endpoint(
        self, api_token: SecretStr, expected: ResourceObservation, *, exports: set[str], public_endpoint: bool,
    ) -> ConfiguredWorkersScript:
        intent = expected.intent
        path = _script_path(intent.target)
        async def require_script() -> None:
            if await observe_owned(self, api_token=api_token, intent=intent, recorded=expected) is None:
                raise ObservationSettling(
                    "Cloudflare uploaded Worker has disappeared during readback",
                    OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_CHANGED_DURING_READBACK,
                )

        await require_script()
        namespaces: dict[str, str] = {}
        for row in await self._script_namespaces(api_token, intent.target):
            if row.get("preview") is True:
                continue
            class_name = row.get("class")
            if not isinstance(class_name, str):
                raise OwnershipConflict(
                    "Cloudflare Worker namespace has no class",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.NAMESPACE_CLASS,
                )
            if class_name in namespaces:
                raise OwnershipConflict(
                    "Cloudflare Worker has ambiguous namespace bindings",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.NAMESPACE_BINDINGS,
                )
            namespaces[class_name] = row["id"]
        if namespaces.keys() - exports:
            raise ObservationPending(
                "Cloudflare namespace inventory includes unexpected exports",
                OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_EXPORTS_UNEXPECTED,
            )
        if exports - namespaces.keys():
            raise ObservationSettling(
                "Cloudflare has not listed all packaged namespace exports",
                OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_EXPORTS_PENDING,
            )
        settings = {"enabled": public_endpoint, "previews_enabled": False}
        response = await self._request(api_token, intent.target.account_id, "GET", f"{path}/subdomain", allow_missing=True)
        readback = response["result"] if response is not None else None
        if not isinstance(readback, dict) or any(readback.get(key) is not value for key, value in settings.items()):
            await require_script()
            # The update returns both authoritative settings. A separate GET
            # can still reflect the previous endpoint while it propagates.
            response = await self._request(
                api_token, intent.target.account_id, "POST", f"{path}/subdomain", json_body=settings,
            )
            confirmed = response["result"] if response is not None else None
            if not isinstance(confirmed, dict) or any(confirmed.get(key) is not value for key, value in settings.items()):
                raise ObservationPending(
                    "Cloudflare Worker endpoint update did not confirm the requested settings",
                    OwnershipResource.WORKERS_ENDPOINT, ObservationCheck.ENDPOINT_UPDATE,
                )
        await require_script()
        if not public_endpoint:
            return ConfiguredWorkersScript(None, namespaces)
        response = await self._request(
            api_token, intent.target.account_id, "GET", "workers/subdomain",
        )
        account = response["result"] if response is not None else None
        subdomain = account.get("subdomain") if isinstance(account, dict) else None
        if not isinstance(subdomain, str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", subdomain):
            raise ObservationPending(
                "Cloudflare account's Workers subdomain could not be verified",
                OwnershipResource.WORKERS_ENDPOINT, ObservationCheck.ACCOUNT_SUBDOMAIN,
            )
        return ConfiguredWorkersScript(f"https://{intent.target.name}.{subdomain}.workers.dev", namespaces)


class WorkersRestProvisioner(ScriptRestProvisioner):
    def __init__(self, http: httpx.AsyncClient, deployment: WorkersDeployment) -> None:
        super().__init__(http)
        self._deployment = deployment

    def _worker_role(self, intent: ResourceIntent) -> WorkerRole:
        if intent.artifact_digest != self._deployment.release.digest:
            raise OwnershipConflict(
                "Cloudflare upload intent is bound to a different release",
                OwnershipResource.WORKERS_RELEASE, OwnershipCheck.RELEASE_INTENT,
            )
        if intent.target.name == self._deployment.launcher_name:
            return "launcher"
        if intent.target.name == self._deployment.capacity_name:
            return "capacity"
        if intent.target.name == self._deployment.gateway_name:
            return "gateway"
        raise OwnershipConflict(
            "Cloudflare upload intent has an unknown deployment target",
            OwnershipResource.WORKER_SCRIPT, OwnershipCheck.TARGET_BINDING,
        )

    def _render_upload(
        self, role: WorkerRole, intent: ResourceIntent, bearer: SecretStr | None,
    ) -> dict[str, tuple[str, str | bytes, str]]:
        deployment = self._deployment
        manifest = deployment.release.manifest
        artifact = manifest.workers[role]
        if (role == "capacity") != (bearer is not None):
            raise OwnershipConflict(
                "Only the capacity Worker must receive the scope bearer",
                OwnershipResource.WORKERS_SCOPE, OwnershipCheck.SCOPE_BEARER,
            )
        values = {
            "worker_ingress_origin": deployment.worker_ingress_origin,
            "launcher_cap": str(deployment.policy.launcher_cap),
            "max_lifetime_seconds": str(deployment.policy.max_lifetime_seconds),
            "slots_per_launcher": str(deployment.policy.slots_per_launcher),
            "linger_seconds": str(deployment.policy.linger_seconds),
            "worker_version": deployment.release.digest,
        }
        if deployment.policy.slots_per_launcher > manifest.max_slots_per_launcher:
            raise OwnershipConflict(
                "Cloudflare release supports fewer slots than this policy",
                OwnershipResource.WORKERS_RELEASE, OwnershipCheck.SLOT_POLICY,
            )
        bindings = []
        for binding in artifact.bindings:
            value = binding.model_dump(exclude_none=True, exclude={"source", "script"})
            if binding.type == "secret_text":
                if role != "capacity" or binding.name != "CAPACITY_TOKEN" or binding.source != "scope_bearer" or bearer is None:
                    raise OwnershipConflict(
                        "Cloudflare release declares an invalid secret binding",
                        OwnershipResource.WORKERS_RELEASE, OwnershipCheck.SECRET_BINDING,
                    )
                value["text"] = bearer.get_secret_value()
            elif binding.source is not None:
                if binding.type != "plain_text" or binding.source not in values:
                    raise OwnershipConflict(
                        "Cloudflare release declares an invalid dynamic binding",
                        OwnershipResource.WORKERS_RELEASE, OwnershipCheck.DYNAMIC_BINDING,
                    )
                value["text"] = values[binding.source]
            if binding.script is not None:
                if (role, binding.type, binding.script) == ("capacity", "durable_object_namespace", "launcher"):
                    value["script_name"] = deployment.launcher_name
                elif (role, binding.type, binding.script) == ("launcher", "service", "gateway"):
                    value.update(service=deployment.gateway_name, environment="production")
                else:
                    raise OwnershipConflict(
                        "Cloudflare release declares an invalid cross-script binding",
                        OwnershipResource.WORKERS_RELEASE, OwnershipCheck.CROSS_SCRIPT_BINDING,
                    )
            bindings.append(value)
        bindings.append({"name": OWNER_BINDING, "type": "plain_text", "text": worker_owner_value(intent)})
        if len({row["name"] for row in bindings}) != len(bindings):
            raise OwnershipConflict(
                "Cloudflare release contains duplicate bindings",
                OwnershipResource.WORKERS_RELEASE, OwnershipCheck.DUPLICATE_BINDING,
            )
        metadata = {
            "main_module": artifact.main_module,
            "compatibility_date": manifest.compatibility_date.isoformat(),
            "compatibility_flags": list(manifest.compatibility_flags),
            "exports": {name: export.model_dump() for name, export in artifact.exports.items()},
            "bindings": bindings,
            "limits": {"cpu_ms": artifact.cpu_ms},
            "observability": {"enabled": False},
            "logpush": False,
            "keep_bindings": [],
        }
        return {
            "metadata": ("metadata.json", json.dumps(metadata), "application/json"),
            **{
                module.name: (module.name, deployment.release.modules[role][module.name], module.mime_type)
                for module in artifact.modules
            },
        }

    async def create(
        self, *, api_token: SecretStr, intent: ResourceIntent, scope_bearer: SecretStr | None,
    ) -> None:
        role = self._worker_role(intent)
        files = self._render_upload(role, intent, scope_bearer)
        await self._upload_script(api_token, intent, files)

    async def configure_endpoint(
        self, *, api_token: SecretStr, expected: ResourceObservation,
    ) -> ConfiguredWorkersScript:
        """Verify the exported classes and finish an uploaded deployment."""
        role = self._worker_role(expected.intent)
        return await self._configure_script_endpoint(
            api_token, expected, exports=set(self._deployment.release.manifest.workers[role].exports),
            public_endpoint=role == "capacity",
        )
