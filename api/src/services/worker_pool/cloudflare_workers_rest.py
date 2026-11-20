"""Cloudflare Workers provisioning and retirement on the qualified REST path."""

import json
import re
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import SecretStr
from shared.models import CloudflareWorkersPolicy

from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
    observe_owned,
)
from src.services.worker_pool.cloudflare_release import WorkerRole, WorkersRelease

API = "https://api.cloudflare.com/client/v4"
OWNER_BINDING = "BIFROST_CAPACITY_OWNER"
_CF_UUID = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")


class CloudflareManagementDenied(Exception):
    """The provider refused the scoped management credential."""


class CloudflareRestUnavailable(Exception):
    """A bounded REST operation can be retried under its journal fence."""


def _script_path(target: ResourceTarget) -> str:
    if (
        target.kind != "worker_script" or target.scope != "workers"
        or not re.fullmatch(r"[a-f0-9]{32}", target.account_id)
        or not re.fullmatch(r"[a-z0-9-]{1,63}", target.name)
    ):
        raise OwnershipConflict("Cloudflare resource is outside the Workers cleanup contract")
    return f"workers/scripts/{target.name}"


def worker_owner_value(intent: ResourceIntent) -> str:
    return f"{intent.target.installation_id}:{intent.creation_effect_id}:{intent.desired_revision}:{intent.artifact_digest}"


class WorkersRestCleanup:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def _request(
        self, token: SecretStr, account: str, method: str, path: str, *,
        allow_missing: bool = False, params: dict[str, int | str] | None = None,
        files: dict[str, tuple[str, str | bytes, str]] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        try:
            response = await self._http.request(
                method, f"{API}/accounts/{account}/{path}", params=params,
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
            raise CloudflareRestUnavailable("Cloudflare REST is temporarily unavailable")
        if not response.is_success:
            raise ObservationPending("Cloudflare REST request did not succeed")
        try:
            payload = response.json()
        except ValueError:
            raise ObservationPending("Cloudflare REST returned an invalid response") from None
        if not isinstance(payload, dict) or payload.get("success") is not True or "result" not in payload:
            raise ObservationPending("Cloudflare REST returned an unconfirmed result")
        return payload

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
            raise ObservationPending("Cloudflare Worker binding readback is unavailable")
        owners = [binding for binding in bindings if binding.get("name") == OWNER_BINDING]
        if len(owners) != 1 or owners[0].get("type") != "plain_text" or owners[0].get("text") != worker_owner_value(intent):
            raise OwnershipConflict("Cloudflare Worker has a different or missing owner")
        deployments = await self._request(
            api_token, target.account_id, "GET", f"{path}/deployments", params={"page": 1, "per_page": 1},
        )
        result = deployments["result"] if deployments is not None else None
        history = result.get("deployments") if isinstance(result, dict) else None
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
            raise OwnershipConflict("Cloudflare Worker does not have one exact active version")
        return ResourceObservation(intent, deployment_id, versions[0]["version_id"])

    async def delete(self, *, api_token: SecretStr, expected: ResourceObservation) -> None:
        # Workers DELETE is by script name. Account-side writers outside the
        # installation lease can still race this last ownership check.
        if await observe_owned(self, api_token=api_token, intent=expected.intent, recorded=expected) is not None:
            target = expected.intent.target
            await self._request(
                api_token, target.account_id, "DELETE", _script_path(target),
                allow_missing=True, params={"force": "true"},
            )

    async def verify_absent(self, *, api_token: SecretStr, target: ResourceTarget) -> None:
        path = _script_path(target)
        seen: set[str] = set()
        total = None
        for page in range(1, 101):
            response = await self._request(
                api_token, target.account_id, "GET", "workers/durable_objects/namespaces",
                params={"page": page, "per_page": 100},
            )
            rows = response["result"] if response is not None else None
            info = response.get("result_info", {}) if response is not None else {}
            if (
                not isinstance(rows, list) or not isinstance(info, dict)
                or info.get("page", page) != page or info.get("count", len(rows)) != len(rows)
            ):
                raise ObservationPending("Cloudflare namespace inventory is incomplete")
            for row in rows:
                if (
                    not isinstance(row, dict) or not isinstance(row.get("script"), str)
                    or not isinstance(row.get("id"), str) or not row["id"] or row["id"] in seen
                ):
                    raise ObservationPending("Cloudflare namespace inventory has invalid identity")
                seen.add(row["id"])
                if row["script"] == target.name:
                    raise ObservationPending("Cloudflare Worker's namespace is still present")
            reported_total = info.get("total_count", total)
            pages = info.get("total_pages")
            if (
                reported_total is not None
                and (type(reported_total) is not int or reported_total < len(seen) or total not in (None, reported_total))
            ) or (
                pages is not None and (type(pages) is not int or pages < 0 or (page > pages and rows))
            ):
                raise ObservationPending("Cloudflare namespace inventory totals changed")
            total = reported_total
            more = (pages is not None and page < pages) or (total is not None and len(seen) < total)
            if not more and (pages is not None or total is not None or len(rows) < 100):
                if total is None or total == len(seen):
                    if await self._request(
                        api_token, target.account_id, "GET", f"{path}/settings", allow_missing=True,
                    ) is not None:
                        raise OwnershipConflict("Cloudflare Worker reappeared during absence verification")
                    return
                break
            if not rows:
                break
        raise ObservationPending("Cloudflare namespace inventory is incomplete")


@dataclass(frozen=True, slots=True)
class WorkersDeployment:
    """One release bound to an installation's public ingress and policy."""

    release: WorkersRelease
    worker_ingress_origin: str
    launcher_name: str
    capacity_name: str
    policy: CloudflareWorkersPolicy


class WorkersRestProvisioner(WorkersRestCleanup):
    def __init__(self, http: httpx.AsyncClient, deployment: WorkersDeployment) -> None:
        super().__init__(http)
        self._deployment = deployment

    def _worker_role(self, intent: ResourceIntent) -> WorkerRole:
        if intent.artifact_digest != self._deployment.release.digest:
            raise OwnershipConflict("Cloudflare upload intent is bound to a different release")
        if intent.target.name == self._deployment.launcher_name:
            return "launcher"
        if intent.target.name == self._deployment.capacity_name:
            return "capacity"
        raise OwnershipConflict("Cloudflare upload intent has an unknown deployment target")

    def _render_upload(
        self, role: WorkerRole, intent: ResourceIntent, bearer: SecretStr | None,
    ) -> dict[str, tuple[str, str | bytes, str]]:
        deployment = self._deployment
        manifest = deployment.release.manifest
        artifact = manifest.workers[role]
        if (role == "capacity") != (bearer is not None):
            raise OwnershipConflict("Only the capacity Worker must receive the scope bearer")
        values = {
            "worker_ingress_origin": deployment.worker_ingress_origin,
            "launcher_cap": str(deployment.policy.launcher_cap),
            "max_lifetime_seconds": str(deployment.policy.max_lifetime_seconds),
            "slots_per_launcher": str(deployment.policy.slots_per_launcher),
            "worker_version": deployment.release.digest,
        }
        if deployment.policy.slots_per_launcher > manifest.max_slots_per_launcher:
            raise OwnershipConflict("Cloudflare release supports fewer slots than this policy")
        bindings = []
        for binding in artifact.bindings:
            value = binding.model_dump(exclude_none=True, exclude={"source", "script"})
            if binding.type == "secret_text":
                if role != "capacity" or binding.name != "CAPACITY_TOKEN" or binding.source != "scope_bearer" or bearer is None:
                    raise OwnershipConflict("Cloudflare release declares an invalid secret binding")
                value["text"] = bearer.get_secret_value()
            elif binding.source is not None:
                if binding.type != "plain_text" or binding.source not in values:
                    raise OwnershipConflict("Cloudflare release declares an invalid dynamic binding")
                value["text"] = values[binding.source]
            if binding.script is not None:
                if role != "capacity" or binding.type != "durable_object_namespace":
                    raise OwnershipConflict("Cloudflare release declares an invalid cross-script binding")
                value["script_name"] = deployment.launcher_name
            bindings.append(value)
        bindings.append({"name": OWNER_BINDING, "type": "plain_text", "text": worker_owner_value(intent)})
        if len({row["name"] for row in bindings}) != len(bindings):
            raise OwnershipConflict("Cloudflare release contains duplicate bindings")
        metadata = {
            "main_module": artifact.main_module,
            "compatibility_date": manifest.compatibility_date.isoformat(),
            "compatibility_flags": list(manifest.compatibility_flags),
            "exports": {name: export.model_dump() for name, export in artifact.exports.items()},
            "bindings": bindings,
            "limits": {"cpu_ms": artifact.cpu_ms},
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
        path = _script_path(intent.target)
        role = self._worker_role(intent)
        files = self._render_upload(role, intent, scope_bearer)
        # Check immediately before the upload. REST has no conditional create,
        # so the caller must retain its effect receipt if confirmation is lost.
        if await observe_owned(self, api_token=api_token, intent=intent, recorded=None) is not None:
            raise OwnershipConflict("Cloudflare Worker already exists at the intended target")
        payload = await self._request(api_token, intent.target.account_id, "PUT", path, files=files)
        worker = payload["result"] if payload is not None else None
        if not isinstance(worker, dict) or worker.get("id") != intent.target.name:
            raise ObservationPending("Cloudflare upload returned an unconfirmed script identity")
        observed = await observe_owned(self, api_token=api_token, intent=intent, recorded=None)
        if observed is None:
            raise ObservationPending("Cloudflare upload could not be observed")

    async def configure_endpoint(self, *, api_token: SecretStr, expected: ResourceObservation) -> str | None:
        """Finish endpoint configuration for an uploaded or recovered deployment."""
        intent = expected.intent
        role = self._worker_role(intent)
        path = _script_path(intent.target)
        if await observe_owned(self, api_token=api_token, intent=intent, recorded=expected) is None:
            raise ObservationPending("Cloudflare uploaded Worker has disappeared")
        settings = {"enabled": role == "capacity", "previews_enabled": False}
        await self._request(
            api_token, intent.target.account_id, "POST", f"{path}/subdomain",
            json_body=settings,
        )
        response = await self._request(api_token, intent.target.account_id, "GET", f"{path}/subdomain")
        readback = response["result"] if response is not None else None
        if not isinstance(readback, dict) or any(readback.get(key) is not value for key, value in settings.items()):
            raise ObservationPending("Cloudflare Worker endpoint settings could not be verified")
        if await observe_owned(self, api_token=api_token, intent=intent, recorded=expected) is None:
            raise ObservationPending("Cloudflare uploaded Worker has disappeared")
        if role == "launcher":
            return None
        response = await self._request(
            api_token, intent.target.account_id, "GET", "workers/subdomain",
        )
        account = response["result"] if response is not None else None
        subdomain = account.get("subdomain") if isinstance(account, dict) else None
        if not isinstance(subdomain, str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", subdomain):
            raise ObservationPending("Cloudflare account's Workers subdomain could not be verified")
        return f"https://{intent.target.name}.{subdomain}.workers.dev"
