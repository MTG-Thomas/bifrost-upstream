"""Cloudflare Workers inventory and retirement on the qualified REST path."""

import re
from typing import Any

import httpx
from pydantic import SecretStr

from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
    observe_owned,
)

API = "https://api.cloudflare.com/client/v4"
OWNER_BINDING = "BIFROST_CAPACITY_OWNER"


class CloudflareManagementDenied(Exception):
    """The provider refused the scoped management credential."""


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
        allow_missing: bool = False, params: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        try:
            response = await self._http.request(
                method, f"{API}/accounts/{account}/{path}", params=params,
                headers={"Authorization": f"Bearer {token.get_secret_value()}"},
            )
        except httpx.HTTPError:
            raise ObservationPending("Cloudflare REST request could not be observed") from None
        if response.status_code in (401, 403):
            raise CloudflareManagementDenied
        if response.status_code == 404 and allow_missing:
            return None
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
        if (
            not isinstance(versions, list) or len(versions) != 1
            or not isinstance(versions[0], dict) or versions[0].get("percentage") != 100
            or not isinstance(versions[0].get("version_id"), str)
            or not re.fullmatch(r"[a-f0-9-]{36}", versions[0]["version_id"])
        ):
            raise OwnershipConflict("Cloudflare Worker does not have one exact active version")
        return ResourceObservation(intent, target.name, versions[0]["version_id"])

    async def delete(self, *, api_token: SecretStr, expected: ResourceObservation) -> None:
        # Workers DELETE is by script name. Account-side writers outside the
        # installation lease can still race this last ownership check.
        if await observe_owned(self, api_token=api_token, intent=expected.intent, recorded=expected) is not None:
            target = expected.intent.target
            await self._request(api_token, target.account_id, "DELETE", _script_path(target), allow_missing=True)

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
