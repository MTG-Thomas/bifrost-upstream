"""Exact-target REST operations for a remotely managed Cloudflare Tunnel."""

import base64
import binascii
import json
import re
from collections.abc import Callable
from hmac import compare_digest
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import SecretStr

from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
)
from src.services.worker_pool.cloudflare_workers_rest import WorkersRestCleanup


class IngressMismatch(OwnershipConflict):
    """The owned tunnel no longer has its approved ingress."""


def connector_secret(token: str, account_id: str, tunnel_id: str) -> str:
    try:
        payload = json.loads(base64.b64decode(token, validate=True))
        if payload["a"] == account_id and payload["t"] == tunnel_id:
            value = payload["s"]
            if isinstance(value, str) and len(base64.b64decode(value, validate=True)) == 32:
                return value
    except (ValueError, TypeError, KeyError, binascii.Error):
        pass
    raise OwnershipConflict("Cloudflare returned a different connector credential")


def dedicated_hostname(origin: str) -> str:
    hostname = urlsplit(origin).hostname
    canonical = f"https://{hostname}"
    if (
        not hostname or origin not in (canonical, canonical + "/") or len(canonical) > 255
        or "." not in hostname
        or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in hostname.split("."))
    ):
        raise ValueError("Cloudflare worker ingress requires a dedicated HTTPS origin")
    return hostname


class TunnelRest(WorkersRestCleanup):
    def __init__(self, http: httpx.AsyncClient, *, account_id: str, zone_id: str, hostname: str, token: SecretStr):
        super().__init__(http)
        self.account_id = account_id
        self.zone_id = zone_id
        self.hostname = hostname
        self.token = token
        self.tunnels = f"accounts/{account_id}/cfd_tunnel"
        self.records = f"zones/{zone_id}/dns_records"

    async def _result(self, method: str, path: str, **kwargs: Any) -> Any:
        result = await self._request_resource(self.token, method, path, **kwargs)
        return result["result"] if result is not None else None

    async def verify_zone(self) -> None:
        zone = await self._result("GET", f"zones/{self.zone_id}")
        if not isinstance(zone, dict) or zone.get("id") != self.zone_id:
            raise OwnershipConflict("Cloudflare zone identity did not match")
        account = zone.get("account")
        name = zone.get("name")
        if not isinstance(account, dict) or account.get("id") != self.account_id:
            raise OwnershipConflict("Cloudflare zone belongs to a different account")
        if not isinstance(name, str) or not self.hostname.endswith("." + name) or zone.get("status") != "active":
            raise ValueError("Managed Tunnel requires a dedicated hostname in the selected active DNS zone")

    async def connectors(self, tunnel_id: str) -> frozenset[str]:
        payload = await self._request_resource(self.token, "GET", f"{self.tunnels}/{tunnel_id}/connections")
        items = payload["result"] if payload else None
        info = payload.get("result_info") if payload else None
        if not isinstance(items, list) or (
            isinstance(info, dict) and info.get("total_count", len(items)) != len(items)
        ):
            raise ObservationPending("Cloudflare connector inventory is incomplete")
        try:
            return frozenset(str(UUID(item["id"])) for item in items)
        except (KeyError, TypeError, ValueError, AttributeError):
            raise ObservationPending("Cloudflare connector inventory is malformed") from None

    async def _find(
        self, path: str, params: dict[str, int | str], name: str,
        *, matches_body: Callable[[dict[str, Any]], bool] | None = None,
    ) -> dict[str, Any] | None:
        payload = await self._request_resource(self.token, "GET", path, params={**params, "per_page": 20})
        items = payload["result"] if payload else None
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ObservationPending("Cloudflare returned an invalid inventory")
        info = payload.get("result_info") if payload else None
        if len(items) >= 20 or (isinstance(info, dict) and info.get("total_pages", 1) not in (0, 1)):
            raise OwnershipConflict("Cloudflare exact-target inventory is ambiguous")
        matches = [
            item for item in items if item.get("name") == name and (matches_body is None or matches_body(item))
        ]
        if len(matches) > 1:
            raise OwnershipConflict("Cloudflare exact target has multiple occupants")
        return matches[0] if matches else None

    async def tunnel(
        self, intent: ResourceIntent, recorded: ResourceObservation | None, *, secret: SecretStr | None = None,
    ) -> ResourceObservation | None:
        name = intent.target.name
        item = await self._find(self.tunnels, {"name": name, "is_deleted": "false"}, name)
        if item is None:
            if recorded is not None:
                exact = await self._result("GET", f"{self.tunnels}/{recorded.provider_id}", allow_missing=True)
                if exact is not None and (
                    not isinstance(exact, dict) or exact.get("id") != recorded.provider_id
                    or exact.get("name") != name or not exact.get("deleted_at")
                ):
                    raise OwnershipConflict("Cloudflare tunnel changed outside its journal")
            return None
        try:
            tunnel_id = str(UUID(item["id"]))
        except (KeyError, TypeError, ValueError, AttributeError):
            raise ObservationPending("Cloudflare tunnel has no resource ID") from None
        created = item.get("created_at")
        if item.get("account_tag") != self.account_id or item.get("config_src") != "cloudflare" or item.get("deleted_at"):
            raise OwnershipConflict("Cloudflare tunnel differs from its journal")
        if not isinstance(created, str) or not created or len(created) > 255:
            raise ObservationPending("Cloudflare tunnel has no creation timestamp")
        observed = ResourceObservation(intent, tunnel_id, created)
        if recorded is not None:
            if observed != recorded:
                raise OwnershipConflict("Cloudflare tunnel differs from its journal")
        elif secret is None or not compare_digest(
            await self.secret(tunnel_id), secret.get_secret_value(),
        ):
            raise OwnershipConflict("Cloudflare tunnel does not match the committed creation credential")
        return observed

    async def create_tunnel(self, intent: ResourceIntent, secret: SecretStr) -> ResourceObservation:
        observed = await self.tunnel(intent, None, secret=secret)
        if observed is None:
            await self._result("POST", self.tunnels, json_body={
                "name": intent.target.name, "config_src": "cloudflare",
                "tunnel_secret": secret.get_secret_value(),
            })
            observed = await self.tunnel(intent, None, secret=secret)
        if observed is None:
            raise ObservationPending("Cloudflare tunnel creation has not settled")
        return observed

    async def connector_token(self, tunnel_id: str) -> SecretStr:
        token = await self._result("GET", f"{self.tunnels}/{tunnel_id}/token")
        if not isinstance(token, str) or len(token) > 4096:
            raise ObservationPending("Cloudflare returned an invalid connector token")
        connector_secret(token, self.account_id, tunnel_id)
        return SecretStr(token)

    async def secret(self, tunnel_id: str) -> str:
        token = await self.connector_token(tunnel_id)
        return connector_secret(token.get_secret_value(), self.account_id, tunnel_id)

    def ingress(self, origin: str) -> list[dict[str, str]]:
        return [{"hostname": self.hostname, "service": origin}, {"service": "http_status:404"}]

    async def _put_ingress(self, path: str, desired: list[dict[str, str]]) -> None:
        await self._result("PUT", path, json_body={
            "config": {"ingress": desired, "warp-routing": {"enabled": False}},
        })

    async def configure_ingress(self, tunnel: ResourceObservation, origin: str) -> None:
        path = f"{self.tunnels}/{tunnel.provider_id}/configurations"
        desired = self.ingress(origin)
        current = await self._result("GET", path, allow_missing=True)
        if current is not None and self._matches_ingress(current, tunnel.provider_id, desired):
            return
        if current is not None and (
            not isinstance(current, dict) or current.get("tunnel_id") != tunnel.provider_id
            or current.get("account_id") != self.account_id or current.get("version") != 0
            or current.get("config") not in ({}, None)
        ):
            raise OwnershipConflict("Cloudflare tunnel already has a different ingress configuration")
        if await self.tunnel(tunnel.intent, tunnel) is None:
            raise ObservationPending("The recorded Cloudflare tunnel is absent")
        await self._put_ingress(path, desired)
        await self.verify_ingress(tunnel, origin)

    def _matches_ingress(self, current: Any, tunnel_id: str, desired: list[dict[str, str]]) -> bool:
        if not self._config_identity(current, tunnel_id):
            return False
        config = current.get("config")
        if not isinstance(config, dict) or config.get("originRequest") not in (None, {}):
            return False
        warp = config.get("warp-routing")
        if warp is not None and warp != {} and (
            not isinstance(warp, dict) or set(warp) != {"enabled"} or warp["enabled"] is not False
        ):
            return False
        rules = config.get("ingress")
        if not isinstance(rules, list) or len(rules) != len(desired):
            return False
        return all(
            isinstance(rule, dict) and rule.get("originRequest") in (None, {})
            and not rule.get("path")
            and {key: rule[key] for key in ("hostname", "service") if rule.get(key)} == expected
            for rule, expected in zip(rules, desired, strict=True)
        )

    def _config_identity(self, current: Any, tunnel_id: str) -> bool:
        return (isinstance(current, dict) and current.get("tunnel_id") == tunnel_id
                and current.get("account_id") == self.account_id)

    async def verify_ingress(self, tunnel: ResourceObservation, origin: str) -> None:
        current = await self._result("GET", f"{self.tunnels}/{tunnel.provider_id}/configurations", allow_missing=True)
        if not self._matches_ingress(current, tunnel.provider_id, self.ingress(origin)):
            raise IngressMismatch("Cloudflare tunnel ingress differs from the approved route")

    async def verify_private_routes(self, tunnel_id: str) -> None:
        rows = await self._result(
            "GET", f"accounts/{self.account_id}/teamnet/routes",
            params={"tunnel_id": tunnel_id, "is_deleted": "false"},
        )
        if not isinstance(rows, list) or any(
            not isinstance(row, dict) or row.get("tunnel_id") != tunnel_id for row in rows
        ):
            raise ObservationPending("Cloudflare private-network route inventory could not be confirmed")
        if rows:
            raise OwnershipConflict("Cloudflare private-network routes target the managed tunnel")

    async def write_ingress(
        self, tunnel: ResourceObservation, origin: str, *, repair: bool = False, prove_write: bool = False,
    ) -> None:
        exact = await self._result("GET", f"{self.tunnels}/{tunnel.provider_id}", allow_missing=True)
        if exact is None:
            raise ObservationPending("The recorded Cloudflare tunnel is absent")
        if not isinstance(exact, dict) or any(
            exact.get(key) != value for key, value in {
                "id": tunnel.provider_id, "name": tunnel.intent.target.name, "account_tag": self.account_id,
                "created_at": tunnel.provider_version, "config_src": "cloudflare",
            }.items()
        ) or exact.get("deleted_at"):
            raise OwnershipConflict("Cloudflare tunnel differs from its journal")
        path = f"{self.tunnels}/{tunnel.provider_id}/configurations"
        current = await self._result("GET", path, allow_missing=True)
        desired = self.ingress(origin)
        matches = self._matches_ingress(current, tunnel.provider_id, desired)
        if repair:
            if current is not None and not self._config_identity(current, tunnel.provider_id):
                raise OwnershipConflict("Cloudflare tunnel configuration identity changed")
            if matches and not prove_write:
                return
        else:
            if not matches:
                raise IngressMismatch("Cloudflare tunnel ingress differs from the approved route")
        await self._put_ingress(path, desired)
        await self.verify_ingress(tunnel, origin)

    def record_body(self, intent: ResourceIntent, tunnel_id: str) -> dict[str, Any]:
        return {
            "name": intent.target.name, "type": "CNAME", "proxied": True, "ttl": 1,
            "comment": f"bifrost:{intent.target.installation_id}:{intent.creation_effect_id}:{intent.desired_revision}",
            "content": f"{tunnel_id}.cfargotunnel.com",
        }

    async def dns(
        self, intent: ResourceIntent, recorded: ResourceObservation | None, tunnel_id: str,
        *, ignore_foreign: bool = False,
    ) -> ResourceObservation | None:
        desired = self.record_body(intent, tunnel_id)

        def matches_body(item: dict[str, Any]) -> bool:
            return all(item.get(key) == value for key, value in desired.items())

        item = await self._find(
            self.records, {"name.exact": intent.target.name}, intent.target.name,
            matches_body=matches_body if ignore_foreign else None,
        )
        if item is None or (ignore_foreign and recorded is not None and item.get("id") != recorded.provider_id):
            if recorded is not None and await self._result(
                "GET", f"{self.records}/{recorded.provider_id}", allow_missing=True,
            ) is not None:
                raise OwnershipConflict("The recorded Cloudflare DNS record moved")
            return None
        identity, version = item.get("id"), item.get("modified_on")
        if not matches_body(item):
            raise OwnershipConflict("The dedicated Cloudflare hostname already has a different DNS record")
        if not isinstance(identity, str) or not re.fullmatch(r"[a-f0-9]{32}", identity):
            raise ObservationPending("Cloudflare DNS record has no resource ID")
        if not isinstance(version, str) or not version or len(version) > 255:
            raise ObservationPending("Cloudflare DNS record has no revision")
        observed = ResourceObservation(intent, identity, version)
        # A same-body permissions probe can advance modified_on even if its response is lost.
        if recorded is not None and observed.provider_id != recorded.provider_id:
            raise OwnershipConflict("Cloudflare DNS record differs from its journal")
        return observed

    async def verify_dns_write(self, record: ResourceObservation, tunnel_id: str) -> None:
        if await self.dns(record.intent, record, tunnel_id) is None:
            raise ObservationPending("The recorded Cloudflare DNS record is absent")
        await self._result("PATCH", f"{self.records}/{record.provider_id}",
                           json_body=self.record_body(record.intent, tunnel_id))
        if await self.dns(record.intent, record, tunnel_id) is None:
            raise ObservationPending("The recorded Cloudflare DNS record is absent")

    async def evict_connectors(self, tunnel: ResourceObservation, previous: set[str]) -> None:
        if await self.tunnel(tunnel.intent, tunnel) is None:
            raise ObservationPending("The recorded Cloudflare tunnel is absent")
        path = f"{self.tunnels}/{tunnel.provider_id}/connections"
        for connector in sorted(await self.connectors(tunnel.provider_id) & previous):
            await self._result("DELETE", path, params={"client_id": connector}, allow_missing=True)

    async def create_dns(self, intent: ResourceIntent, tunnel: ResourceObservation) -> ResourceObservation:
        await self.verify_zone()
        if await self.tunnel(tunnel.intent, tunnel) is None:
            raise ObservationPending("The recorded Cloudflare tunnel is absent")
        observed = await self.dns(intent, None, tunnel.provider_id)
        if observed is None:
            await self._result("POST", self.records, json_body=self.record_body(intent, tunnel.provider_id))
            observed = await self.dns(intent, None, tunnel.provider_id)
        if observed is None:
            raise ObservationPending("Cloudflare DNS creation has not settled")
        return observed

    async def remove_dns(self, record: ResourceObservation, tunnel_id: str) -> None:
        await self.verify_zone()
        if await self.dns(record.intent, record, tunnel_id, ignore_foreign=True) is not None:
            await self._result("DELETE", f"{self.records}/{record.provider_id}", allow_missing=True)
        if await self.dns(record.intent, record, tunnel_id, ignore_foreign=True) is not None:
            raise ObservationPending("Cloudflare DNS removal has not settled")

    async def remove_tunnel(self, tunnel: ResourceObservation) -> None:
        if await self.tunnel(tunnel.intent, tunnel) is not None:
            if await self.connectors(tunnel.provider_id):
                raise DeletionSettling("Cloudflare connectors remain attached to the tunnel")
            await self._result("DELETE", f"{self.tunnels}/{tunnel.provider_id}", allow_missing=True)
        if await self.tunnel(tunnel.intent, tunnel) is not None:
            raise DeletionSettling("Cloudflare tunnel removal has not settled")

    async def rotate(self, tunnel: ResourceObservation, current: SecretStr, replacement: SecretStr) -> SecretStr:
        if await self.tunnel(tunnel.intent, tunnel) is None:
            raise ObservationPending("The recorded Cloudflare tunnel is absent")
        token = await self.connector_token(tunnel.provider_id)
        old = current.get_secret_value()
        new = replacement.get_secret_value()
        observed = connector_secret(token.get_secret_value(), self.account_id, tunnel.provider_id)
        if compare_digest(observed, new):
            return token
        if not compare_digest(observed, old):
            raise OwnershipConflict("The Cloudflare tunnel credential changed outside Bifrost")
        await self._result("PATCH", f"{self.tunnels}/{tunnel.provider_id}", json_body={"tunnel_secret": new})
        token = await self.connector_token(tunnel.provider_id)
        if not compare_digest(connector_secret(token.get_secret_value(), self.account_id, tunnel.provider_id), new):
            raise ObservationPending("Cloudflare tunnel credential rotation has not settled")
        return token
