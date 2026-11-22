"""Exact Container inventory and journal-bound registry effects."""

import base64
import json
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from pydantic import SecretStr

from src.services.worker_pool.cloudflare_containers_release import (
    ContainersReleaseUnavailable,
    VerifiedContainerRelease,
    run_oci_tool,
)
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
)
from src.services.worker_pool.cloudflare_workers_rest import WorkersRestCleanup

REGISTRY = "registry.cloudflare.com"
_HEX32 = re.compile(r"[a-f0-9]{32}")
_APP_ID = re.compile(r"[a-f0-9]{32}|[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")


def _name(intent: ResourceIntent, kind: str) -> str:
    target = intent.target
    name = f"bifrost-{target.installation_id.hex}-r{intent.desired_revision}"
    if (
        target.scope != "containers" or target.kind != kind or target.name != name
        or intent.desired_revision < 1 or not _HEX32.fullmatch(target.account_id)
    ):
        raise OwnershipConflict("Container target does not match its installation and revision")
    return name


class ContainersRest(WorkersRestCleanup):
    async def _inventory(
        self, token: SecretStr, account: str, path: str, **filters: str,
    ) -> list[dict[str, Any]]:
        params: dict[str, int | str] = {"per_page": 100, **filters}
        rows: dict[str, dict[str, Any]] = {}
        cursors: set[str] = set()
        total = None
        for _ in range(100):
            envelope = await self._request(token, account, "GET", path, params=params)
            assert envelope is not None
            batch, info = envelope["result"], envelope.get("result_info", {})
            if not isinstance(batch, list) or not isinstance(info, dict) or info.get("count", len(batch)) != len(batch):
                break
            if any(not isinstance(row, dict) or not isinstance(row.get("id"), str) for row in batch):
                break
            ids = {row["id"] for row in batch}
            if len(ids) != len(batch) or ids & rows.keys():
                break
            rows.update((row["id"], row) for row in batch)
            if "total_count" in info:
                reported = info["total_count"]
                if type(reported) is not int or reported < len(rows) or (total is not None and total != reported):
                    break
                total = reported
            cursor = info.get("next_page_token")
            if cursor is None or cursor == "":
                if (total is not None and total != len(rows)) or (not info and len(batch) >= 100):
                    break
                return list(rows.values())
            if not isinstance(cursor, str) or cursor in cursors or not batch:
                break
            cursors.add(cursor)
            params["page_token"] = cursor
        raise ObservationPending("Container inventory is incomplete")

    @staticmethod
    def _application(row: object, account: str, name: str, namespace: str) -> str:
        if (
            not isinstance(row, dict) or not isinstance(row.get("id"), str) or not _APP_ID.fullmatch(row["id"])
            or row.get("name") != name or row.get("account_id") != account
            or row.get("scheduling_policy") != "durable_object"
            or not isinstance(row.get("durable_objects"), dict)
            or row["durable_objects"].get("namespace_id") != namespace
        ):
            raise OwnershipConflict("Container application identity or namespace differs from its journal")
        return row["id"]

    async def application(
        self, token: SecretStr, intent: ResourceIntent, namespace: str, *, previously_sent: bool,
        recorded_id: str | None = None,
    ) -> ResourceObservation:
        """Resolve an existing write intent before issuing any create."""
        name = _name(intent, "container_application")
        account = intent.target.account_id
        if not _HEX32.fullmatch(namespace):
            raise OwnershipConflict("Container namespace is invalid")
        path = "containers/applications"
        rows = await self._inventory(token, account, path, name=name)
        if rows:
            if len(rows) != 1 or not previously_sent:
                raise OwnershipConflict("Container application name is occupied")
            app_id = self._application(rows[0], account, name, namespace)
        elif previously_sent:
            raise ObservationPending("Earlier Container create has no confirmed application")
        else:
            result = await self._request(token, account, "POST", path, json_body={
                "name": name, "scheduling_policy": "durable_object",
                "durable_objects": {"namespace_id": namespace},
            })
            assert result is not None
            app_id = self._application(result["result"], account, name, namespace)
        if recorded_id is not None and app_id != recorded_id:
            raise OwnershipConflict("Container application ID differs from its journal")
        current = await self._request(token, account, "GET", f"{path}/{app_id}")
        assert current is not None
        if self._application(current["result"], account, name, namespace) != app_id:
            raise OwnershipConflict("Container application ID changed")
        return ResourceObservation(intent, app_id, namespace)

class ContainerRegistry:
    """Use the qualified crane and cf paths with isolated credential files."""

    def __init__(self, rest: ContainersRest, *, crane: Path, cf: Path) -> None:
        self.rest = rest
        self.crane = crane
        self.cf = cf

    async def _tags(self, token: SecretStr, account: str, name: str, cwd: Path) -> frozenset[str]:
        env = {"CLOUDFLARE_ACCOUNT_ID": account, "CLOUDFLARE_API_TOKEN": token.get_secret_value()}
        if (await run_oci_tool(self.cf, "--version", cwd=cwd)).strip() != b"1.0.0-beta.31":
            raise ContainersReleaseUnavailable("Expected the pinned registry CLI")
        try:
            owner = json.loads(await run_oci_tool(self.cf, "accounts", "get", "-q", cwd=cwd, env=env))
            if owner["id"] != account:
                raise OwnershipConflict("Registry CLI selected another account")
            rows = json.loads(await run_oci_tool(
                self.cf, "containers", "images", "list", "--filter",
                f"^(?:{account}/)?{name}$", cwd=cwd, env=env,
            ))
        except (ValueError, TypeError, KeyError):
            raise ObservationPending("Registry tag inventory is malformed") from None
        if not isinstance(rows, list) or len(rows) > 1 or any(
            not isinstance(row, dict) or row.get("name") not in {name, f"{account}/{name}"}
            or not isinstance(row.get("tags"), list) or not row["tags"]
            or any(not isinstance(tag, str) or not re.fullmatch(r"[\w][\w.-]{0,127}", tag, re.ASCII)
                   for tag in row["tags"])
            or len(set(row["tags"])) != len(row["tags"])
            for row in rows
        ):
            raise ObservationPending("Registry tag inventory is unconfirmed")
        return frozenset(rows[0]["tags"]) if rows else frozenset()

    async def copy(
        self, token: SecretStr, intent: ResourceIntent, release: VerifiedContainerRelease, *, previously_sent: bool,
    ) -> ResourceObservation:
        """Replay only the journaled digest into an empty owned repository."""
        name = _name(intent, "container_image")
        account = intent.target.account_id
        if release.digest != intent.artifact_digest:
            raise OwnershipConflict("Container image release differs from its journal")
        destination = f"{REGISTRY}/{account}/{name}:release"
        with TemporaryDirectory(prefix="bifrost-oci-") as temp:
            cwd = Path(temp)
            if (await run_oci_tool(self.crane, "version", cwd=cwd)).strip() != b"v0.20.6":
                raise ContainersReleaseUnavailable("Expected the pinned OCI copy tool")
            await self._verify_image(release.manifest.image, release, cwd)
            tags = await self._tags(token, account, name, cwd)
            visible = "release" in tags
            if tags and (not previously_sent or not visible):
                raise OwnershipConflict("Container registry repository is occupied")
            credentials = await self.rest._request(
                token, account, "POST", f"containers/registries/{REGISTRY}/credentials",
                json_body={"expiration_minutes": 15, "permissions": ["pull"] if visible else ["pull", "push"]},
            )
            assert credentials is not None
            row = credentials["result"]
            if (
                not isinstance(row, dict) or row.get("account_id") != account or row.get("registry_host") != REGISTRY
                or any(not isinstance(row.get(key), str) or not row[key] for key in ("username", "password"))
            ):
                raise OwnershipConflict("Registry credentials have a different account or host")
            encoded = base64.b64encode(f"{row['username']}:{row['password']}".encode()).decode()
            with os.fdopen(os.open(cwd / "config.json", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as config:
                json.dump({"auths": {REGISTRY: {"auth": encoded}}}, config)
            if not visible:
                await run_oci_tool(self.crane, "copy", release.manifest.image, destination, cwd=cwd, timeout=90)
            await self._verify_image(destination, release, cwd)
            if "release" not in await self._tags(token, account, name, cwd):
                raise ObservationPending("Exact registry tag is still unconfirmed")
        return ResourceObservation(intent, destination, release.manifest.image_digest)

    async def _verify_image(self, image: str, release: VerifiedContainerRelease, cwd: Path) -> None:
        manifest = await run_oci_tool(self.crane, "manifest", image, cwd=cwd)
        config = await run_oci_tool(self.crane, "config", image, cwd=cwd)
        release.verify_image(manifest.removesuffix(b"\n"), config.removesuffix(b"\n"))
