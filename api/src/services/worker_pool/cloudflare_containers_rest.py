"""Exact Container inventory and journal-bound registry effects."""

import base64
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
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
    DeletionSettling,
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnershipCheck,
    OwnershipConflict,
    OwnershipResource,
    ResourceIntent,
    ResourceObservation,
    observe_owned,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    CloudflareRestObservation,
    CloudflareRestUnavailable,
    WorkersRestCleanup,
)

REGISTRY = "registry.cloudflare.com"
_HEX32 = re.compile(r"[a-f0-9]{32}")
_APP_ID = re.compile(r"[a-f0-9]{32}|[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")


@dataclass(frozen=True, slots=True)
class InstanceStop:
    started_at: datetime
    stopped_at: datetime


def _name(intent: ResourceIntent, kind: str) -> str:
    target = intent.target
    name = f"bifrost-{target.installation_id.hex}-r{intent.desired_revision}"
    if (
        target.scope != "containers" or target.kind != kind or target.name != name
        or intent.desired_revision < 1 or not _HEX32.fullmatch(target.account_id)
    ):
        raise OwnershipConflict("Container target does not match its installation and revision",
                                OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.TARGET_BINDING)
    return name


class ContainerApplicationCreateRejected(Exception):
    """The POST was rejected before an application could be created."""

    def __init__(self, failure: CloudflareManagementDenied | CloudflareRestObservation | CloudflareRestUnavailable) -> None:
        super().__init__("Cloudflare rejected Container application creation")
        self.failure = failure


class ContainersRest(WorkersRestCleanup):
    async def verify_launcher_namespace(
        self, token: SecretStr, expected: ResourceObservation, namespace: str,
    ) -> None:
        if not _HEX32.fullmatch(namespace):
            raise OwnershipConflict("Invalid Container launcher namespace",
                                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.NAMESPACE_CLASS)
        if await observe_owned(self, api_token=token, intent=expected.intent, recorded=expected) is None:
            raise ObservationSettling("The owned Container launcher is absent",
                                      OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_RECORDED_ABSENT)
        namespaces = await self._script_namespaces(token, expected.intent.target, namespace_ids=frozenset({namespace}))
        launchers = [row for row in namespaces if row.get("class") == "ContainerLauncher" or row["id"] == namespace]
        if (
            len(launchers) != 1 or launchers[0]["id"] != namespace
            or launchers[0].get("class") != "ContainerLauncher"
            or launchers[0].get("script") != expected.intent.target.name
        ):
            raise OwnershipConflict("Container launcher namespace differs from the deployment receipt",
                                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.RECEIPT_MISMATCH)

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
        raise ObservationPending("Container inventory is incomplete",
                                 OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)

    @staticmethod
    def _application(row: object, account: str, name: str, namespace: str) -> str:
        if (
            not isinstance(row, dict) or not isinstance(row.get("id"), str) or not _APP_ID.fullmatch(row["id"])
            or row.get("name") != name or row.get("account_id") != account
            or row.get("scheduling_policy") != "durable_object"
            or not isinstance(row.get("durable_objects"), dict)
            or row["durable_objects"].get("namespace_id") != namespace
        ):
            raise OwnershipConflict("Container application identity or namespace differs from its journal",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        return row["id"]

    async def observe_application(
        self, token: SecretStr, intent: ResourceIntent, namespace: str, *,
        recorded_id: str | None = None,
    ) -> ResourceObservation | None:
        name = _name(intent, "container_application")
        account = intent.target.account_id
        if not _HEX32.fullmatch(namespace) or (recorded_id is not None and not _APP_ID.fullmatch(recorded_id)):
            raise OwnershipConflict("Container journal lacks a valid application identity",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        path = "containers/applications"
        inventory = await self._inventory(token, account, path, name=name)
        if any(not isinstance(row.get("name"), str) for row in inventory):
            raise ObservationPending("Container inventory is missing application names",
                                     OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
        rows = [row for row in inventory if row["name"] == name]
        if len(rows) > 1:
            raise OwnershipConflict("Multiple Container applications match the name",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.MULTIPLE_OCCUPANTS)
        if not rows:
            if recorded_id is not None:
                receipt = await self._request(token, account, "GET", f"{path}/{recorded_id}", allow_missing=True)
                if receipt is not None:
                    self._application(receipt["result"], account, name, namespace)
                    raise ObservationSettling("Container application inventory differs from ID lookup",
                                              OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
            return None
        app_id = self._application(rows[0], account, name, namespace)
        if recorded_id is not None and app_id != recorded_id:
            raise OwnershipConflict("Container application ID differs from its journal",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        receipt = await self._request(token, account, "GET", f"{path}/{app_id}", allow_missing=True)
        if receipt is None:
            raise ObservationSettling("Container application has no matching ID readback",
                                      OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
        if self._application(receipt["result"], account, name, namespace) != app_id:
            raise OwnershipConflict("Container application ID changed",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        return ResourceObservation(intent, app_id, namespace)

    async def delete_application(self, token: SecretStr, expected: ResourceObservation) -> None:
        intent = expected.intent
        observed = await self.observe_application(
            token, intent, expected.provider_version, recorded_id=expected.provider_id,
        )
        if observed is None:
            return
        account = intent.target.account_id
        path = f"containers/applications/{expected.provider_id}"
        placements = await self._inventory(token, account, f"{path}/instances-v2")
        if any(
            row.get("application_id") != expected.provider_id
            or not isinstance(row.get("status"), dict)
            or row["status"].get("state") not in ("stopped", "inactive")
            for row in placements
        ):
            raise DeletionSettling("Container application still has live or unverified placements",
                                   OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_PLACEMENTS)
        await self._request(token, account, "DELETE", path, allow_missing=True)
        if await self.observe_application(token, intent, expected.provider_version, recorded_id=expected.provider_id):
            raise DeletionSettling("Container application is still visible",
                                   OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)

    async def create_application(self, token: SecretStr, intent: ResourceIntent, namespace: str) -> ResourceObservation:
        """Send the already-journaled POST. The caller independently observes the result."""
        name = _name(intent, "container_application")
        account = intent.target.account_id
        if not _HEX32.fullmatch(namespace):
            raise OwnershipConflict("Container namespace is invalid",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.NAMESPACE_CLASS)
        try:
            result = await self._request(token, account, "POST", "containers/applications", json_body={
                "name": name, "scheduling_policy": "durable_object",
                "durable_objects": {"namespace_id": namespace},
            })
        except (CloudflareManagementDenied, CloudflareRestObservation, CloudflareRestUnavailable) as exc:
            if isinstance(exc, CloudflareManagementDenied) or exc.status_code in (400, 429):
                raise ContainerApplicationCreateRejected(exc) from None
            if exc.status_code == 409:
                raise ObservationSettling("Waiting to read back the occupied Container name",
                                          OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_CREATE) from None
            raise
        assert result is not None
        return ResourceObservation(intent, self._application(result["result"], account, name, namespace), namespace)

    async def observe_instance_stop(
        self, token: SecretStr, application: ResourceObservation, *, instance_id: str,
        image: str, not_before: datetime,
    ) -> InstanceStop:
        """Read the exact application and actor without waking the Durable Object."""
        placements = await self._instance_inventory(token, application, instance_id)
        if not placements:
            raise ObservationPending("Missing instance readback does not witness a stopped placement")
        if len(placements) != 1:
            raise OwnershipConflict("Container readback has multiple placements for its start fence")
        row = placements[0]
        if row.get("id") != instance_id or row.get("application_id") != application.provider_id or row.get("image") != image:
            raise OwnershipConflict("Container placement differs from its journal")
        try:
            status = row["status"]
            if status["state"] not in ("stopped", "inactive"):
                raise ValueError
            started_at = datetime.fromisoformat(row["started_at"])
            stopped_at = datetime.fromisoformat(status["updated_at"])
            if not not_before <= started_at <= stopped_at <= datetime.now(UTC):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ObservationPending("Provider has not witnessed a bounded stop for this placement") from None
        return InstanceStop(started_at.astimezone(UTC), stopped_at.astimezone(UTC))

    async def observe_no_placement(self, token: SecretStr, application: ResourceObservation, instance_id: str) -> None:
        if await self._instance_inventory(token, application, instance_id):
            raise ObservationPending("A void Container still has a provider placement")

    async def _instance_inventory(
        self, token: SecretStr, application: ResourceObservation, instance_id: str,
    ) -> list[dict[str, Any]]:
        name = _name(application.intent, "container_application")
        account = application.intent.target.account_id
        if (
            not _APP_ID.fullmatch(application.provider_id) or not _HEX32.fullmatch(application.provider_version)
            or not re.fullmatch(r"[a-f0-9]{64}", instance_id)
        ):
            raise OwnershipConflict("Invalid Container instance observation target")
        path = f"containers/applications/{application.provider_id}"
        app = await self._request(token, account, "GET", path)
        assert app is not None
        if self._application(app["result"], account, name, application.provider_version) != application.provider_id:
            raise OwnershipConflict("Container application ID changed")
        return await self._inventory(token, account, f"{path}/instances-v2", name_prefix=instance_id)


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
            or not isinstance(row.get("tags"), list)
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
            if tags and (not previously_sent or tags != {"release"}):
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
