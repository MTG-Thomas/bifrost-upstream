"""Project only the active tunnel token into the opted-in local connector channel."""

import asyncio
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import UUID

import httpx
from sqlalchemy import select, text

from src.config import get_settings
from src.core.database import get_db_context
from src.jobs.platform.kubernetes_client import (
    CLOUDFLARE_CONNECTOR_SECRET,
    KubernetesApiError,
    KubernetesConfigurationError,
    KubernetesJobClient,
    in_cluster_namespace,
)
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.worker_pool.cloudflare_tunnel import active_connector_token

COMPOSE_PATH = Path("/run/bifrost-tunnel/token")
Projection = tuple[UUID | None, int | None, int | None]


async def connector_channel_ready() -> bool:
    channel = get_settings().cloudflare_tunnel_channel
    if channel == "compose":
        return COMPOSE_PATH.parent.is_mount() and os.access(COMPOSE_PATH.parent, os.W_OK)
    if channel == "kustomize":
        namespace = in_cluster_namespace()
        if namespace is None:
            return False
        try:
            client = KubernetesJobClient(namespace=namespace)
            try:
                await client.read_cloudflare_connector()
                return await client.allows("patch", "secrets", name=CLOUDFLARE_CONNECTOR_SECRET)
            finally:
                await client.close()
        except (KubernetesApiError, KubernetesConfigurationError, httpx.HTTPError):
            return False
    return False


def _publish_file(path: Path, value: str) -> None:
    if not value:
        path.unlink(missing_ok=True)
        return
    with NamedTemporaryFile(dir=path.parent, delete=False, mode="w", encoding="ascii") as handle:
        tmp = Path(handle.name)
        try:
            handle.write(value)
            handle.close()
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)


async def project_connector(previous: Projection | None) -> Projection | None:
    """Use the existing ingress watch. Re-read under the installation lock before a write."""
    channel = get_settings().cloudflare_tunnel_channel
    if channel is None:
        return previous
    query = select(CloudflareInstallation).where(CloudflareInstallation.retired_at.is_(None))
    async with asyncio.timeout(8), get_db_context() as db:
        row = await db.scalar(query)
        key = (row.id, row.tunnel_token_version, row.desired_revision) if row is not None else (None, None, None)
        if key == previous:
            return previous
        await db.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('bifrost:cloudflare:connector', 0))"))
        row = await db.scalar(query.with_for_update().execution_options(populate_existing=True))
        token = await active_connector_token(db, row) if row is not None else None
        value = token.get_secret_value() if token is not None else ""
        key = (row.id, row.tunnel_token_version, row.desired_revision) if row is not None else (None, None, None)
        if channel == "compose":
            _publish_file(COMPOSE_PATH, value)
        else:
            namespace = in_cluster_namespace()
            if namespace is None:
                raise KubernetesConfigurationError("The connector projection requires the API pod namespace")
            client = KubernetesJobClient(namespace=namespace)
            try:
                await client.publish_cloudflare_connector(value, fence="/".join(map(str, key)))
            finally:
                await client.close()
        if row is not None and row.reachability_mode == "managed_tunnel":
            row.tunnel_token_projected_version = row.tunnel_token_version or 0
        return key
