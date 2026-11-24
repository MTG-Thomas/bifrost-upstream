"""Review workspace proposals and verify approved sources before an opaque send."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from uuid import UUID

import httpx

from bifrost.cloudflare_analysis import (
    MAX_BUNDLE_BYTES,
    MAX_SOURCE_BYTES,
    POLICY_VERSION,
    AnalysisRejected,
    SourceUnresolved,
    digest,
)
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import lazyload, load_only

from src.core.database import get_db_context
from src.core.storage.contracts import StorageError
from src.models.contracts.cloudflare_approval import (
    ApprovalDeclaration,
    LINUX_REVIEW_VERSION,
    LinuxReview,
    SolutionApprovalScope,
    linux_review_digest,
)
from src.models.orm import (
    CloudflareApproval,
    FileIndex,
    Solution,
    SolutionFileIndex,
    Workflow,
)
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.execution.cloudflare_analysis_runner import analyze
from src.services.execution.cloudflare_hosts import reviewed_https_allowlist
from src.services.repo_storage import RepoStorage
from src.services.solutions.storage import SolutionStorage
from src.services.worker_pool.cloudflare_containers_policy import ContainersPolicy
from src.services.worker_pool.cloudflare_containers_release import (
    ContainersReleaseFetchError,
    ContainersReleaseUnavailable,
    load_containers_release,
)
from src.services.worker_pool.cloudflare_release import WorkersReleaseUnavailable, load_workers_release


def workflow_review_scope(workflow: Workflow) -> tuple[UUID | None, UUID | None, str, str]:
    return workflow.organization_id, workflow.solution_id, workflow.path, workflow.function_name


@dataclass(frozen=True)
class ApprovalProposal:
    workflow_id: UUID
    organization_id: UUID | None
    previous_revision: str | None
    declaration: ApprovalDeclaration
    inactive_history_count: int = 0
    container: LinuxReview | None = None

    def review_document(self) -> dict[str, object]:
        return {
            "policy_version": LINUX_REVIEW_VERSION if self.container is not None else POLICY_VERSION,
            "workflow_id": str(self.workflow_id),
            "organization_id": str(self.organization_id) if self.organization_id else None,
            "previous_revision": self.previous_revision,
            "inactive_history_count": self.inactive_history_count,
            **self.declaration.review_fields(),
        }

    @property
    def revision(self) -> str:
        if self.container is not None:
            return linux_review_digest(self.workflow_id, self.organization_id, self.container.model_dump(mode="json"))
        return digest(self.review_document())


@dataclass(frozen=True)
class ApprovedWorkspace:
    bundle: dict[str, object]
    entry_sha256: str


async def current_approval(db: AsyncSession, workflow_id: UUID) -> CloudflareApproval | None:
    return await db.scalar(select(CloudflareApproval).where(
        CloudflareApproval.workflow_id == workflow_id, CloudflareApproval.is_current,
    ))


async def _history_count(db: AsyncSession, workflow_id: UUID) -> int:
    return await db.scalar(
        select(func.count()).select_from(CloudflareApproval).where(
            CloudflareApproval.workflow_id == workflow_id, ~CloudflareApproval.is_review,
        )
    ) or 0


async def lock_workflow(db: AsyncSession, workflow_id: UUID) -> Workflow | None:
    # PostgreSQL FOR NO KEY UPDATE serializes review writers without
    # conflicting with foreign-key checks on the workflow row.
    return await db.get(Workflow, workflow_id, with_for_update={"key_share": True}, populate_existing=True)


async def revoke_approval(db: AsyncSession, workflow_id: UUID) -> str | None:
    if await lock_workflow(db, workflow_id) is None:
        raise AnalysisRejected("Workflow does not exist")
    current = await current_approval(db, workflow_id)
    if current is None:
        return None
    current.is_current = False
    return current.revision


async def indexed_pins(
    db: AsyncSession, paths: set[str], *, install_id: UUID | None = None,
) -> dict[str, str | None]:
    if install_id is None:
        query = select(FileIndex.path, FileIndex.content_hash).where(FileIndex.path.in_(paths))
    else:
        query = select(SolutionFileIndex.path, SolutionFileIndex.content_hash).where(
            SolutionFileIndex.solution_id == install_id, SolutionFileIndex.path.in_(paths),
        )
    rows = await db.execute(query)
    return {row.path: row.content_hash for row in rows}


async def solution_approval_scope(
    db: AsyncSession, install_id: UUID, organization_id: UUID | None,
) -> SolutionApprovalScope | None:
    row = (await db.execute(select(Solution.version, Solution.runtime_source_generation).where(
        Solution.id == install_id, Solution.status == "active",
        Solution.organization_id.is_not_distinct_from(organization_id),
        Solution.runtime_source_generation.is_not(None),
        ~Solution.allow_outbound_access,
    ))).one_or_none()
    if row is None:
        return None
    return SolutionApprovalScope(
        install_id=install_id, version=row.version, source_generation=row.runtime_source_generation,
    )


def _source_storage(install_id: UUID | None) -> RepoStorage | SolutionStorage:
    return SolutionStorage(install_id) if install_id is not None else RepoStorage()


class ObservationChanged(AnalysisRejected):
    """Storage has moved beyond the source digests used to schedule the job."""


def resolve_approval_placement(workflow: Workflow, solution: Solution | None) -> SolutionApprovalScope | None:
    if not workflow.is_active:
        raise ObservationChanged("Expected an active workflow")
    if workflow.type != "workflow":
        raise ObservationChanged("Only workflows can be approved", "CF_EXECUTABLE_TYPE")
    if workflow.execution_lane == "fast_path":
        raise ObservationChanged("Cloudflare supplies Normal capacity only", "CF_FAST_PATH")
    if workflow.solution_id is None:
        return None
    if solution is not None and solution.allow_outbound_access:
        raise ObservationChanged("The Solution install allows outbound access", "CF_OUTBOUND")
    if (
        solution is None or solution.status != "active" or solution.organization_id != workflow.organization_id
        or solution.runtime_source_generation is None
    ):
        raise ObservationChanged("Expected an active Solution with published install-local source")
    return SolutionApprovalScope(
        install_id=solution.id, version=solution.version, source_generation=solution.runtime_source_generation,
    )


def solution_placement_query():
    return select(Solution).options(
        load_only(
            Solution.id, Solution.status, Solution.organization_id, Solution.version,
            Solution.runtime_source_generation, Solution.allow_outbound_access,
        ),
        lazyload("*"),
    ).execution_options(populate_existing=True)


async def approval_placement(db: AsyncSession, workflow: Workflow) -> SolutionApprovalScope | None:
    solution = None
    if workflow.is_active and workflow.solution_id is not None:
        solution = await db.scalar(solution_placement_query().where(Solution.id == workflow.solution_id))
    return resolve_approval_placement(workflow, solution)


async def read_pinned_sources(declaration: ApprovalDeclaration) -> dict[str, str]:
    """Read an exact reviewed bundle without retaining a second copy."""
    manifest = declaration.manifest
    if digest(manifest) != declaration.manifest_sha256:
        raise ObservationChanged("Reviewed source changed")
    install_id = declaration.solution.install_id if declaration.solution else None
    storage = _source_storage(install_id)
    sources: dict[str, str] = {}
    remaining = MAX_BUNDLE_BYTES
    try:
        for path, sha256 in manifest.items():
            content = await storage.read(path, max_bytes=min(remaining, MAX_SOURCE_BYTES))
            if hashlib.sha256(content).hexdigest() != sha256:
                raise ObservationChanged("Reviewed source changed")
            sources[path] = content.decode("utf-8")
            remaining -= len(content)
    except ObservationChanged:
        raise
    except ValueError as exc:
        raise ObservationChanged("Reviewed source changed") from exc
    except StorageError as exc:
        if exc.kind == "not_found":
            raise ObservationChanged("Reviewed source changed") from exc
        raise
    return sources


async def propose_approval(
    workflow_id: UUID, *, subrequest_allowance: int, cpu_budget_ms: int,
    expected_pins: dict[str, str | None] | None = None,
    analysed_pins: dict[str, str | None] | None = None,
    https_hosts: list[str] | None = None,
    sources: dict[str, bytes] | None = None,
    expected_scope: tuple[UUID | None, UUID | None, str, str] | None = None,
    containers: bool = False,
) -> ApprovalProposal:
    installation = None
    release = None
    async with get_db_context() as db:
        workflow = await db.get(Workflow, workflow_id)
        if workflow is None:
            raise ObservationChanged("Expected an active workflow")
        if expected_scope is not None and workflow_review_scope(workflow) != expected_scope:
            raise ObservationChanged("Workflow scope changed before analysis")
        solution = await approval_placement(db, workflow)
        install_id = workflow.solution_id
        current = await current_approval(db, workflow_id)
        previous_revision = current.revision if current else None
        history_count = 0 if current else await _history_count(db, workflow_id)
        entry_path, function_name, organization_id = workflow.path, workflow.function_name, workflow.organization_id
        if containers:
            installation = await db.scalar(select(CloudflareInstallation).where(
                CloudflareInstallation.retired_at.is_(None),
                CloudflareInstallation.admitted_revision == CloudflareInstallation.desired_revision,
                CloudflareInstallation.containers_ready_revision == CloudflareInstallation.desired_revision,
            ))
            if installation is None or installation.containers_policy.get("enabled") is not True:
                raise AnalysisRejected("Enable the Containers lane before reviewing this workflow")
            if https_hosts:
                raise AnalysisRejected("Container workflows may only use the worker ingress", "CF_TRANSPORT")

    linux_packages = None
    if installation is not None:
        try:
            async with httpx.AsyncClient(timeout=10, trust_env=False) as http:
                release = await load_containers_release(http, load_workers_release())
        except (ContainersReleaseFetchError, ContainersReleaseUnavailable, WorkersReleaseUnavailable) as exc:
            raise SourceUnresolved("The signed Container release is unavailable") from exc
        if installation.containers_release_digest != release.digest:
            raise ObservationChanged("Container release changed before review")
        # Wheel distribution names identify imports where the normalized names
        # agree. Keep the known distribution aliases from the supported set.
        aliases = {"python_dotenv": "dotenv", "pyyaml": "yaml"}
        linux_packages = {}
        for wheel in release.manifest.wheels:
            root = re.sub(r"[-.]+", "_", wheel.partition("-")[0]).lower()
            module = aliases.get(root, root)
            if re.fullmatch(r"[a-z_][a-z0-9_]*", module) and module not in linux_packages:
                linux_packages[module] = wheel

    storage = _source_storage(install_id)

    async def read(path: str, max_bytes: int) -> bytes | None:
        try:
            content = await storage.read(path, max_bytes=max_bytes)
        except ValueError:
            if analysed_pins is not None:
                # A bounded read cannot hash an oversized object. Confirm
                # the full pin by streaming before recording a refusal.
                await check_pin(path, await storage.content_hash(path))
            raise AnalysisRejected("Workspace source exceeds its analysis read budget", "CF_PARSE_BUDGET") from None
        except StorageError as error:
            if error.kind != "not_found":
                raise
            content = None
        if analysed_pins is not None:
            await check_pin(path, hashlib.sha256(content).hexdigest() if content is not None else None)
        if sources is not None and content is not None:
            sources[path] = content
        return content

    async def check_pin(path: str, pin: str | None) -> None:
        if pin is None:
            async with get_db_context() as db:
                if path in await indexed_pins(db, {path}, install_id=install_id):
                    raise SourceUnresolved("Indexed workspace source is missing from storage")
        if expected_pins is not None and path in expected_pins and pin != expected_pins[path]:
            raise ObservationChanged("Observed workspace sources changed before analysis")
        if analysed_pins is not None:
            analysed_pins[path] = pin

    result = await analyze(
        read, entry_path, function_name, subrequest_allowance=subrequest_allowance, cpu_budget_ms=cpu_budget_ms,
        **({"linux_packages": linux_packages} if linux_packages is not None else {}),
    )
    findings = result.pop("linux_findings", None)
    declaration = ApprovalDeclaration.model_validate(result)
    merged_hosts = sorted(set(declaration.required_https_hosts) | set(reviewed_https_allowlist(https_hosts or [])))
    if len(merged_hosts) > 32:
        raise AnalysisRejected("The reviewed HTTPS allowlist exceeds 32 hosts", "CF_HOST_BUDGET")
    declaration = declaration.model_copy(update={"https_hosts": merged_hosts, "solution": solution})
    linux_review = None
    if installation is not None and release is not None:
        policy = ContainersPolicy.model_validate(installation.containers_policy)
        if workflow.memory_request_mib is None or not 0 < workflow.memory_request_mib < 4096:
            raise AnalysisRejected("Set an explicit workflow memory request below 4 GiB before Container review")
        if workflow.timeout_seconds + policy.idle_seconds >= policy.lifetime_seconds:
            raise AnalysisRejected("Workflow timeout exceeds the Containers lifetime allowance")
        linux_review = LinuxReview.model_validate({
            "findings": findings, "release_digest": release.digest, "installation_id": installation.id,
            "deployment_revision": installation.desired_revision, "consent_version": installation.consent_version,
            "worker_ingress_origin": installation.worker_ingress_origin, "egress": release.manifest.ingress_contract,
            "path": entry_path, "function_name": function_name, "source_pins": declaration.manifest,
            "wheels": {wheel: release.manifest.wheels[wheel] for wheel in declaration.packages},
            "solution": solution, "memory_bytes": workflow.memory_request_mib * 2**20,
            "timeout_seconds": workflow.timeout_seconds, "ingress_bytes": policy.ingress_bytes,
            "outbound_bytes": policy.outbound_bytes, "previous_revision": previous_revision,
            "inactive_history_count": history_count,
        })
    return ApprovalProposal(workflow_id, organization_id, previous_revision, declaration, history_count, linux_review)


async def accept_proposal(
    db: AsyncSession, proposal: ApprovalProposal, *, accepted_by: str,
) -> CloudflareApproval:
    """Serialize acceptance and revocation with a PostgreSQL FOR NO KEY UPDATE lock."""
    workflow = await lock_workflow(db, proposal.workflow_id)
    declaration = proposal.declaration
    current = await current_approval(db, proposal.workflow_id)
    solution = declaration.solution
    install_id = solution.install_id if solution else None
    linux = proposal.container
    if linux is not None:
        installation = await db.get(CloudflareInstallation, linux.installation_id, populate_existing=True)
        if (
            installation is None or installation.admitted_revision != linux.deployment_revision
            or installation.retired_at is not None or installation.containers_ready_revision != linux.deployment_revision
            or installation.containers_release_digest != linux.release_digest
            or installation.consent_version != linux.consent_version
            or installation.worker_ingress_origin != linux.worker_ingress_origin
            or workflow is None or workflow.timeout_seconds != linux.timeout_seconds
            or (workflow.memory_request_mib or 0) * 2**20 != linux.memory_bytes
        ):
            raise AnalysisRejected("Container policy or workflow limits changed during review")
    if (
        workflow is None or not workflow.is_active or workflow.type != "workflow"
        or workflow.execution_lane == "fast_path"
        or (workflow.organization_id, workflow.solution_id, workflow.path, workflow.function_name) != (
            proposal.organization_id, install_id, declaration.entry_path, declaration.function_name,
        )
        or (install_id is not None and await solution_approval_scope(db, install_id, proposal.organization_id) != solution)
        or (current.revision if current else None) != proposal.previous_revision
        or (current is None and await _history_count(db, proposal.workflow_id) != proposal.inactive_history_count)
        or await indexed_pins(db, set(declaration.manifest), install_id=install_id) != declaration.manifest
    ):
        raise AnalysisRejected("Workflow scope, source pins or current approval changed during review")

    try:
        async with db.begin_nested():
            if current is not None:
                current.is_current = False
                await db.flush()
            row = CloudflareApproval(
                revision=proposal.revision, workflow_id=proposal.workflow_id, organization_id=proposal.organization_id,
                policy_version=LINUX_REVIEW_VERSION if linux is not None else POLICY_VERSION,
                declaration=linux.model_dump(mode="json") if linux is not None else declaration.review_fields(),
                accepted_by=accepted_by,
            )
            db.add(row)
            await db.flush()
    except IntegrityError as exc:
        raise AnalysisRejected("Workflow scope, source pins or current approval changed during review") from exc
    return row


async def prepare_approved_workspace(
    *,
    workflow_id: str,
    entry_path: str | None,
    function_name: str | None,
    entry_sha256: str | None,
    expected_revision: str,
    solution_id: str | None = None,
) -> ApprovedWorkspace | None:
    """Check the active approval and send only the exact pinned UTF-8 bytes."""
    install_id = UUID(solution_id) if solution_id is not None else None
    matching = (
        select(CloudflareApproval)
        .join(Workflow, Workflow.id == CloudflareApproval.workflow_id)
        .where(
            CloudflareApproval.revision == expected_revision,
            CloudflareApproval.workflow_id == UUID(workflow_id),
            CloudflareApproval.policy_version == POLICY_VERSION,
            CloudflareApproval.is_current,
            CloudflareApproval.organization_id.is_not_distinct_from(Workflow.organization_id),
            Workflow.is_active, Workflow.solution_id.is_not_distinct_from(install_id), Workflow.type == "workflow",
            Workflow.path == entry_path, Workflow.function_name == function_name,
        )
    )
    async with get_db_context() as db:
        approval = await db.scalar(matching)
        if approval is None:
            return None
        declaration = ApprovalDeclaration.model_validate(approval.declaration)
        owner = approval.organization_id
        solution = declaration.solution
        if (solution.install_id if solution else None) != install_id:
            return None
        if install_id is not None and await solution_approval_scope(db, install_id, owner) != solution:
            return None
    if (
        (declaration.entry_path, declaration.function_name) != (entry_path, function_name)
        or (install_id is None and declaration.entry_sha256 != entry_sha256)
    ):
        return None

    manifest = declaration.manifest
    try:
        async with asyncio.timeout(20):
            sources = await read_pinned_sources(declaration)
    except (StorageError, TimeoutError, ObservationChanged):
        return None

    # A review or scope change can race the storage reads.
    async with get_db_context() as db:
        if await db.scalar(matching.with_only_columns(CloudflareApproval.revision)) is None:
            return None
        if install_id is not None and (
            await indexed_pins(db, set(manifest), install_id=install_id) != manifest
            or await solution_approval_scope(db, install_id, owner) != solution
        ):
            return None
    return ApprovedWorkspace({
        "manifest": manifest, "sources": sources,
        **({"solution_id": solution_id} if install_id is not None else {}),
        "packages": declaration.packages, "https_hosts": declaration.https_hosts,
        "subrequest_allowance": declaration.subrequest_allowance, "cpu_budget_ms": declaration.cpu_budget_ms,
    }, declaration.entry_sha256)
