"""Review workspace proposals and verify approved sources before an opaque send."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bifrost.cloudflare_analysis import (
    MAX_BUNDLE_BYTES,
    MAX_CPU_MS,
    MAX_MODULES,
    MAX_SOURCE_BYTES,
    MAX_SUBREQUESTS,
    POLICY_VERSION,
    AnalysisRejected,
    SourceUnresolved,
    analyze,
    digest,
)
from src.core.database import get_db_context
from src.core.storage.contracts import StorageError
from src.models.orm import (
    CloudflareApproval,
    FileIndex,
    Solution,
    SolutionFileIndex,
    Workflow,
)
from src.services.execution.cloudflare_hosts import reviewed_https_allowlist
from src.services.repo_storage import RepoStorage
from src.services.solutions.storage import SolutionStorage

_SourceDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class SolutionApprovalScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    install_id: UUID
    version: str | None
    source_generation: UUID


class ApprovalDeclaration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_path: str
    function_name: str
    entry_sha256: _SourceDigest
    module_sha256: dict[str, _SourceDigest] = Field(max_length=MAX_MODULES - 1)
    manifest_sha256: _SourceDigest
    packages: list[str] = Field(max_length=32)
    https_hosts: list[str] = Field(max_length=32)
    required_https_hosts: list[str] = Field(max_length=32)
    unresolved_url_calls: int = Field(default=0, strict=True, ge=0)
    estimated_subrequests: int = Field(strict=True, ge=0)
    subrequest_allowance: int = Field(strict=True, ge=0, le=MAX_SUBREQUESTS)
    estimated_cpu_ms: int = Field(strict=True, ge=0)
    cpu_budget_ms: int = Field(strict=True, gt=0, le=MAX_CPU_MS)
    solution: SolutionApprovalScope | None = None

    @property
    def manifest(self) -> dict[str, str]:
        return {**self.module_sha256, self.entry_path: self.entry_sha256}

    def review_fields(self) -> dict[str, object]:
        fields = self.model_dump(mode="json")
        if self.solution is None:
            fields.pop("solution")
        return fields


@dataclass(frozen=True)
class ApprovalProposal:
    workflow_id: UUID
    organization_id: UUID | None
    previous_revision: str | None
    declaration: ApprovalDeclaration
    inactive_history_count: int = 0

    def review_document(self) -> dict[str, object]:
        return {
            "policy_version": POLICY_VERSION,
            "workflow_id": str(self.workflow_id),
            "organization_id": str(self.organization_id) if self.organization_id else None,
            "previous_revision": self.previous_revision,
            "inactive_history_count": self.inactive_history_count,
            **self.declaration.review_fields(),
        }

    @property
    def revision(self) -> str:
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
        select(func.count()).select_from(CloudflareApproval).where(CloudflareApproval.workflow_id == workflow_id)
    ) or 0


async def _lock_workflow(db: AsyncSession, workflow_id: UUID) -> Workflow | None:
    # PostgreSQL FOR NO KEY UPDATE serializes review writers without
    # conflicting with foreign-key checks on the workflow row.
    return await db.get(Workflow, workflow_id, with_for_update={"key_share": True}, populate_existing=True)


async def revoke_approval(db: AsyncSession, workflow_id: UUID) -> str | None:
    if await _lock_workflow(db, workflow_id) is None:
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


async def propose_approval(
    workflow_id: UUID, *, subrequest_allowance: int, cpu_budget_ms: int,
    expected_pins: dict[str, str | None] | None = None,
    analysed_pins: dict[str, str | None] | None = None,
    https_hosts: list[str] | None = None,
) -> ApprovalProposal:
    async with get_db_context() as db:
        workflow = await db.get(Workflow, workflow_id)
        if workflow is None or not workflow.is_active or workflow.type != "workflow":
            raise ObservationChanged("Expected an active workflow")
        install_id = workflow.solution_id
        solution = None
        if install_id is not None:
            solution = await solution_approval_scope(db, install_id, workflow.organization_id)
            if solution is None:
                raise ObservationChanged("Expected an active Solution with published install-local source")
        current = await current_approval(db, workflow_id)
        previous_revision = current.revision if current else None
        history_count = 0 if current else await _history_count(db, workflow_id)
        entry_path, function_name, organization_id = workflow.path, workflow.function_name, workflow.organization_id

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
    )
    declaration = ApprovalDeclaration.model_validate(result)
    merged_hosts = sorted(set(declaration.required_https_hosts) | set(reviewed_https_allowlist(https_hosts or [])))
    if len(merged_hosts) > 32:
        raise AnalysisRejected("The reviewed HTTPS allowlist exceeds 32 hosts")
    declaration = declaration.model_copy(update={"https_hosts": merged_hosts, "solution": solution})
    return ApprovalProposal(workflow_id, organization_id, previous_revision, declaration, history_count)


async def accept_proposal(
    db: AsyncSession, proposal: ApprovalProposal, *, accepted_by: str,
) -> CloudflareApproval:
    """Serialize acceptance and revocation with a PostgreSQL FOR NO KEY UPDATE lock."""
    workflow = await _lock_workflow(db, proposal.workflow_id)
    declaration = proposal.declaration
    current = await current_approval(db, proposal.workflow_id)
    solution = declaration.solution
    install_id = solution.install_id if solution else None
    if (
        workflow is None or not workflow.is_active or workflow.type != "workflow"
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
                policy_version=POLICY_VERSION, declaration=declaration.review_fields(), accepted_by=accepted_by,
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

    sources: dict[str, str] = {}
    remaining = MAX_BUNDLE_BYTES
    storage = _source_storage(install_id)
    manifest = declaration.manifest
    if digest(manifest) != declaration.manifest_sha256:
        return None
    try:
        async with asyncio.timeout(20):
            for path, sha256 in manifest.items():
                content = await storage.read(path, max_bytes=min(remaining, MAX_SOURCE_BYTES))
                if hashlib.sha256(content).hexdigest() != sha256:
                    return None
                sources[path] = content.decode("utf-8")
                remaining -= len(content)
    except (StorageError, TimeoutError, ValueError):
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
