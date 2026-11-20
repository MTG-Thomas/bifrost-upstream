"""Workflow review over the same approval fences used by execution."""

from dataclasses import asdict
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bifrost.cloudflare_analysis import POLICY_VERSION, AnalysisRejected, Finding, SourceUnresolved
from src.core.principal import UserPrincipal
from src.core.storage.contracts import StorageError
from src.models.contracts.cloudflare_approval import (
    ApprovalDeclaration,
    CloudflareApprovalAccept,
    CloudflareApprovalDiagnostic,
    CloudflareApprovalOptions,
    CloudflareApprovalReview,
    CloudflareApprovalState,
)
from src.models.orm import Solution, Workflow
from src.services.authorization.enforce import load_caller, require_permission, user_reach
from src.services.execution.cloudflare_eligibility import (
    ApprovalProposal,
    ObservationChanged,
    accept_proposal,
    current_approval,
    indexed_pins,
    propose_approval,
    revoke_approval,
    solution_approval_scope,
)


async def _authorized_workflow(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID, *, manage: bool,
) -> tuple[Workflow, bool]:
    caller = await load_caller(db, user)
    can_manage = (
        caller.ctx is not None and not caller.is_execution_credential
        and (caller.is_platform_admin or user.is_provider_org)
    )
    if manage and not can_manage:
        raise HTTPException(403, "Cloudflare approval requires a Global operator")
    workflow = await db.get(Workflow, workflow_id, populate_existing=True)
    if workflow is None or (not can_manage and not user_reach(caller).covers(workflow.organization_id)):
        raise HTTPException(404, "Workflow not found")
    if not can_manage:
        require_permission(caller, "workflows.read", workflow.organization_id)
    return workflow, can_manage


def _diagnostic(error: AnalysisRejected) -> CloudflareApprovalDiagnostic:
    return CloudflareApprovalDiagnostic(**asdict(Finding.from_refusal(error)))


def _placement_refusal(workflow: Workflow) -> AnalysisRejected | None:
    if not workflow.is_active:
        return AnalysisRejected("Workflow is inactive")
    if workflow.type != "workflow":
        return AnalysisRejected("Only workflows can be approved", "CF_EXECUTABLE_TYPE")
    if workflow.execution_lane == "fast_path":
        return AnalysisRejected("Cloudflare supplies Normal capacity only", "CF_FAST_PATH")
    return None


async def _state(db: AsyncSession, workflow: Workflow, can_manage: bool) -> CloudflareApprovalState:
    current = await current_approval(db, workflow.id)
    state = CloudflareApprovalState(
        status="pending_review", can_manage=can_manage,
    )
    if current is not None:
        state.current_revision = current.revision
        declaration = ApprovalDeclaration.model_validate(current.declaration)
        install_id = declaration.solution.install_id if declaration.solution else None
        if (current.organization_id, install_id) == (workflow.organization_id, workflow.solution_id):
            state.current_declaration = declaration
    refusal = _placement_refusal(workflow)
    if refusal is not None:
        state.status = "ineligible"
        state.diagnostics = [_diagnostic(refusal)]
        return state
    solution = None
    if workflow.solution_id is not None:
        solution = await solution_approval_scope(db, workflow.solution_id, workflow.organization_id)
        if solution is None:
            outbound = await db.scalar(
                select(Solution.allow_outbound_access).where(Solution.id == workflow.solution_id),
            )
            state.status = "ineligible"
            state.diagnostics = [_diagnostic(
                AnalysisRejected("The Solution install allows outbound access", "CF_OUTBOUND")
                if outbound else AnalysisRejected("The Solution has no eligible published source"),
            )]
            return state
    if current is not None:
        approved = state.current_declaration
        state.status = "revision_changed"
        if (
            approved is not None and current.policy_version == POLICY_VERSION
            and (approved.entry_path, approved.function_name) == (workflow.path, workflow.function_name)
            and approved.solution == solution
            and await indexed_pins(db, set(approved.manifest), install_id=workflow.solution_id) == approved.manifest
        ):
            state.status = "approved"
    return state


def _review(proposal: ApprovalProposal) -> CloudflareApprovalReview:
    return CloudflareApprovalReview.model_validate({
        **proposal.review_document(),
        "review_sha256": proposal.revision,
        "wildcard_https_hosts": [host for host in proposal.declaration.https_hosts if host.startswith("*.")],
    })


def _url_diagnostics(declaration: ApprovalDeclaration) -> list[CloudflareApprovalDiagnostic]:
    if not declaration.unresolved_url_calls:
        return []
    return [_diagnostic(AnalysisRejected(
        f"{declaration.unresolved_url_calls} outbound URL call(s) require hostname review", "CF_URL_REVIEW",
    ))]


def _temporary() -> CloudflareApprovalDiagnostic:
    return CloudflareApprovalDiagnostic(
        rule_id="CF_ANALYSIS_UNAVAILABLE",
        severity="warning",
        message="Cloudflare analysis could not complete.",
        hint="Retry after analysis capacity or workflow source storage is available.",
    )


async def get_approval_state(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
) -> CloudflareApprovalState:
    workflow, can_manage = await _authorized_workflow(db, user, workflow_id, manage=False)
    return await _state(db, workflow, can_manage)


async def review_approval(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID, options: CloudflareApprovalOptions,
) -> CloudflareApprovalState:
    accepting = isinstance(options, CloudflareApprovalAccept)
    workflow, _ = await _authorized_workflow(
        db, user, workflow_id, manage=accepting or bool(options.https_hosts),
    )
    source_scope = (workflow.organization_id, workflow.solution_id, workflow.path, workflow.function_name)
    candidate = None
    diagnostic = None
    try:
        candidate = await propose_approval(
            workflow_id, subrequest_allowance=options.subrequest_allowance,
            cpu_budget_ms=options.cpu_budget_ms, https_hosts=options.https_hosts, analysed_pins={},
        )
    except (StorageError, TimeoutError, SourceUnresolved):
        diagnostic = _temporary()
    except ObservationChanged:
        diagnostic = _diagnostic(AnalysisRejected("Workflow scope or source changed during review"))
    except AnalysisRejected as error:
        diagnostic = _diagnostic(error)

    workflow, can_manage = await _authorized_workflow(
        db, user, workflow_id, manage=accepting or bool(options.https_hosts),
    )
    state = await _state(db, workflow, can_manage)
    if source_scope != (workflow.organization_id, workflow.solution_id, workflow.path, workflow.function_name):
        candidate = None
        diagnostic = _diagnostic(AnalysisRejected("Workflow scope or source changed during review"))
    if diagnostic is not None:
        state.diagnostics = [diagnostic]
        if diagnostic.rule_id == "CF_ANALYSIS_UNAVAILABLE":
            state.status = "temporarily_unavailable"
        elif state.status == "pending_review":
            state.status = "ineligible"
        if accepting:
            raise HTTPException(
                503 if state.status == "temporarily_unavailable" else 409,
                detail=state.model_dump(mode="json"),
            )
        return state

    assert candidate is not None
    if isinstance(options, CloudflareApprovalAccept):
        if candidate.revision != options.review_sha256:
            raise HTTPException(409, "The proposal changed. Generate and review a new proposal.")
        try:
            await accept_proposal(db, candidate, accepted_by=str(user.user_id))
        except AnalysisRejected as error:
            raise HTTPException(409, str(error)) from error
        state = await _state(db, workflow, can_manage)
    state.proposal = _review(candidate)
    state.diagnostics = _url_diagnostics(candidate.declaration)
    return state


async def revoke_review(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
) -> CloudflareApprovalState:
    workflow, can_manage = await _authorized_workflow(db, user, workflow_id, manage=True)
    try:
        await revoke_approval(db, workflow_id)
        await db.flush()
    except AnalysisRejected as error:
        raise HTTPException(409, str(error)) from error
    return await _state(db, workflow, can_manage)
