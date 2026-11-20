"""Workflow review over the same approval fences used by execution."""

from dataclasses import asdict
from uuid import UUID

from bifrost.cloudflare_analysis import (
    POLICY_VERSION,
    AnalysisRejected,
    Finding,
    SourceUnresolved,
)
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

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
from src.models.orm import Workflow
from src.services.audit import emit_audit
from src.services.authorization.enforce import (
    allows_permission,
    load_caller,
    require_permission,
    user_reach,
)
from src.services.execution.cloudflare_eligibility import (
    ApprovalProposal,
    ObservationChanged,
    accept_proposal,
    approval_placement,
    current_approval,
    indexed_pins,
    propose_approval,
    revoke_approval,
    workflow_review_scope,
)


async def _authorized_workflow(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID, *, manage: bool,
) -> tuple[Workflow, bool]:
    caller = await load_caller(db, user)
    operator_permission = "platform.readwrite"
    can_manage = not caller.is_execution_credential and allows_permission(caller, operator_permission, None)
    if manage:
        if caller.is_execution_credential:
            raise HTTPException(403, "Cloudflare approval requires a human Global operator")
        require_permission(caller, operator_permission, None)
    workflow = await db.get(Workflow, workflow_id, populate_existing=True)
    if workflow is None or (not can_manage and not user_reach(caller).covers(workflow.organization_id)):
        raise HTTPException(404, "Workflow not found")
    if not can_manage:
        require_permission(caller, "workflows.read", workflow.organization_id)
    return workflow, can_manage


def _diagnostic(error: AnalysisRejected) -> CloudflareApprovalDiagnostic:
    return CloudflareApprovalDiagnostic(**asdict(Finding.from_refusal(error)))


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
    try:
        solution = await approval_placement(db, workflow)
    except ObservationChanged as error:
        state.status = "ineligible"
        state.diagnostics = [_diagnostic(error)]
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
        db, user, workflow_id, manage=True,
    )
    scope = workflow_review_scope(workflow)
    candidate = None
    diagnostic = None
    try:
        candidate = await propose_approval(
            workflow_id, subrequest_allowance=options.subrequest_allowance,
            cpu_budget_ms=options.cpu_budget_ms, https_hosts=options.https_hosts, analysed_pins={}, expected_scope=scope,
        )
    except (StorageError, TimeoutError, SourceUnresolved):
        diagnostic = _temporary()
    except AnalysisRejected as error:
        diagnostic = _diagnostic(error)

    workflow, can_manage = await _authorized_workflow(
        db, user, workflow_id, manage=True,
    )
    state = await _state(db, workflow, can_manage)
    if workflow_review_scope(workflow) != scope:
        candidate = None
        state.status = "revision_changed"
        diagnostic = _diagnostic(AnalysisRejected("Workflow scope changed during review"))
    if candidate is not None:
        declaration = candidate.declaration
        solution = declaration.solution
        try:
            placement_matches = await approval_placement(db, workflow) == solution
        except ObservationChanged:
            placement_matches = False
        if (
            (candidate.organization_id, solution.install_id if solution else None,
             declaration.entry_path, declaration.function_name)
            != scope
            or not placement_matches
            or candidate.previous_revision != state.current_revision
            or await indexed_pins(db, set(declaration.manifest), install_id=workflow.solution_id)
            != declaration.manifest
        ):
            candidate = None
            state.status = "revision_changed"
            diagnostic = _diagnostic(AnalysisRejected("Workflow scope, source or approval changed during review"))
    if diagnostic is not None:
        state.diagnostics = [diagnostic]
        if diagnostic.rule_id == "CF_ANALYSIS_UNAVAILABLE":
            state.status = "temporarily_unavailable"
        elif state.status == "pending_review":
            state.status = "ineligible"
        if accepting:
            raise _review_error(state, diagnostic.message)
        return state

    assert candidate is not None
    if isinstance(options, CloudflareApprovalAccept):
        if candidate.revision != options.review_sha256:
            raise _revision_conflict(state, "The proposal changed. Generate and review a new proposal.")
        try:
            await accept_proposal(db, candidate, accepted_by=str(user.user_id))
        except AnalysisRejected as error:
            raise _revision_conflict(state, str(error)) from error
        await emit_audit(
            db, "workflow.cloudflare_approval.accept", resource_type="workflow", resource_id=workflow_id,
            details={"revision": candidate.revision},
        )
        state = await _state(db, workflow, can_manage)
    state.proposal = _review(candidate)
    state.diagnostics = _url_diagnostics(candidate.declaration)
    return state


async def revoke_review(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
) -> CloudflareApprovalState:
    workflow, can_manage = await _authorized_workflow(db, user, workflow_id, manage=True)
    try:
        revision = await revoke_approval(db, workflow_id)
        await db.flush()
    except AnalysisRejected as error:
        raise _revision_conflict(await _state(db, workflow, can_manage), str(error)) from error
    if revision is not None:
        await emit_audit(
            db, "workflow.cloudflare_approval.revoke", resource_type="workflow", resource_id=workflow_id,
            details={"revision": revision},
        )
    return await _state(db, workflow, can_manage)


def _review_error(state: CloudflareApprovalState, message: str) -> HTTPException:
    return HTTPException(
        503 if state.status == "temporarily_unavailable" else 409,
        detail={"message": message, "state": state.model_dump(mode="json")},
    )


def _revision_conflict(state: CloudflareApprovalState, message: str) -> HTTPException:
    state.status = "revision_changed"
    state.diagnostics = [_diagnostic(AnalysisRejected(message))]
    return _review_error(state, message)
