"""Workflow review over the same approval fences used by execution."""

from collections import Counter
from dataclasses import asdict
from uuid import UUID

from bifrost.cloudflare_analysis import (
    POLICY_VERSION,
    AnalysisRejected,
    Finding,
    SourceUnresolved,
    digest,
)
from fastapi import HTTPException
from sqlalchemy import case, delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.constants import PROVIDER_ORG_ID
from src.core.principal import UserPrincipal
from src.core.storage.contracts import StorageError
from src.models.contracts.cloudflare_approval import (
    ApprovalDeclaration,
    CloudflareApprovalAccept,
    CloudflareApprovalDiagnostic,
    CloudflareApprovalList,
    CloudflareApprovalListItem,
    CloudflareApprovalOptions,
    CloudflareApprovalReview,
    CloudflareApprovalState,
    StoredCloudflareReview,
)
from src.models.orm import CloudflareApproval, Workflow
from src.services.audit import emit_audit
from src.services.authorization.enforce import (
    Caller,
    allows_permission,
    load_caller,
    user_reach,
)
from src.services.execution.cloudflare_eligibility import (
    ApprovalProposal,
    ObservationChanged,
    accept_proposal,
    approval_placement,
    current_approval,
    indexed_pins,
    lock_workflow,
    propose_approval,
    revoke_approval,
    workflow_review_scope,
)


def _can_propose(caller: Caller) -> bool:
    return not caller.is_execution_credential and allows_permission(caller, "platform.readwrite", None)


def _can_see(caller: Caller, workflow: Workflow, can_propose: bool) -> bool:
    return user_reach(caller).covers(workflow.organization_id) and (
        can_propose or allows_permission(caller, "workflows.read", workflow.organization_id)
    )


async def _authorized_workflow(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID, *, manage: bool, lock: bool = False,
) -> tuple[Workflow, bool]:
    caller = await load_caller(db, user)
    can_manage = _can_propose(caller)
    if manage and not can_manage:
        raise HTTPException(403, "Cloudflare approval requires a human Global operator")
    workflow = (
        await lock_workflow(db, workflow_id) if lock
        else await db.get(Workflow, workflow_id, populate_existing=True)
    )
    if workflow is None or not _can_see(caller, workflow, can_manage):
        raise HTTPException(404, "Workflow not found")
    return workflow, can_manage


def _diagnostic(error: AnalysisRejected) -> CloudflareApprovalDiagnostic:
    return CloudflareApprovalDiagnostic(**asdict(Finding.from_refusal(error)))


async def _state(
    db: AsyncSession, workflow: Workflow, can_manage: bool, *, include_review: bool = True,
    include_sources: bool = True,
) -> CloudflareApprovalState:
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
    if include_review:
        snapshot = CloudflareApproval.declaration
        if not include_sources:
            snapshot = snapshot.op("-")("sources")
        stored = await db.scalar(select(snapshot).where(
            CloudflareApproval.workflow_id == workflow.id, CloudflareApproval.is_review,
            CloudflareApproval.policy_version == POLICY_VERSION,
        ))
        if stored is not None:
            review = StoredCloudflareReview.model_validate(stored)
            if (
                review.scope == workflow_review_scope(workflow)
                and review.solution == solution
                and review.current_revision == state.current_revision
                and await _observations_match(db, workflow, review.observed_pins)
            ):
                if state.status != "approved":
                    state.status = review.status
                if can_manage:
                    state.proposal = review.proposal
                    if include_sources:
                        state.proposal_sources = review.sources
                    state.diagnostics = review.diagnostics
            elif state.status == "pending_review" and review.scope == workflow_review_scope(workflow):
                state.status = "revision_changed"
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


async def _observations_match(
    db: AsyncSession, workflow: Workflow, observations: dict[str, str | None],
) -> bool:
    indexed = await indexed_pins(db, set(observations), install_id=workflow.solution_id)
    return all(indexed.get(path) == pin for path, pin in observations.items())


async def list_approvals(db: AsyncSession, user: UserPrincipal) -> CloudflareApprovalList:
    caller = await load_caller(db, user)
    can_propose = _can_propose(caller)
    reach = user_reach(caller).where(Workflow.organization_id)
    scope_order = case(
        (Workflow.organization_id == PROVIDER_ORG_ID, 0),
        (Workflow.organization_id.is_(None), 1),
        else_=2,
    )
    query = select(Workflow).where(Workflow.is_active, Workflow.type == "workflow").order_by(
        scope_order, Workflow.name, Workflow.id,
    )
    if reach is not None:
        query = query.where(reach)
    items = []
    for workflow in await db.scalars(query):
        if not _can_see(caller, workflow, can_propose):
            continue
        state = await _state(db, workflow, can_propose, include_sources=False)
        owner = workflow.organization_id
        items.append(CloudflareApprovalListItem(
            workflow_id=workflow.id, name=workflow.display_name or workflow.name, organization_id=owner,
            scope="provider" if owner == PROVIDER_ORG_ID else "global" if owner is None else "organization",
            status=state.status, has_proposal=state.proposal is not None,
        ))
    counts = Counter(item.status for item in items)
    return CloudflareApprovalList(
        counts={key: counts[key] for key in (
            "approved", "pending_review", "revision_changed", "ineligible", "temporarily_unavailable",
        )},
        workflows=items,
        can_propose=can_propose,
    )


async def _save_review(
    db: AsyncSession, workflow: Workflow, state: CloudflareApprovalState, observed_pins: dict[str, str | None],
) -> None:
    try:
        solution = await approval_placement(db, workflow)
    except ObservationChanged:
        return  # Placement refusals are available directly from the workflow state.
    if not await _observations_match(db, workflow, observed_pins):
        return
    review = StoredCloudflareReview(
        scope=workflow_review_scope(workflow), solution=solution, current_revision=state.current_revision,
        observed_pins=observed_pins, status=state.status, proposal=state.proposal,
        sources=state.proposal_sources, diagnostics=state.diagnostics,
    )
    stmt = insert(CloudflareApproval).values(
        revision=digest(["pending-workflow-review", str(workflow.id)]),
        workflow_id=workflow.id, organization_id=workflow.organization_id, policy_version=POLICY_VERSION,
        declaration=review.model_dump(mode="json"), is_current=False, is_review=True,
    )
    await db.execute(stmt.on_conflict_do_update(
        index_elements=[CloudflareApproval.revision],
        set_={
            "declaration": stmt.excluded.declaration,
            "organization_id": stmt.excluded.organization_id,
            "policy_version": stmt.excluded.policy_version,
        },
    ))


async def _clear_review(db: AsyncSession, workflow_id: UUID) -> None:
    await db.execute(delete(CloudflareApproval).where(
        CloudflareApproval.workflow_id == workflow_id, CloudflareApproval.is_review,
    ))


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
    observed_pins: dict[str, str | None] = {}
    sources: dict[str, bytes] = {}
    try:
        candidate = await propose_approval(
            workflow_id, subrequest_allowance=options.subrequest_allowance,
            cpu_budget_ms=options.cpu_budget_ms, https_hosts=options.https_hosts, analysed_pins=observed_pins, expected_scope=scope,
            sources=None if accepting else sources,
        )
    except (StorageError, TimeoutError, SourceUnresolved):
        diagnostic = _temporary()
    except AnalysisRejected as error:
        diagnostic = _diagnostic(error)

    workflow, can_manage = await _authorized_workflow(
        db, user, workflow_id, manage=True, lock=True,
    )
    state = await _state(db, workflow, can_manage, include_review=False)
    changed_during_review = workflow_review_scope(workflow) != scope
    if changed_during_review:
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
            changed_during_review = True
            state.status = "revision_changed"
            diagnostic = _diagnostic(AnalysisRejected("Workflow scope, source or approval changed during review"))
    if diagnostic is not None:
        state.diagnostics = [diagnostic]
        if diagnostic.rule_id == "CF_ANALYSIS_UNAVAILABLE":
            state.status = "temporarily_unavailable"
        elif state.status != "approved" and not changed_during_review:
            state.status = "ineligible"
        if accepting:
            raise _review_error(state, diagnostic.message)
        if not changed_during_review:
            await _save_review(db, workflow, state, observed_pins)
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
        await _clear_review(db, workflow_id)
        state = await _state(db, workflow, can_manage, include_review=False)
    state.proposal = _review(candidate)
    state.diagnostics = _url_diagnostics(candidate.declaration)
    if not accepting:
        state.proposal_sources = {path: sources[path].decode("utf-8") for path in candidate.declaration.manifest}
        await _save_review(db, workflow, state, observed_pins)
    return state


async def revoke_review(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
) -> CloudflareApprovalState:
    workflow, can_manage = await _authorized_workflow(db, user, workflow_id, manage=True, lock=True)
    try:
        revision = await revoke_approval(db, workflow_id)
        await db.flush()
    except AnalysisRejected as error:
        raise _revision_conflict(await _state(db, workflow, can_manage), str(error)) from error
    await _clear_review(db, workflow_id)
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
