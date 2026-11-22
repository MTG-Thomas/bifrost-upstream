"""Workflow review over the same approval fences used by execution."""

from collections import Counter, defaultdict
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
from sqlalchemy import case, delete, func, or_, select, tuple_
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
    CloudflareApprovalStatus,
    StoredCloudflareReview,
)
from src.models.orm import CloudflareApproval, Solution, SolutionFileIndex, Workflow
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
    indexed_pins,
    lock_workflow,
    propose_approval,
    read_pinned_sources,
    resolve_approval_placement,
    revoke_approval,
    solution_placement_query,
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
) -> tuple[Workflow, Caller, bool]:
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
    return workflow, caller, can_manage


def _diagnostic(error: AnalysisRejected) -> CloudflareApprovalDiagnostic:
    return CloudflareApprovalDiagnostic(**asdict(Finding.from_refusal(error)))


_BUDGET_RULES = {"CF_CPU", "CF_SUBREQUESTS"}
_TEMPORARY_RULE = "CF_ANALYSIS_UNAVAILABLE"


def _state_from_observations(
    workflow: Workflow, can_manage: bool, current: CloudflareApproval | None,
    declaration: ApprovalDeclaration | None, review: StoredCloudflareReview | None,
    solution: Solution | None, pins: dict[str, str | None],
) -> CloudflareApprovalState:
    state = CloudflareApprovalState(status="pending_review", can_manage=can_manage)
    if current is not None:
        state.current_revision = current.revision
        if declaration is not None:
            install_id = declaration.solution.install_id if declaration.solution else None
            if (current.organization_id, install_id) == (workflow.organization_id, workflow.solution_id):
                state.current_declaration = declaration
    try:
        placement = resolve_approval_placement(workflow, solution)
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
            and approved.solution == placement
            and all(pins.get(path) == pin for path, pin in approved.manifest.items())
        ):
            state.status = "approved"
    if review is not None and review.status != "temporarily_unavailable":
        scope_matches = review.scope == workflow_review_scope(workflow)
        if (
            scope_matches and review.solution == placement and review.current_revision == state.current_revision
            and all(pins.get(path) == pin for path, pin in review.observed_pins.items())
        ):
            if state.status != "approved":
                needs_budget = any(item.rule_id in _BUDGET_RULES for item in review.diagnostics)
                state.status = "pending_review" if needs_budget else review.status
            if can_manage:
                state.proposal = review.proposal
                state.diagnostics = review.diagnostics
        elif state.status == "pending_review" and scope_matches:
            state.status = "revision_changed"
    return state


async def _states(
    db: AsyncSession, workflows: list[Workflow], can_manage: bool, *, include_review: bool = True,
) -> list[CloudflareApprovalState]:
    if not workflows:
        return []
    workflow_ids = [workflow.id for workflow in workflows]
    approval_filter = (
        or_(CloudflareApproval.is_current, CloudflareApproval.is_review & (CloudflareApproval.policy_version == POLICY_VERSION))
        if include_review else CloudflareApproval.is_current
    )
    rows = await db.scalars(select(CloudflareApproval).where(
        CloudflareApproval.workflow_id.in_(workflow_ids), approval_filter,
    ))
    current: dict[UUID, CloudflareApproval] = {}
    declarations: dict[UUID, ApprovalDeclaration] = {}
    reviews: dict[UUID, StoredCloudflareReview] = {}
    for row in rows:
        if row.is_current:
            current[row.workflow_id] = row
            declarations[row.workflow_id] = ApprovalDeclaration.model_validate(row.declaration)
        else:
            reviews[row.workflow_id] = StoredCloudflareReview.model_validate(row.declaration)

    paths: dict[UUID | None, set[str]] = defaultdict(set)
    install_ids = set()
    for workflow in workflows:
        install_id = workflow.solution_id
        if install_id is not None:
            install_ids.add(install_id)
        if (declaration := declarations.get(workflow.id)) is not None:
            paths[install_id].update(declaration.manifest)
        if (review := reviews.get(workflow.id)) is not None:
            paths[install_id].update(review.observed_pins)

    solutions = {}
    if install_ids:
        solutions = {row.id: row for row in await db.scalars(solution_placement_query().where(Solution.id.in_(install_ids)))}
    pins: dict[UUID | None, dict[str, str | None]] = defaultdict(dict)
    if loose_paths := paths.pop(None, None):
        pins[None] = await indexed_pins(db, loose_paths)
    installed_paths = [(install_id, path) for install_id, names in paths.items() for path in names]
    if installed_paths:
        installed = await db.execute(select(
            SolutionFileIndex.solution_id, SolutionFileIndex.path, SolutionFileIndex.content_hash,
        ).where(tuple_(SolutionFileIndex.solution_id, SolutionFileIndex.path).in_(installed_paths)))
        for install_id, path, pin in installed:
            pins[install_id][path] = pin

    return [
        _state_from_observations(
            workflow, can_manage, current.get(workflow.id), declarations.get(workflow.id), reviews.get(workflow.id),
            solutions.get(workflow.solution_id) if workflow.solution_id else None, pins[workflow.solution_id],
        )
        for workflow in workflows
    ]


async def _state(
    db: AsyncSession, workflow: Workflow, can_manage: bool, *, include_review: bool = True,
) -> CloudflareApprovalState:
    return (await _states(db, [workflow], can_manage, include_review=include_review))[0]


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
        rule_id=_TEMPORARY_RULE,
        severity="warning",
        message="Cloudflare analysis could not complete.",
        hint="Retry after analysis capacity or workflow source storage is available.",
    )


async def _observations_match(
    db: AsyncSession, workflow: Workflow, observations: dict[str, str | None],
) -> bool:
    indexed = await indexed_pins(db, set(observations), install_id=workflow.solution_id)
    return all(indexed.get(path) == pin for path, pin in observations.items())


async def _visible_workflows(db: AsyncSession, caller: Caller, *, current_only: bool = False) -> list[Workflow]:
    can_propose = _can_propose(caller)
    reach = user_reach(caller).where(Workflow.organization_id)
    scope_order = case(
        (Workflow.organization_id == PROVIDER_ORG_ID, 0),
        (Workflow.organization_id.is_(None), 1),
        else_=2,
    )
    query = select(Workflow).where(Workflow.is_active, Workflow.type == "workflow").order_by(
        scope_order, func.coalesce(func.nullif(Workflow.display_name, ""), Workflow.name), Workflow.id,
    )
    if reach is not None:
        query = query.where(reach)
    if current_only:
        query = query.where(select(CloudflareApproval.revision).where(
            CloudflareApproval.workflow_id == Workflow.id, CloudflareApproval.is_current,
            CloudflareApproval.policy_version == POLICY_VERSION,
        ).exists())
    return [workflow for workflow in await db.scalars(query) if _can_see(caller, workflow, can_propose)]


async def current_visible_approval_count(db: AsyncSession, caller: Caller) -> int:
    workflows = await _visible_workflows(db, caller, current_only=True)
    states = await _states(db, workflows, _can_propose(caller), include_review=False)
    return sum(state.status == "approved" for state in states)


async def list_approvals(db: AsyncSession, user: UserPrincipal) -> CloudflareApprovalList:
    caller = await load_caller(db, user)
    can_propose = _can_propose(caller)
    workflows = await _visible_workflows(db, caller)
    states = await _states(db, workflows, can_propose)
    items = []
    for workflow, state in zip(workflows, states, strict=True):
        owner = workflow.organization_id
        items.append(CloudflareApprovalListItem(
            workflow_id=workflow.id, name=workflow.display_name or workflow.name, organization_id=owner,
            scope="provider" if owner == PROVIDER_ORG_ID else "global" if owner is None else "organization",
            status=state.status, has_proposal=state.proposal is not None,
            needs_larger_budget=any(item.rule_id in _BUDGET_RULES for item in state.diagnostics),
        ))
    statuses: tuple[CloudflareApprovalStatus, ...] = (
        "approved", "pending_review", "revision_changed", "ineligible", "temporarily_unavailable",
    )
    counts = Counter(item.status for item in items)
    return CloudflareApprovalList(
        counts={key: counts[key] for key in statuses},
        workflows=items,
        can_propose=can_propose,
    )


async def _save_review(
    db: AsyncSession, workflow: Workflow, state: CloudflareApprovalState, observed_pins: dict[str, str | None],
) -> bool:
    try:
        solution = await approval_placement(db, workflow)
    except ObservationChanged:
        return False
    if not await _observations_match(db, workflow, observed_pins):
        return False
    review = StoredCloudflareReview(
        scope=workflow_review_scope(workflow), solution=solution, current_revision=state.current_revision,
        observed_pins=observed_pins, status=state.status, proposal=state.proposal,
        diagnostics=state.diagnostics,
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
            "accepted_at": func.now(),
        },
    ))
    return True


async def _clear_review(db: AsyncSession, workflow_id: UUID) -> None:
    await db.execute(delete(CloudflareApproval).where(
        CloudflareApproval.workflow_id == workflow_id, CloudflareApproval.is_review,
    ))


async def get_approval_state(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
) -> CloudflareApprovalState:
    workflow, caller, can_manage = await _authorized_workflow(db, user, workflow_id, manage=False)
    state = await _state(db, workflow, can_manage)
    proposal = state.proposal
    if proposal is not None and allows_permission(caller, "repository.read", None):
        scope = workflow_review_scope(workflow)
        try:
            sources = await read_pinned_sources(proposal)
            workflow, caller, can_manage = await _authorized_workflow(db, user, workflow_id, manage=False)
            if workflow_review_scope(workflow) != scope or await approval_placement(db, workflow) != proposal.solution:
                raise ObservationChanged("Reviewed source changed")
            state = await _state(db, workflow, can_manage)
            if state.proposal and state.proposal.review_sha256 == proposal.review_sha256:
                if allows_permission(caller, "repository.read", None):
                    state.proposal_sources = sources
            elif state.proposal is not None:
                raise ObservationChanged("Reviewed source changed")
        except ObservationChanged as exc:
            state.status = "revision_changed"
            state.proposal = None
            state.diagnostics = [_diagnostic(exc)]
        except (StorageError, TimeoutError):
            state.status = "temporarily_unavailable"
            state.proposal = None
            state.diagnostics = [_temporary()]
    return state


async def review_approval(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
    options: CloudflareApprovalOptions | CloudflareApprovalAccept,
) -> CloudflareApprovalState:
    accepting = isinstance(options, CloudflareApprovalAccept)
    workflow, caller, _ = await _authorized_workflow(
        db, user, workflow_id, manage=True,
    )
    scope = workflow_review_scope(workflow)
    candidate = None
    diagnostic = None
    observed_pins: dict[str, str | None] = {}
    sources: dict[str, bytes] | None = (
        {} if not accepting and allows_permission(caller, "repository.read", None) else None
    )
    try:
        candidate = await propose_approval(
            workflow_id, subrequest_allowance=options.subrequest_allowance,
            cpu_budget_ms=options.cpu_budget_ms, https_hosts=options.https_hosts, analysed_pins=observed_pins, expected_scope=scope,
            sources=sources,
        )
    except (StorageError, TimeoutError, SourceUnresolved):
        diagnostic = _temporary()
    except AnalysisRejected as error:
        diagnostic = _diagnostic(error)

    workflow, caller, can_manage = await _authorized_workflow(
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
        if diagnostic.rule_id == _TEMPORARY_RULE:
            state.status = "temporarily_unavailable"
        elif state.status != "approved" and not changed_during_review:
            state.status = "pending_review" if diagnostic.rule_id in _BUDGET_RULES else "ineligible"
        if accepting:
            raise _review_error(state, diagnostic.message)
        if not changed_during_review and diagnostic.rule_id != _TEMPORARY_RULE:
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
        if await _save_review(db, workflow, state, observed_pins):
            await emit_audit(
                db, "workflow.cloudflare_approval.propose", resource_type="workflow", resource_id=workflow_id,
                details={"revision": candidate.revision},
            )
            if sources is not None and allows_permission(caller, "repository.read", None):
                state.proposal_sources = {path: sources[path].decode("utf-8") for path in candidate.declaration.manifest}
        else:
            state.status = "revision_changed"
            state.proposal = None
            state.proposal_sources = None
            state.diagnostics = [_diagnostic(AnalysisRejected("Workflow source changed during review"))]
    return state


async def revoke_review(
    db: AsyncSession, user: UserPrincipal, workflow_id: UUID,
) -> CloudflareApprovalState:
    workflow, _, can_manage = await _authorized_workflow(db, user, workflow_id, manage=True, lock=True)
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
