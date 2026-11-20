"""Administration of inbound workload trust rules."""

from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from sqlalchemy.exc import IntegrityError

from src.core.auth import CurrentActiveUser
from src.core.db_deps import DbSession
from src.models.contracts.workload_trust import (
    WorkloadTrustRuleList,
    WorkloadTrustRulePublic,
    WorkloadTrustRuleUpdate,
    WorkloadTrustRuleWrite,
)
from src.models.orm.workload_trust import WorkloadTrustRule
from src.services.audit import emit_audit
from src.services.authorization.enforce import load_caller, require_permission
from src.services.workload_auth.errors import RuleValidationError
from src.services.workload_auth.rules import (
    create_rule,
    get_rule,
    list_rules,
    to_public,
    update_rule,
)

router = APIRouter(prefix="/api/settings/workload-trust", tags=["Workload Trust"])


async def _authorize(db: DbSession, user: CurrentActiveUser, *, write: bool) -> None:
    caller = await load_caller(db, user)
    require_permission(caller, "settings.readwrite" if write else "settings.read", None)


def _audit_details(row: WorkloadTrustRule) -> dict[str, object]:
    return {
        "name": row.name,
        "provider": row.provider,
        "identity_id": str(row.identity_id),
        "enabled": row.enabled,
    }


@router.get(
    "", response_model=WorkloadTrustRuleList, summary="List Workload Trust Rules"
)
async def list_workload_trust_rules(
    user: CurrentActiveUser, db: DbSession
) -> WorkloadTrustRuleList:
    await _authorize(db, user, write=False)
    return WorkloadTrustRuleList(rules=await list_rules(db))


@router.post(
    "",
    response_model=WorkloadTrustRulePublic,
    status_code=status.HTTP_201_CREATED,
    summary="Create Workload Trust Rule",
)
async def create_workload_trust_rule(
    body: WorkloadTrustRuleWrite,
    user: CurrentActiveUser,
    db: DbSession,
) -> WorkloadTrustRulePublic:
    await _authorize(db, user, write=True)
    try:
        row = await create_rule(db, body, created_by=user.user_id)
    except RuleValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A trust rule with this name already exists.",
        ) from None
    await emit_audit(
        db,
        "settings.workload_trust.create",
        resource_type="workload_trust_rule",
        resource_id=row.id,
        details=_audit_details(row),
    )
    await db.commit()
    return to_public(row)


@router.get(
    "/{rule_id}",
    response_model=WorkloadTrustRulePublic,
    summary="Get Workload Trust Rule",
)
async def get_workload_trust_rule(
    rule_id: UUID,
    user: CurrentActiveUser,
    db: DbSession,
) -> WorkloadTrustRulePublic:
    await _authorize(db, user, write=False)
    row = await get_rule(db, rule_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trust rule not found."
        )
    return to_public(row)


@router.patch(
    "/{rule_id}",
    response_model=WorkloadTrustRulePublic,
    summary="Update Workload Trust Rule",
)
async def patch_workload_trust_rule(
    rule_id: UUID,
    body: WorkloadTrustRuleUpdate,
    user: CurrentActiveUser,
    db: DbSession,
) -> WorkloadTrustRulePublic:
    await _authorize(db, user, write=True)
    row = await get_rule(db, rule_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trust rule not found."
        )
    try:
        updated = await update_rule(db, row, body)
    except RuleValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A trust rule with this name already exists.",
        ) from None
    await emit_audit(
        db,
        "settings.workload_trust.update",
        resource_type="workload_trust_rule",
        resource_id=updated.id,
        details=_audit_details(updated),
    )
    await db.commit()
    return to_public(updated)


@router.delete(
    "/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete Workload Trust Rule",
)
async def delete_workload_trust_rule(
    rule_id: UUID, user: CurrentActiveUser, db: DbSession
) -> None:
    await _authorize(db, user, write=True)
    row = await get_rule(db, rule_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trust rule not found."
        )
    details = _audit_details(row)
    await db.delete(row)
    await emit_audit(
        db,
        "settings.workload_trust.delete",
        resource_type="workload_trust_rule",
        resource_id=rule_id,
        details=details,
    )
    await db.commit()
