"""A role name held by more than one role is never resolved to one of them.

Role names are not unique. Resolving by name used to keep whichever row the
database returned last, so a Solution or git-sync manifest referencing
"Operators" could grant an entity to either of two same-named roles.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from bifrost.manifest import ManifestRole
from src.models.orm.forms import FormRole
from src.models.orm.solutions import Solution
from src.models.orm.users import Role
from src.services.manifest_import import ManifestResolver, _resolve_role_names
from src.services.solutions.deploy import SolutionBundle, SolutionDeployConflict, SolutionDeployer


@pytest.fixture(autouse=True)
def _guard():
    from src.services.solutions.guard import install_solution_write_guard

    install_solution_write_guard()
    yield


async def _same_named_roles(db, count: int = 2) -> tuple[str, list[Role]]:
    name = f"Operators {uuid.uuid4().hex[:6]}"
    roles = [Role(name=name, created_by="test") for _ in range(count)]
    db.add_all(roles)
    await db.flush()
    return name, roles


@pytest.mark.parametrize("create_missing", [False, True])
async def test_resolver_refuses_a_name_held_by_several_roles(db_session, create_missing):
    name, _ = await _same_named_roles(db_session)

    with pytest.raises(ValueError, match="ambiguous role name"):
        await _resolve_role_names(db_session, [name], create_missing=create_missing)


async def test_solution_deploy_does_not_bind_an_ambiguous_role(db_session):
    db = db_session
    name, roles = await _same_named_roles(db)
    sol = Solution(id=uuid.uuid4(), slug=f"amb-{uuid.uuid4().hex[:8]}", name="Ambiguous", organization_id=None)
    db.add(sol)
    await db.flush()

    with pytest.raises(SolutionDeployConflict, match="ambiguous role name"):
        await SolutionDeployer(db).deploy(SolutionBundle(
            solution=sol,
            forms=[{
                "id": str(uuid.uuid4()), "name": "gated", "access_level": "role_based",
                "role_names": [name], "form_schema": {"fields": []},
            }],
        ))

    bound = await db.execute(select(FormRole.role_id).where(FormRole.role_id.in_([r.id for r in roles])))
    assert bound.scalars().all() == []


async def test_git_sync_name_match_refuses_an_ambiguous_role(db_session):
    name, _ = await _same_named_roles(db_session)
    resolver = ManifestResolver(db_session)
    cache = await resolver._prefetch_existing_entities()

    with pytest.raises(ValueError, match="ambiguous role name"):
        resolver._resolve_role(ManifestRole(id=str(uuid.uuid4()), name=name), cache)


async def test_git_sync_id_match_still_resolves_a_same_named_role(db_session):
    name, roles = await _same_named_roles(db_session)
    resolver = ManifestResolver(db_session)
    cache = await resolver._prefetch_existing_entities()

    ops = resolver._resolve_role(ManifestRole(id=str(roles[0].id), name=name), cache)

    assert [(op.id, op.match_on) for op in ops] == [(roles[0].id, "id")]
