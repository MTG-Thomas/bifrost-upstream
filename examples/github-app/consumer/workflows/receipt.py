"""Read-only example: one complete receipt per installation/provider delivery."""

import re

from bifrost import api, context, tables, workflow


@workflow(name="GitHub App Event Receipt", category="GitHub App")
async def record() -> dict:
    event = context.event
    if not event or not context.organization or not context.solution_id or event.organization_id != context.organization.id:
        raise ValueError("Receipt requires a topic event in this organization")
    data = event.data
    iid, delivery = str(data.get("installation_id", "")), data.get("delivery_id", "")
    if not re.fullmatch(r"[1-9][0-9]{0,19}", iid) or not re.fullmatch(r"[0-9a-fA-F-]{36}", delivery):
        raise ValueError("Invalid GitHub event identity")
    key = f"{iid}:{delivery}"
    # Server-default attribution needs no permission to impersonate an actor.
    response = await api.post("/api/tables/github_app_receipts/documents", params={
        "scope": context.organization.id, "solution": context.solution_id,
        "caller_solution": context.solution_id,
    }, json={"id": key, "data": data})
    if response.status_code == 409:
        existing = await tables.get("github_app_receipts", key)
        if not existing or existing.data != data:
            raise ValueError("Delivery identity conflicts with the retained receipt") from None
        return {"receipt": key, "duplicate": True}
    response.raise_for_status()
    return {"receipt": key, "duplicate": False}
