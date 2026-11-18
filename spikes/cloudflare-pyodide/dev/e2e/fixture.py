import logging

from bifrost import tables, workflow


@workflow(name="phase5a_fixture")
async def phase5a_fixture(case: str):
    # This SDK read exercises the actual run credential through TenantApi.
    rows = await tables.list()
    from js import fetch

    response = await fetch(f"https://phase5-fixture.invalid/hold/{case}")
    await response.text()
    logging.getLogger("workflow").info("phase5a committed fixture %s", case)
    return {"case": case, "table_count": len(rows)}
