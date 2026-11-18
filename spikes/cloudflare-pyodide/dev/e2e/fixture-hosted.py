import logging

from bifrost import tables, workflow


@workflow(name="phase5b_fixture")
async def phase5b_fixture(case: str):
    rows = await tables.list()
    if case == "cpu":
        # Finite CPU-only work. Small enough to complete in an ordinary process,
        # deliberately larger than this fixture's 1,000 ms CPU budget in Pyodide.
        value = 0
        for index in range(100_000_000):
            value = (value + index) % 1024
        return {"unexpected_completion": value}
    if case == "allowance":
        for _ in range(8):
            await tables.list()
    logging.getLogger("workflow").info("phase5b committed fixture %s", case)
    return {"case": case, "table_count": len(rows)}
