"""Generate and accept Cloudflare workspace workflow proposals as a server operator."""

from __future__ import annotations

import argparse
import asyncio
import json
from uuid import UUID

from src.core.database import get_db_context
from src.core.storage.contracts import StorageError
from src.services.execution.cloudflare_eligibility import (
    AnalysisRejected,
    accept_proposal,
    propose_approval,
)


async def run(args: argparse.Namespace) -> None:
    proposal = await propose_approval(
        args.workflow_id, subrequest_allowance=args.subrequest_allowance, cpu_budget_ms=args.cpu_budget_ms,
    )
    if args.command == "accept":
        if proposal.revision != args.review_sha256:
            raise AnalysisRejected("The generated proposal has changed. Review the new proposal before accepting.")
        if not args.operator.strip() or len(args.operator) > 255:
            raise AnalysisRejected("An operator label of 1–255 characters is required")
        async with get_db_context() as db:
            await accept_proposal(db, proposal, accepted_by=args.operator.strip())

    print(json.dumps({**proposal.review_document(), "review_sha256": proposal.revision}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("propose", "accept"):
        command = commands.add_parser(name)
        command.add_argument("workflow_id", type=UUID)
        command.add_argument("--subrequest-allowance", type=int, required=True)
        command.add_argument("--cpu-budget-ms", type=int, required=True)
        if name == "accept":
            command.add_argument("--review-sha256", required=True)
            command.add_argument("--operator", required=True)
    try:
        asyncio.run(run(parser.parse_args()))
    except (AnalysisRejected, StorageError, TimeoutError, ValueError) as error:
        parser.exit(1, f"Cloudflare approval refused: {error}\n")


if __name__ == "__main__":
    main()
