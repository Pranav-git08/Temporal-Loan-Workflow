from __future__ import annotations

import argparse
import asyncio
import json

from temporalio.client import Client

from loan_origination.config import EMAIL_WORKFLOW_ID_PREFIX, LOAN_WORKFLOW_ID_PREFIX, TEMPORAL_ADDRESS
from loan_origination.workflows import LoanEmailWorkflow, LoanOriginationWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query loan or email workflow status")
    parser.add_argument("loan_id", help="Loan/application ID")
    parser.add_argument(
        "--workflow-type",
        choices=["loan", "email"],
        default="loan",
    )
    return parser


async def main() -> None:
    args = build_parser().parse_args()
    client = await Client.connect(TEMPORAL_ADDRESS)

    if args.workflow_type == "loan":
        handle = client.get_workflow_handle(f"{LOAN_WORKFLOW_ID_PREFIX}{args.loan_id}")
        status = await handle.query(LoanOriginationWorkflow.get_status)
    else:
        handle = client.get_workflow_handle(f"{EMAIL_WORKFLOW_ID_PREFIX}{args.loan_id}")
        status = await handle.query(LoanEmailWorkflow.get_status)

    print(json.dumps(status.__dict__, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

