from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path

from temporalio.client import Client

from loan_origination.config import LOAN_TASK_QUEUE, LOAN_WORKFLOW_ID_PREFIX, TEMPORAL_ADDRESS
from loan_origination.models import LoanApplication
from loan_origination.workflows import LoanOriginationWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start a loan origination workflow")
    parser.add_argument("--input-file", type=Path, help="Path to a loan application JSON file")
    parser.add_argument("--workflow-id", help="Explicit workflow ID override")
    parser.add_argument(
        "--reminder-interval-days",
        type=int,
        default=30,
        help="Email reminder interval in days",
    )
    return parser


def load_application(input_file: Path | None) -> LoanApplication:
    if input_file:
        payload = json.loads(input_file.read_text(encoding="utf-8"))
    else:
        payload = {
            "applicationId": "loan-1001",
            "applicantName": "Ava Patel",
            "email": "ava.patel@example.com",
            "phone": "+1-555-0101",
            "loanAmount": 15000,
            "loanPurpose": "Home improvement",
        }
    return LoanApplication(**payload)


async def main() -> None:
    args = build_parser().parse_args()
    application = load_application(args.input_file)
    workflow_id = args.workflow_id or f"{LOAN_WORKFLOW_ID_PREFIX}{application.applicationId}"
    client = await Client.connect(TEMPORAL_ADDRESS)

    handle = await client.start_workflow(
        LoanOriginationWorkflow.run,
        args=[application, args.reminder_interval_days, True],
        id=workflow_id,
        task_queue=LOAN_TASK_QUEUE,
    )
    print(json.dumps({"workflowId": handle.id}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
