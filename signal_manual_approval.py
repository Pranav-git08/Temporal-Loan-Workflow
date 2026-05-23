from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from temporalio.client import Client

from loan_origination.config import LOAN_WORKFLOW_ID_PREFIX, TEMPORAL_ADDRESS
from loan_origination.models import ApprovalData, ConditionalTerms
from loan_origination.workflows import LoanOriginationWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send manual approval signal")
    parser.add_argument("loan_id", help="Loan/application ID")
    parser.add_argument("--input-file", type=Path, help="JSON file with approval payload")
    parser.add_argument("--reviewer-id", default="officer-001")
    parser.add_argument(
        "--approval-status",
        choices=["approved", "rejected", "conditional"],
        default="approved",
    )
    parser.add_argument("--comments", default="Reviewed and approved.")
    parser.add_argument("--interest-rate", type=float)
    parser.add_argument("--loan-tenure", type=int)
    return parser


def load_review(args: argparse.Namespace) -> ApprovalData:
    if args.input_file:
        payload = json.loads(args.input_file.read_text(encoding="utf-8"))
        conditional = payload.get("conditionalTerms")
        return ApprovalData(
            reviewerId=payload["reviewerId"],
            approvalStatus=payload["approvalStatus"],
            comments=payload["comments"],
            conditionalTerms=ConditionalTerms(**conditional) if conditional else None,
        )

    conditional = None
    if args.interest_rate is not None or args.loan_tenure is not None:
        conditional = ConditionalTerms(
            interestRate=args.interest_rate,
            loanTenure=args.loan_tenure,
        )
    return ApprovalData(
        reviewerId=args.reviewer_id,
        approvalStatus=args.approval_status,
        comments=args.comments,
        conditionalTerms=conditional,
    )


async def main() -> None:
    args = build_parser().parse_args()
    review = load_review(args)
    client = await Client.connect(TEMPORAL_ADDRESS)
    handle = client.get_workflow_handle(f"{LOAN_WORKFLOW_ID_PREFIX}{args.loan_id}")
    await handle.signal(LoanOriginationWorkflow.submit_manual_approval, review)
    print(f"Manual approval signal sent for {args.loan_id}")


if __name__ == "__main__":
    asyncio.run(main())

