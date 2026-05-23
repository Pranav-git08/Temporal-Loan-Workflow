from __future__ import annotations

import argparse
import asyncio
from datetime import datetime

from temporalio.client import Client

from loan_origination.config import EMAIL_WORKFLOW_ID_PREFIX, TEMPORAL_ADDRESS
from loan_origination.models import PaidInFullSignal
from loan_origination.workflows import LoanEmailWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send paid-in-full signal to email workflow")
    parser.add_argument("loan_id", help="Loan/application ID")
    parser.add_argument(
        "--payment-status",
        choices=["paidInFull", "partiallyPaid"],
        default="paidInFull",
    )
    parser.add_argument("--payment-date", default=datetime.utcnow().isoformat())
    return parser


async def main() -> None:
    args = build_parser().parse_args()
    client = await Client.connect(TEMPORAL_ADDRESS)
    handle = client.get_workflow_handle(f"{EMAIL_WORKFLOW_ID_PREFIX}{args.loan_id}")
    await handle.signal(
        LoanEmailWorkflow.mark_paid_in_full,
        PaidInFullSignal(
            loanId=args.loan_id,
            paymentDate=args.payment_date,
            paymentStatus=args.payment_status,
        ),
    )
    print(f"Paid-in-full signal sent for {args.loan_id}")


if __name__ == "__main__":
    asyncio.run(main())

