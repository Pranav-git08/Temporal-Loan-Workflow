from __future__ import annotations

import argparse
import asyncio

from temporalio.client import Client

from loan_origination.config import LOAN_WORKFLOW_ID_PREFIX, TEMPORAL_ADDRESS
from loan_origination.workflows import LoanOriginationWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send e-signature confirmation signal")
    parser.add_argument("loan_id", help="Loan/application ID")
    parser.add_argument("--signer-name", default="Applicant")
    return parser


async def main() -> None:
    args = build_parser().parse_args()
    client = await Client.connect(TEMPORAL_ADDRESS)
    handle = client.get_workflow_handle(f"{LOAN_WORKFLOW_ID_PREFIX}{args.loan_id}")
    await handle.signal(LoanOriginationWorkflow.confirm_esignature, args.signer_name)
    print(f"E-signature confirmation sent for {args.loan_id}")


if __name__ == "__main__":
    asyncio.run(main())

