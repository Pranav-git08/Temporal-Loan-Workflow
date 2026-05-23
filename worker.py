from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from loan_origination.activities import (
    calculate_loan_pricing,
    cancel_remaining_notifications,
    fetch_credit_profile,
    mark_loan_paid_in_full,
    persist_eligibility_result,
    persist_esignature_confirmation,
    persist_loan_application,
    persist_manual_review,
    persist_pricing,
    process_loan_disbursement,
    publish_disbursement_message,
    run_eligibility_check,
    send_periodic_email,
)
from loan_origination.config import LOAN_TASK_QUEUE, TEMPORAL_ADDRESS
from loan_origination.workflows import LoanEmailWorkflow, LoanOriginationWorkflow


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = Worker(
        client,
        task_queue=LOAN_TASK_QUEUE,
        workflows=[LoanOriginationWorkflow, LoanEmailWorkflow],
        activities=[
            persist_loan_application,
            fetch_credit_profile,
            run_eligibility_check,
            persist_eligibility_result,
            persist_manual_review,
            calculate_loan_pricing,
            persist_pricing,
            persist_esignature_confirmation,
            process_loan_disbursement,
            publish_disbursement_message,
            send_periodic_email,
            mark_loan_paid_in_full,
            cancel_remaining_notifications,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

