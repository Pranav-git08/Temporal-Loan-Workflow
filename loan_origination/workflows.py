from __future__ import annotations

from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
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
    from .config import DEFAULT_REMINDER_INTERVAL_DAYS, EMAIL_WORKFLOW_ID_PREFIX
    from .domain import generate_email_schedule
    from .models import (
        ApprovalData,
        EmailNotification,
        KafkaLoanMessage,
        LoanApplication,
        LoanWorkflowResult,
        PaidInFullSignal,
        WorkflowStatus,
    )


ACTIVITY_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class LoanOriginationWorkflow:
    def __init__(self) -> None:
        self.loan_id: str = ""
        self.status = "created"
        self.manual_review: ApprovalData | None = None
        self.esign_signer: str | None = None
        self.result: LoanWorkflowResult | None = None

    @workflow.run
    async def run(
        self,
        application: LoanApplication,
        reminder_interval_days: int = DEFAULT_REMINDER_INTERVAL_DAYS,
        auto_start_email_workflow: bool = True,
    ) -> LoanWorkflowResult:
        self.loan_id = application.applicationId
        self.status = "application_received"

        await workflow.execute_activity(
            persist_loan_application,
            application,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        profile = await workflow.execute_activity(
            fetch_credit_profile,
            application.applicationId,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        eligibility = await workflow.execute_activity(
            run_eligibility_check,
            args=[application, profile],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        await workflow.execute_activity(
            persist_eligibility_result,
            args=[application.applicationId, profile, eligibility],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        if not eligibility.isEligible:
            self.status = "rejected"
            return LoanWorkflowResult(
                loanId=application.applicationId,
                status="rejected",
                eligibility=eligibility,
                comments=eligibility.rejectionReason,
            )

        self.status = "awaiting_manual_review"
        await workflow.wait_condition(lambda: self.manual_review is not None)
        review = self.manual_review
        assert review is not None

        await workflow.execute_activity(
            persist_manual_review,
            args=[application.applicationId, review],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        if review.approvalStatus == "rejected":
            self.status = "rejected"
            return LoanWorkflowResult(
                loanId=application.applicationId,
                status="rejected",
                eligibility=eligibility,
                comments=review.comments,
            )

        try:
            pricing = await workflow.execute_activity(
                calculate_loan_pricing,
                args=[application, profile, review],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
        except ActivityError as exc:
            self.status = "rejected"
            return LoanWorkflowResult(
                loanId=application.applicationId,
                status="rejected",
                eligibility=eligibility,
                comments=str(exc.cause) if exc.cause else str(exc),
            )
        await workflow.execute_activity(
            persist_pricing,
            args=[application.applicationId, pricing],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        self.status = "awaiting_esignature"
        await workflow.wait_condition(lambda: self.esign_signer is not None)
        assert self.esign_signer is not None
        await workflow.execute_activity(
            persist_esignature_confirmation,
            args=[application.applicationId, self.esign_signer],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        disbursement = await workflow.execute_activity(
            process_loan_disbursement,
            args=[application, pricing],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        kafka_message = await workflow.execute_activity(
            publish_disbursement_message,
            args=[application, pricing, disbursement, reminder_interval_days],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )

        if auto_start_email_workflow and disbursement.success:
            await workflow.start_child_workflow(
                LoanEmailWorkflow.run,
                args=[kafka_message],
                id=f"{EMAIL_WORKFLOW_ID_PREFIX}{application.applicationId}",
                parent_close_policy=ParentClosePolicy.ABANDON,
            )

        self.status = "completed"
        self.result = LoanWorkflowResult(
            loanId=application.applicationId,
            status="completed",
            eligibility=eligibility,
            pricing=pricing,
            disbursement=disbursement,
            kafkaMessage=kafka_message,
            comments="Loan disbursed successfully.",
        )
        return self.result

    @workflow.signal
    def submit_manual_approval(self, review: ApprovalData) -> None:
        self.manual_review = review
        self.status = f"manual_{review.approvalStatus}"

    @workflow.signal
    def confirm_esignature(self, signer_name: str) -> None:
        self.esign_signer = signer_name
        self.status = "esign_verified"

    @workflow.query
    def get_status(self) -> WorkflowStatus:
        return WorkflowStatus(
            workflowType="loan_origination",
            loanId=self.loan_id,
            status=self.status,
        )


@workflow.defn
class LoanEmailWorkflow:
    def __init__(self) -> None:
        self.status = "created"
        self.signal_data: PaidInFullSignal | None = None
        self.loan_id: str = ""

    @workflow.run
    async def run(self, kafka_message: KafkaLoanMessage) -> str:
        self.loan_id = kafka_message.loanId
        self.status = "active"

        schedule = generate_email_schedule(
            disbursement_date=workflow.now(),
            tenure_months=kafka_message.loanTenure,
            reminder_interval_days=kafka_message.reminderIntervalDays,
        )

        for due_at in schedule:
            if self.signal_data and self.signal_data.paymentStatus == "paidInFull":
                break

            delay = due_at - workflow.now()
            if delay > timedelta(0):
                await workflow.sleep(delay)

            if self.signal_data and self.signal_data.paymentStatus == "paidInFull":
                break

            email_data = EmailNotification(
                emailRecipient=kafka_message.email,
                subject=f"Loan EMI Reminder for {kafka_message.loanId}",
                body=(
                    f"Hello {kafka_message.applicantName}, your EMI of "
                    f"{kafka_message.monthlyEMI:.2f} is due."
                ),
                sendDate=due_at.isoformat(),
            )
            await workflow.execute_activity(
                send_periodic_email,
                args=[kafka_message.loanId, email_data],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )

        if self.signal_data and self.signal_data.paymentStatus == "paidInFull":
            self.status = "paid_in_full"
            await workflow.execute_activity(
                mark_loan_paid_in_full,
                self.signal_data,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            await workflow.execute_activity(
                cancel_remaining_notifications,
                args=[kafka_message.loanId],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            return "Email notifications cancelled after paid-in-full signal."

        self.status = "completed"
        return "Email reminder schedule completed."

    @workflow.signal
    def mark_paid_in_full(self, signal_data: PaidInFullSignal) -> None:
        self.signal_data = signal_data
        self.status = signal_data.paymentStatus

    @workflow.query
    def get_status(self) -> WorkflowStatus:
        return WorkflowStatus(
            workflowType="loan_email",
            loanId=self.loan_id,
            status=self.status,
        )
