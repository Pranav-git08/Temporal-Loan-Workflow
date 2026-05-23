from __future__ import annotations
import asyncio
import hashlib
from dataclasses import asdict
from datetime import datetime
from uuid import uuid4
from temporalio import activity

from loan_origination.config import DEFAULT_REMINDER_INTERVAL_DAYS
from loan_origination.domain import build_kafka_message, evaluate_eligibility, price_loan
from loan_origination.models import (
    ApplicantFinancialProfile,
    ApprovalData,
    DisbursementResult,
    EmailNotification,
    EligibilityResult,
    KafkaLoanMessage,
    LoanApplication,
    LoanStatus,
    PaidInFullSignal,
    PricingResult,
)
from loan_origination.repository import JsonRepository

repo = JsonRepository()

def _synthetic_profile(applicant_id: str) -> ApplicantFinancialProfile:
    digest = hashlib.sha256(applicant_id.encode("utf-8")).hexdigest()
    credit_score = 550 + (int(digest[:4], 16) % 251)
    annual_income = 30000 + (int(digest[4:10], 16) % 120001)
    return ApplicantFinancialProfile(
        applicantId=applicant_id,
        creditScore=int(credit_score),
        annualIncome=float(annual_income),
    )

@activity.defn
async def persist_loan_application(application: LoanApplication) -> str:
    def _run() -> str:
        repo.upsert_loan_record(application.applicationId, {"application": application, "status": "application_received", "updatedAt": datetime.utcnow().isoformat()})
        return application.applicationId
    return await asyncio.to_thread(_run)

@activity.defn
async def fetch_credit_profile(applicant_id: str) -> ApplicantFinancialProfile:
    return await asyncio.to_thread(_synthetic_profile, applicant_id)

@activity.defn
async def persist_eligibility_result(application_id: str, profile: ApplicantFinancialProfile, eligibility_result: EligibilityResult) -> None:
    def _run() -> None:
        repo.upsert_loan_record(application_id, {"financialProfile": profile, "eligibility": eligibility_result, "status": "eligible" if eligibility_result.isEligible else "rejected", "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def run_eligibility_check(application: LoanApplication, profile: ApplicantFinancialProfile) -> EligibilityResult:
    return await asyncio.to_thread(evaluate_eligibility, application, profile)

@activity.defn
async def persist_manual_review(application_id: str, review: ApprovalData) -> None:
    def _run() -> None:
        repo.append_item("reviews.json", {"loanId": application_id, **asdict(review)})
        repo.upsert_loan_record(application_id, {"manualReview": review, "status": f"manual_{review.approvalStatus}", "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def calculate_loan_pricing(application: LoanApplication, profile: ApplicantFinancialProfile, review: ApprovalData) -> PricingResult:
    return await asyncio.to_thread(price_loan, application, profile, review)

@activity.defn
async def persist_pricing(application_id: str, pricing: PricingResult) -> None:
    def _run() -> None:
        repo.upsert_loan_record(application_id, {"pricing": pricing, "status": "priced", "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def persist_esignature_confirmation(application_id: str, signer_name: str) -> None:
    def _run() -> None:
        repo.upsert_loan_record(application_id, {"esignature": {"verified": True, "signerName": signer_name, "verifiedAt": datetime.utcnow().isoformat()}, "status": "esign_verified", "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def process_loan_disbursement(application: LoanApplication, pricing: PricingResult) -> DisbursementResult:
    transaction_id = f"TXN-{uuid4().hex[:12].upper()}"
    result = DisbursementResult(success=True, transactionId=transaction_id, disbursementStatus="success", processedAt=datetime.utcnow().isoformat())
    def _run() -> None:
        repo.append_item("disbursements.json", {"loanId": application.applicationId, "loanAmount": application.loanAmount, "loanTenure": pricing.loanTenure, "monthlyEMI": pricing.monthlyEMI, **asdict(result)})
        repo.upsert_loan_record(application.applicationId, {"disbursement": result, "status": "disbursed" if result.success else "disbursement_failed", "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)
    return result

@activity.defn
async def publish_disbursement_message(application: LoanApplication, pricing: PricingResult, disbursement: DisbursementResult, reminder_interval_days: int = DEFAULT_REMINDER_INTERVAL_DAYS) -> KafkaLoanMessage:
    message = build_kafka_message(application=application, pricing=pricing, transaction_id=disbursement.transactionId, disbursement_status=disbursement.disbursementStatus, reminder_interval_days=reminder_interval_days)
    def _run() -> None:
        repo.append_item("kafka_outbox.json", asdict(message))
        repo.upsert_loan_record(application.applicationId, {"kafkaMessage": message, "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)
    return message

@activity.defn
async def send_periodic_email(loan_id: str, email_data: EmailNotification) -> None:
    def _run() -> None:
        repo.append_item("emails.json", {"loanId": loan_id, **asdict(email_data)})
        repo.upsert_loan_record(loan_id, {"lastEmailNotification": email_data, "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def mark_loan_paid_in_full(signal_data: PaidInFullSignal) -> None:
    def _run() -> None:
        repo.upsert_loan_record(signal_data.loanId, {"paidInFull": asdict(signal_data), "status": "paid_in_full", "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def cancel_remaining_notifications(loan_id: str) -> None:
    def _run() -> None:
        repo.upsert_loan_record(loan_id, {"emailNotificationsCancelled": True, "updatedAt": datetime.utcnow().isoformat()})
    await asyncio.to_thread(_run)

@activity.defn
async def credit_check_activity(applicant_id: str) -> ApplicantFinancialProfile:
    return await fetch_credit_profile(applicant_id)