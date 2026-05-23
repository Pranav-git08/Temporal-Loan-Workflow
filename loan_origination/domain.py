from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from math import pow
from typing import List, Optional

from .models import (
    ApplicantFinancialProfile,
    ApprovalData,
    ConditionalTerms,
    EligibilityResult,
    KafkaLoanMessage,
    LoanApplication,
    PricingResult,
)


class PricingError(ValueError):
    """Raised when the pricing rules cannot produce a valid offer."""


def evaluate_eligibility(
    application: LoanApplication, profile: ApplicantFinancialProfile
) -> EligibilityResult:
    if profile.creditScore < 600:
        return EligibilityResult(
            isEligible=False,
            rejectionReason="Credit score below minimum threshold of 600.",
        )

    if application.loanAmount >= profile.annualIncome * 5:
        return EligibilityResult(
            isEligible=False,
            rejectionReason="Requested loan amount exceeds 5x annual income.",
        )

    return EligibilityResult(isEligible=True, rejectionReason=None)


def base_interest_rate_for_credit_score(credit_score: int) -> Optional[float]:
    if credit_score >= 750:
        return 5.0
    if credit_score >= 700:
        return 7.0
    if credit_score >= 650:
        return 9.0
    return None


def normalize_tenure(requested_tenure: Optional[int]) -> int:
    allowed = (12, 24, 36)
    if requested_tenure in allowed:
        return int(requested_tenure)
    return 12


def calculate_monthly_emi(principal: float, annual_interest_rate: float, tenure_months: int) -> float:
    monthly_rate = annual_interest_rate / (12 * 100)
    if monthly_rate == 0:
        return round(principal / tenure_months, 2)
    factor = pow(1 + monthly_rate, tenure_months)
    emi = principal * monthly_rate * factor / (factor - 1)
    return round(emi, 2)


def price_loan(
    application: LoanApplication,
    profile: ApplicantFinancialProfile,
    approval: ApprovalData,
) -> PricingResult:
    conditional_terms = approval.conditionalTerms or ConditionalTerms()
    base_rate = base_interest_rate_for_credit_score(profile.creditScore)
    chosen_rate = conditional_terms.interestRate if conditional_terms.interestRate is not None else base_rate

    if chosen_rate is None:
        raise PricingError(
            "Credit score below 650 requires a conditional rate from the reviewing officer."
        )

    tenure = normalize_tenure(conditional_terms.loanTenure)
    emi = calculate_monthly_emi(application.loanAmount, chosen_rate, tenure)
    return PricingResult(interestRate=chosen_rate, loanTenure=tenure, monthlyEMI=emi)


def build_kafka_message(
    application: LoanApplication,
    pricing: PricingResult,
    transaction_id: str,
    disbursement_status: str,
    reminder_interval_days: int,
) -> KafkaLoanMessage:
    return KafkaLoanMessage(
        loanId=application.applicationId,
        applicantName=application.applicantName,
        email=application.email,
        loanAmount=application.loanAmount,
        loanTenure=pricing.loanTenure,
        monthlyEMI=pricing.monthlyEMI,
        disbursementStatus=disbursement_status,
        transactionId=transaction_id,
        reminderIntervalDays=reminder_interval_days,
    )


def generate_email_schedule(
    disbursement_date: datetime, tenure_months: int, reminder_interval_days: int
) -> List[datetime]:
    return [
        disbursement_date + timedelta(days=reminder_interval_days * month)
        for month in range(1, tenure_months + 1)
    ]

