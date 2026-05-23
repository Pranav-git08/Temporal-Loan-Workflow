from __future__ import annotations

import unittest
from datetime import datetime

from loan_origination.domain import (
    PricingError,
    calculate_monthly_emi,
    evaluate_eligibility,
    generate_email_schedule,
    price_loan,
)
from loan_origination.models import (
    ApplicantFinancialProfile,
    ApprovalData,
    ConditionalTerms,
    LoanApplication,
)


class LoanDomainTests(unittest.TestCase):
    def test_eligibility_rejects_low_credit_score(self) -> None:
        application = LoanApplication(
            applicationId="loan-1",
            applicantName="Test User",
            email="user@example.com",
            phone="123",
            loanAmount=10000,
            loanPurpose="Car",
        )
        profile = ApplicantFinancialProfile(
            applicantId="loan-1",
            creditScore=590,
            annualIncome=50000,
        )

        result = evaluate_eligibility(application, profile)

        self.assertFalse(result.isEligible)
        self.assertIn("600", result.rejectionReason or "")

    def test_pricing_uses_conditional_rate_for_low_score(self) -> None:
        application = LoanApplication(
            applicationId="loan-2",
            applicantName="Test User",
            email="user@example.com",
            phone="123",
            loanAmount=18000,
            loanPurpose="Medical",
        )
        profile = ApplicantFinancialProfile(
            applicantId="loan-2",
            creditScore=620,
            annualIncome=90000,
        )
        review = ApprovalData(
            reviewerId="officer-1",
            approvalStatus="conditional",
            comments="Approved with risk pricing",
            conditionalTerms=ConditionalTerms(interestRate=12.5, loanTenure=24),
        )

        pricing = price_loan(application, profile, review)

        self.assertEqual(pricing.interestRate, 12.5)
        self.assertEqual(pricing.loanTenure, 24)
        self.assertGreater(pricing.monthlyEMI, 0)

    def test_pricing_rejects_missing_conditional_rate_for_sub_650_score(self) -> None:
        application = LoanApplication(
            applicationId="loan-3",
            applicantName="Test User",
            email="user@example.com",
            phone="123",
            loanAmount=10000,
            loanPurpose="Emergency",
        )
        profile = ApplicantFinancialProfile(
            applicantId="loan-3",
            creditScore=640,
            annualIncome=80000,
        )
        review = ApprovalData(
            reviewerId="officer-2",
            approvalStatus="approved",
            comments="Approved",
            conditionalTerms=None,
        )

        with self.assertRaises(PricingError):
            price_loan(application, profile, review)

    def test_generate_email_schedule_matches_tenure(self) -> None:
        schedule = generate_email_schedule(datetime(2026, 1, 1), 3, 30)
        self.assertEqual(len(schedule), 3)
        self.assertEqual(schedule[0].day, 31)

    def test_emi_calculation_rounds_to_cents(self) -> None:
        emi = calculate_monthly_emi(15000, 7.0, 24)
        self.assertEqual(round(emi, 2), emi)
        self.assertGreater(emi, 0)


if __name__ == "__main__":
    unittest.main()

