from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum

@dataclass
class LoanApplication:
    applicationId: str
    applicantName: str
    email: str
    phone: str
    loanAmount: float
    loanPurpose: str

@dataclass
class ApplicantFinancialProfile:
    applicantId: str
    creditScore: int
    annualIncome: float

@dataclass
class EligibilityResult:
    isEligible: bool
    rejectionReason: Optional[str]

@dataclass
class ConditionalTerms:
    interestRate: Optional[float] = None
    loanTenure: Optional[int] = None

@dataclass
class ApprovalData:
    reviewerId: str
    approvalStatus: str
    comments: str
    conditionalTerms: Optional[ConditionalTerms] = None

@dataclass
class PricingResult:
    interestRate: float
    loanTenure: int
    monthlyEMI: float

@dataclass
class DisbursementResult:
    success: bool
    transactionId: str
    disbursementStatus: str
    processedAt: str

@dataclass
class KafkaLoanMessage:
    loanId: str
    status: str
    reminderIntervalDays: int

@dataclass
class EmailNotification:
    recipient: str
    subject: str
    body: str

@dataclass
class PaidInFullSignal:
    loanId: str
    paidAt: str

class LoanStatus(Enum):
    RECEIVED = "application_received"
    ELIGIBLE = "eligible"
    REJECTED = "rejected"
    PRICED = "priced"
    ESIGN_VERIFIED = "esign_verified"
    DISBURSED = "disbursed"
    PAID_IN_FULL = "paid_in_full"