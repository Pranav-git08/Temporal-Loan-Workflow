from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


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
    applicantName: str
    email: str
    loanAmount: float
    loanTenure: int
    monthlyEMI: float
    disbursementStatus: str
    transactionId: str
    reminderIntervalDays: int = 30


@dataclass
class EmailNotification:
    emailRecipient: str
    subject: str
    body: str
    sendDate: str


@dataclass
class PaidInFullSignal:
    loanId: str
    paymentDate: str
    paymentStatus: str


@dataclass
class LoanWorkflowResult:
    loanId: str
    status: str
    eligibility: Optional[EligibilityResult] = None
    pricing: Optional[PricingResult] = None
    disbursement: Optional[DisbursementResult] = None
    kafkaMessage: Optional[KafkaLoanMessage] = None
    comments: Optional[str] = None


@dataclass
class WorkflowStatus:
    workflowType: str
    loanId: str
    status: str
    updatedAt: str = field(default_factory=lambda: datetime.utcnow().isoformat())

