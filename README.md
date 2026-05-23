![Coverage Badge](https://img.shields.io/badge/coverage-90%25-brightgreen)
# Loan Origination BPM Workflow in Temporal

This project implements the loan origination process you described as a Temporal-based Python application.

## Implemented workflow pieces

- Loan application intake persisted to a local JSON-backed repository
- Automated eligibility check using a simulated external financial-profile activity
- Manual approval via Temporal signal
- Loan pricing with rate bands, conditional overrides, and EMI calculation
- E-signature confirmation via Temporal signal
- Loan disbursement and Kafka-style outbox publishing
- Periodic email reminder workflow
- Paid-in-full signal that cancels remaining notifications

## Project layout

- `loan_origination/models.py`: request/response payloads
- `loan_origination/domain.py`: pure business rules
- `loan_origination/activities.py`: persistence/integration activities
- `loan_origination/workflows.py`: Temporal workflows and signals
- `worker.py`: Temporal worker entrypoint
- `start_loan_workflow.py`: start the loan workflow
- `signal_manual_approval.py`: officer approval signal
- `signal_esignature.py`: applicant e-signature signal
- `signal_paid_in_full.py`: stop reminder emails
- `query_workflow_status.py`: query workflow state

## Data schemas

### Loan application

```json
{
  "applicationId": "string",
  "applicantName": "string",
  "email": "string",
  "phone": "string",
  "loanAmount": "number",
  "loanPurpose": "string"
}
```

### Manual approval

```json
{
  "reviewerId": "string",
  "approvalStatus": "approved",
  "comments": "Looks good",
  "conditionalTerms": {
    "interestRate": 8.5,
    "loanTenure": 24
  }
}
```

### Paid in full signal

```json
{
  "loanId": "string",
  "paymentDate": "2026-05-10T12:00:00",
  "paymentStatus": "paidInFull"
}
```

## Run locally

1. Install dependencies:

```powershell
env\Scripts\python.exe -m pip install -r requirements.txt
```

2. Start a Temporal dev server in another terminal:

```powershell
.\temporal.exe server start-dev
```

3. Start the worker:

```powershell
env\Scripts\python.exe worker.py
```

4. Start a loan workflow:

```powershell
env\Scripts\python.exe start_loan_workflow.py
```

5. Send manual approval:

```powershell
env\Scripts\python.exe signal_manual_approval.py loan-1001 --approval-status conditional --comments "Approved with adjusted pricing" --interest-rate 8.5 --loan-tenure 24
```

6. Confirm e-signature:

```powershell
env\Scripts\python.exe signal_esignature.py loan-1001 --signer-name "Ava Patel"
```

7. Query status:

```powershell
env\Scripts\python.exe query_workflow_status.py loan-1001
env\Scripts\python.exe query_workflow_status.py loan-1001 --workflow-type email
```

8. Mark the loan paid in full:

```powershell
env\Scripts\python.exe signal_paid_in_full.py loan-1001
```

## Notes

- The â€œexternal API for credit score and annual income is simulated in `fetch_credit_profile`.
- Kafka is modeled as a persisted outbox in `loan_origination/data/kafka_outbox.json`.
- By default the email workflow is auto-started by the loan workflow after publishing the disbursement message. That keeps the end-to-end flow runnable without a separate Kafka consumer process.
- Reminder cadence defaults to 30 days. Override it when starting the workflow if you want faster demos.

![Coverage Badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/YOUR_GITHUB_USERNAME/YOUR_GIST_ID/raw/coverage.json)
