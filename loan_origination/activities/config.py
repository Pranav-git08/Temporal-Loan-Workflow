from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "loan_origination" / "data"

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
LOAN_TASK_QUEUE = os.getenv("LOAN_TASK_QUEUE", "loan-origination-task-queue")
EMAIL_WORKFLOW_ID_PREFIX = "loan-email-"
LOAN_WORKFLOW_ID_PREFIX = "loan-origination-"

# Thirty days keeps the business behavior sensible. Override for demos/tests.
DEFAULT_REMINDER_INTERVAL_DAYS = int(os.getenv("REMINDER_INTERVAL_DAYS", "30"))

