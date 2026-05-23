import pytest
from loan_origination.activities.activities import credit_check_activity

@pytest.mark.asyncio
async def test_credit_check_declined():
    bad_credit_data = {'applicant_name': 'John Doe', 'score': 450}
    result = await credit_check_activity(bad_credit_data)
    assert result['status'] == 'DECLINED'

@pytest.mark.asyncio
async def test_credit_check_approved():
    good_credit_data = {'applicant_name': 'Jane Doe', 'score': 750}
    result = await credit_check_activity(good_credit_data)
    assert result['status'] == 'APPROVED'
