"""hotfix-70: the DAF form's fund name and contribution are read.

The live DAF form calls them what_would_you_like_to_name_your_fund_ and
estimated_initial_contribution_amount (on all 50 newest submissions,
2026-10-21). Only legacy names were mapped, so every review and every
monthly inquiry summary showed them as missing. The contribution is a
dropdown band that carries its own currency ("$10,001-$25,000",
"Under $2,500"), so nothing is prefixed to it. Invented values; no network.
"""

from datetime import datetime

from intents import daf_workflow, reports


def live_submission(ms):
    values = {"firstname": "Test", "lastname": "Donor",
              "email": "t@example.invalid",
              "what_would_you_like_to_name_your_fund_": "Test Family Fund",
              "estimated_initial_contribution_amount": "$10,001-$25,000"}
    return {"conversionId": "c1", "submittedAt": ms,
            "values": [{"name": k, "value": v, "objectTypeId": "0-1"}
                       for k, v in values.items()]}


def test_the_chat_parser_reads_the_live_names():
    parsed = daf_workflow._parse_submission(live_submission(1))
    assert parsed["fund_name"] == "Test Family Fund"
    assert parsed["initial_contribution"] == "$10,001-$25,000"


def test_the_legacy_names_still_work():
    sub = {"conversionId": "c2", "submittedAt": 1, "values": [
        {"name": "fund_name", "value": "Old Fund"},
        {"name": "initial_contribution", "value": "5000"}]}
    parsed = daf_workflow._parse_submission(sub)
    assert (parsed["fund_name"], parsed["initial_contribution"]) == \
        ("Old Fund", "5000")


def test_the_monthly_summary_reads_them_and_adds_no_second_dollar():
    now_ms = int(datetime.now().timestamp() * 1000) - 60_000

    class Hub:
        def get_daf_inquiry_submissions(self, limit=50):
            return {"results": [live_submission(now_ms)]}

        def get_endowment_inquiry_submissions(self, limit=50):
            return {"results": []}

    reply = reports._report_daf_inquiry_summary("daf inquiries", Hub())

    assert "Test Family Fund" in reply
    assert "$10,001-$25,000" in reply
    assert "$$" not in reply
    assert "Fund name not provided" not in reply
