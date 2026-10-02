"""A ticket is closed only if it is THIS donor's, open, and this inquiry type.

Three things production said on 2026-10-02 that the old approach could not
survive:

* **177 open DAF-pipeline tickets exist, and the call fetched 10.** The right
  ticket usually was not among them, so "No matching ticket" was often false.
* **Of 100 sampled, only 43 had any content at all.** 57 were titled
  "New ticket created from form submission" and 32 "DAF Form Submission -" with
  no donor name. Matching the donor's email in the text could not work.
* **Endowment tickets were unreachable.** They live in pipeline 1395576547;
  the filter asked for stage "1", which only the DAF Pipeline has.

And one thing no ticket property can tell us: **nothing in the portal identifies
the inquiry type.** Every ticket property definition was read; there is no
category, type or source-form field. "Asset Transfer Notification: …" in the
subject is the only signal, so the text check stays — as a second guard, after
association, pipeline and stage.

No network.
"""

import pytest

from config import Config
from intents import daf_workflow
from intents.daf_workflow import open_inquiry_tickets, ticket_subject_kind
from tests.csuite_doubles import NoDuplicates, contact

EMAIL = "sarah.ahmed@example.invalid"
DAF = Config.TICKET_PIPELINES["daf"]
ENDOW = Config.TICKET_PIPELINES["endowment"]


def ticket(tid, subject="", content="", pipeline=None, stage=None):
    return {"id": tid, "properties": {
        "subject": subject, "content": content,
        "hs_pipeline": DAF["pipeline"] if pipeline is None else pipeline,
        "hs_pipeline_stage": DAF["new_stage"] if stage is None else stage}}


class HubSpot:
    """Association-scoped: get_contact_tickets returns THIS contact's tickets."""

    def __init__(self, tickets=None, contact_id="70123"):
        self.tickets = tickets or []
        self.contact_id = contact_id
        self.closed = []
        self.asked_for = []

    def search_contact_by_email(self, email, properties=None):
        return contact(self.contact_id)

    def update_contact_by_email(self, email, properties):
        return {"id": self.contact_id}

    def get_contact_tickets(self, contact_id, properties=None):
        self.asked_for.append(contact_id)
        return self.tickets

    def get_open_tickets(self, limit=10):
        raise AssertionError(
            "the portal-wide ticket list must not be used to find one donor's "
            "ticket")

    def close_ticket(self, ticket_id):
        self.closed.append(ticket_id)
        return {"id": ticket_id}


class CSuite(NoDuplicates):
    base_url = "https://amuslimcf.fcsuite.com/api/v2"

    def create_individual_profile(self, **kwargs):
        return {"success": True, "data": {"profile_id": 21800}}


def run(monkeypatch, tickets, wf_type="daf", enabled=True, hubspot=None):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TICKET_CLOSE_ENABLED", enabled)
    hs = hubspot if hubspot is not None else HubSpot(tickets)
    form = (Config.DAF_INQUIRY_FORM_ID if wf_type == "daf"
            else Config.ENDOWMENT_INQUIRY_FORM_ID)
    state = {"active": True, "workflow_type": "daf", "type": wf_type,
             "step": "confirm", "form_id": form,
             "submission_data": {"first_name": "Sarah", "last_name": "Ahmed",
                                 "email": EMAIL},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hs, CSuite())
    return reply, hs, state


# ---------------------------------------------------------------------------
# The pipeline and stage ids, VERIFIED from production
# ---------------------------------------------------------------------------

def test_the_verified_pipeline_ids():
    assert DAF == {"pipeline": "0", "label": "DAF Pipeline", "new_stage": "1"}
    assert ENDOW["pipeline"] == "1395576547"
    assert ENDOW["new_stage"] == "2250175191"


def test_stage_1_belongs_to_the_daf_pipeline_alone():
    """Which is why the old `hs_pipeline_stage == "1"` filter was implicitly
    DAF-only — narrower than it looked, and still wrong."""
    stages = {spec["new_stage"] for spec in Config.TICKET_PIPELINES.values()}
    assert "1" in stages
    assert ENDOW["new_stage"] != "1"


# ---------------------------------------------------------------------------
# Association, pipeline, stage
# ---------------------------------------------------------------------------

def test_only_this_contacts_tickets_are_considered(monkeypatch):
    hs = HubSpot([ticket("T-DAF", subject="DAF Form Submission - Sarah")])
    reply, hs, _ = run(monkeypatch, None, hubspot=hs)

    assert hs.asked_for == ["70123"], "scoped to the contact"
    assert hs.closed == ["T-DAF"]


def test_the_portal_wide_list_is_never_used(monkeypatch):
    """HubSpot.get_open_tickets raises in this double. 177 open tickets live
    there and only 10 were ever fetched."""
    reply, hs, _ = run(monkeypatch, [ticket("T", subject="DAF Form Submission")])
    assert hs.closed == ["T"]


def test_a_ticket_in_another_pipeline_is_ignored(monkeypatch):
    reply, hs, _ = run(monkeypatch, [
        ticket("T-OTHER", subject="DAF Form Submission",
               pipeline="2510066372", stage="4179239673")])

    assert hs.closed == []
    assert "No matching ticket" in reply


def test_a_closed_ticket_is_ignored(monkeypatch):
    reply, hs, _ = run(monkeypatch, [
        ticket("T-CLOSED", subject="DAF Form Submission", stage="4")])

    assert hs.closed == []
    assert "No matching ticket" in reply


def test_an_endowment_inquiry_uses_its_own_pipeline(monkeypatch):
    """Endowment tickets were unreachable before: their pipeline is not the
    DAF one and their New stage is not "1"."""
    reply, hs, _ = run(monkeypatch, [
        ticket("T-ENDOW", subject="Endowment inquiry",
               pipeline=ENDOW["pipeline"], stage=ENDOW["new_stage"]),
        ticket("T-DAF", subject="DAF Form Submission")],
        wf_type="endowment")

    assert hs.closed == ["T-ENDOW"], "and not the DAF ticket"


# ---------------------------------------------------------------------------
# The text check, as a second guard
# ---------------------------------------------------------------------------

def test_an_asset_transfer_ticket_in_the_daf_pipeline_is_not_closed(monkeypatch):
    """The brief's case. Both live in pipeline 0 stage 1 — the real subject
    string from production is "Asset Transfer Notification: <name>"."""
    reply, hs, state = run(monkeypatch, [
        ticket("T-ASSET", subject="Asset Transfer Notification: Sarah Ahmed"),
        ticket("T-DAF", subject="DAF Form Submission - Sarah Ahmed")])

    assert hs.closed == ["T-DAF"]
    assert state["ticket_id"] == "T-DAF"


def test_an_asset_transfer_ticket_alone_closes_nothing(monkeypatch):
    reply, hs, _ = run(monkeypatch, [
        ticket("T-ASSET", subject="Asset Transfer Notification: Sarah Ahmed")])

    assert hs.closed == []
    assert "No matching ticket" in reply


def test_a_subject_that_identifies_nothing_is_still_closed(monkeypatch):
    """89 of 100 production tickets are titled "New ticket created from form
    submission" or "DAF Form Submission -". Association, pipeline and stage
    already identify them; the text guard only EXCLUDES other request types, so
    an uninformative subject must not block a close."""
    reply, hs, _ = run(monkeypatch, [
        ticket("T-VAGUE", subject="New ticket created from form submission")])

    assert hs.closed == ["T-VAGUE"]


@pytest.mark.parametrize("text,expected", [
    ("DAF inquiry", "daf"),
    ("DAF Form Submission - Sarah Ahmed", "daf"),
    ("Endowment inquiry", "endowment"),
    ("Asset Transfer Notification: Yasser Shohoud", "other"),
    ("Investment request", "other"),
    ("New ticket created from form submission", None),
    ("", None),
    ("DAF and endowment", None),
])
def test_what_each_real_subject_reads_as(text, expected):
    assert ticket_subject_kind(text) == expected


def test_an_asset_word_beats_an_incidental_daf_mention():
    assert ticket_subject_kind("Asset transfer for the Smith DAF") == "other"


# ---------------------------------------------------------------------------
# Ambiguity, and the cap
# ---------------------------------------------------------------------------

def test_two_candidates_close_nothing_and_both_are_listed(monkeypatch):
    reply, hs, state = run(monkeypatch, [
        ticket("T-A", subject="DAF Form Submission - Sarah"),
        ticket("T-B", subject="DAF Form Submission - Sarah again")])

    assert hs.closed == []
    assert state["ticket_id"] is None
    assert "2 open tickets" in reply
    assert "T-A" in reply and "T-B" in reply


def test_too_many_tickets_is_reported_not_silently_truncated(monkeypatch):
    from intents.daf_workflow import MAX_TICKETS_CONSIDERED

    many = [ticket(f"T{i}", subject="New ticket created from form submission")
            for i in range(MAX_TICKETS_CONSIDERED + 5)]
    reply, hs, _ = run(monkeypatch, many)

    assert hs.closed == []
    assert f"only the first {MAX_TICKETS_CONSIDERED} were considered" in reply


def test_no_contact_means_no_ticket_and_a_stated_reason():
    candidates, note = open_inquiry_tickets(HubSpot([]), None, "daf")
    assert candidates == []
    assert "no HubSpot contact" in note


def test_an_unconfigured_inquiry_type_says_so():
    candidates, note = open_inquiry_tickets(HubSpot([]), "70123", "asset")
    assert candidates == []
    assert "no ticket pipeline is configured" in note


# ---------------------------------------------------------------------------
# The flag
# ---------------------------------------------------------------------------

def test_the_flag_is_off_by_default():
    assert Config.CSUITE_TICKET_CLOSE_ENABLED is False


def test_off_closes_nothing_and_says_so(monkeypatch):
    hs = HubSpot([ticket("T-DAF", subject="DAF Form Submission")])
    hs.get_contact_tickets = lambda *a, **kw: (_ for _ in ()).throw(
        AssertionError("nothing may be looked up when the flag is off"))
    reply, hs, _ = run(monkeypatch, None, enabled=False, hubspot=hs)

    assert hs.closed == []
    assert "🎫 Ticket close: off" in reply
    assert "No matching ticket" not in reply, "off is not 'none found'"


def test_off_does_not_look_like_a_failure(monkeypatch):
    reply, _, _ = run(monkeypatch, [], enabled=False)
    assert "**Issues:**" not in reply
    assert "NOT closed" not in reply


def test_a_failed_close_still_reports_the_ticket_as_open(monkeypatch):
    class Refusing(HubSpot):
        def close_ticket(self, ticket_id):
            return {"error": "insufficient scopes"}

    hs = Refusing([ticket("T-E", subject="DAF Form Submission")])
    reply, hs, _ = run(monkeypatch, None, hubspot=hs)

    assert "NOT closed" in reply
    assert "insufficient scopes" in reply
    assert "T-E" in reply


def test_the_closed_line_names_the_ticket(monkeypatch):
    reply, _, _ = run(monkeypatch, [
        ticket("T-E", subject="DAF Form Submission - Sarah Ahmed")])

    assert "📋 Ticket T-E closed" in reply
    assert "DAF Form Submission - Sarah Ahmed" in reply
