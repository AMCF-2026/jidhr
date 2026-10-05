"""A processed submission is never offered again, and "skip" goes forward.

The 2026-10-05 live run created profile 21696 for a donor, and
"process daf inquiry" immediately offered the same submission back. Four
things were wrong at once:

* **Nothing recorded a success.** Only the failure path wrote an identifiable
  row, so there was no way to ask "is this one done?". `_initiate_workflow`
  took `submissions[0]` unconditionally and would have offered it forever,
  until a newer submission displaced it.
* **"Skip" cancelled.** It reset the workflow, so skip and cancel did the same
  thing and the next submission in the page was unreachable.
* **The email went out as typed.** `primary_email` matching is exact and
  case-sensitive, so one capital letter meant the next inquiry from that donor
  searched for the lowercase form, found nothing, and created a second profile.
* **"Submitted:" printed 1790704953446.** Nothing formatted it anywhere.

The due-date clamp from the same run is tested in test_followup_task.py,
beside the rest of the due-date arithmetic.

No network, no database.
"""

import pytest

import clients.audit as audit
from clients.csuite import CSuiteClient
from config import Config
from intents import daf_workflow
from intents.daf_workflow import (_initiate_workflow, _is_processed,
                                  _next_unprocessed, format_submitted_at)
from sync.sandbox_writes import WriteBudget
from tests.csuite_doubles import NoDuplicates, contact

GENC_MS = 1790704953446          # 2026-09-29 14:02 ET, the real submission
FORM = Config.DAF_INQUIRY_FORM_ID


def submission(sub_id, first="Genc", email=None, submitted=GENC_MS):
    return {"conversionId": sub_id, "submittedAt": submitted,
            "values": [{"name": "firstname", "value": first},
                       {"name": "lastname", "value": ""},
                       {"name": "email",
                        "value": email or f"{first.lower()}@example.invalid"}]}


class HubSpot:
    def __init__(self, submissions):
        self._submissions = submissions

    def get_daf_inquiry_submissions(self, limit=50):
        return {"results": self._submissions[:limit]}

    def search_contact_by_email(self, email, properties=None):
        return contact("70123")

    def update_contact_by_email(self, email, properties):
        return {"id": "70123"}


def state_dict():
    return dict(daf_workflow.default_workflow_state())


def offer(monkeypatch, submissions, processed=None, raises=None):
    """Run _initiate_workflow with the processed history stubbed."""
    def fake(form_id, endpoint=audit.PROFILE_CREATE_ENDPOINT):
        if raises is not None:
            raise raises
        return dict(processed or {})

    monkeypatch.setattr(audit, "processed_submissions", fake)
    state = state_dict()
    reply = _initiate_workflow("process daf inquiry", state,
                               HubSpot(submissions))
    return reply, state


# ---------------------------------------------------------------------------
# FIX 1 — a successful create records what it came from
# ---------------------------------------------------------------------------

class AuditedClient(CSuiteClient):
    """The real _request, with the transport and the audit store faked."""

    def __init__(self):
        self.api_key = "k"
        self.api_secret = "s"
        self.base_url = "https://amuslimcf-sandbox.fcsuite.com/api/v2"
        self.env = "live"
        self.verify_writes = False
        self.write_budget = WriteBudget(5)
        self.sent_body = None

        outer = self

        class Session:
            def post(_self, url, data=None, **kwargs):
                import json as _json
                outer.sent_body = _json.loads(data)

                class R:
                    status_code = 200
                    text = '{"success":1,"data":{"profile_id":21696}}'

                    def json(self):
                        return {"success": 1,
                                "data": {"profile_id": 21696}}
                return R()

        self.session = Session()


@pytest.fixture
def captured(monkeypatch):
    """The payload reserve_write was given, as payload_meta renders it."""
    seen = {}

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        seen["payload"] = payload
        seen["meta"] = audit.payload_meta(payload)
        return {"id": 1}

    import clients.csuite as mod
    monkeypatch.setattr(mod, "reserve_write", reserve)
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)
    return seen


def test_a_successful_create_audits_the_form_and_submission_ids(captured):
    client = AuditedClient()

    client.create_individual_profile(
        "Genc", "", email="genc@example.invalid",
        audit_meta={"hubspot_form_id": FORM,
                    "hubspot_submission_id": "conv-genc"})

    ids = captured["meta"]["ids"]
    assert ids["hubspot_form_id"] == FORM
    assert ids["hubspot_submission_id"] == "conv-genc"


def test_the_audit_only_fields_are_never_sent_to_csuite(captured):
    """They are not CSuite input names. A 200 would have discarded them in
    silence, and the allowlist would have refused them first."""
    client = AuditedClient()

    client.create_individual_profile(
        "Genc", "", email="genc@example.invalid",
        audit_meta={"hubspot_form_id": FORM,
                    "hubspot_submission_id": "conv-genc"})

    assert "hubspot_form_id" not in client.sent_body
    assert "hubspot_submission_id" not in client.sent_body
    assert client.sent_body["first_name"] == "Genc"


def test_a_create_with_no_audit_meta_is_unchanged(captured):
    client = AuditedClient()
    client.create_individual_profile("Genc", "", email="genc@example.invalid")

    assert captured["meta"]["ids"] == {}
    assert "hubspot_form_id" not in client.sent_body


def test_the_workflow_passes_both_ids_on_the_live_path(monkeypatch):
    seen = {}

    class CSuite(NoDuplicates):
        base_url = "https://amuslimcf.fcsuite.com/api/v2"

        def create_individual_profile(self, **kwargs):
            seen.update(kwargs.get("audit_meta") or {})
            return {"success": True, "data": {"profile_id": 21696}}

    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TICKET_CLOSE_ENABLED", False)

    state = state_dict()
    state.update({"active": True, "workflow_type": "daf", "type": "daf",
                  "step": "confirm", "form_id": FORM,
                  "submission_data": {"first_name": "Genc", "last_name": "",
                                      "email": "genc@example.invalid",
                                      "submission_id": "conv-genc"}})
    daf_workflow._step_create("yes", state, HubSpot([]), CSuite())

    assert seen == {"hubspot_form_id": FORM,
                    "hubspot_submission_id": "conv-genc"}


# ---------------------------------------------------------------------------
# FIX 1 — reading the history back
# ---------------------------------------------------------------------------

def test_processed_submissions_maps_submission_id_to_profile_id(monkeypatch):
    monkeypatch.setattr(audit, "_csuite_env_column_exists", lambda: True)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)
    monkeypatch.setattr(
        "clients.database.execute_query",
        lambda sql, params=None, fetch=True: [
            {"submission_id": "conv-genc", "target_id": "21696"},
            {"submission_id": "conv-older", "target_id": "21690"}])

    assert audit.processed_submissions(FORM) == {"conv-genc": "21696",
                                                 "conv-older": "21690"}


def test_the_query_asks_only_for_successful_live_creates(monkeypatch):
    """A sandbox create of the same submission must not hide a donor who
    still needs a real profile."""
    seen = {}
    monkeypatch.setattr(audit, "_csuite_env_column_exists", lambda: True)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)

    def capture(sql, params=None, fetch=True):
        seen["sql"] = " ".join(sql.split())
        seen["params"] = params
        return []

    monkeypatch.setattr("clients.database.execute_query", capture)
    audit.processed_submissions(FORM)

    assert "status = 'success'" in seen["sql"]
    assert "csuite_env = 'live'" in seen["sql"]
    assert seen["params"] == (audit.PROFILE_CREATE_ENDPOINT, FORM)


def test_a_missing_env_column_is_unavailable_not_empty(monkeypatch):
    """Without it a production create cannot be told from a sandbox one, and
    an empty answer would read as "nothing has ever been processed"."""
    monkeypatch.setattr(audit, "_csuite_env_column_exists", lambda: False)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)

    with pytest.raises(audit.ProcessedHistoryUnavailable) as caught:
        audit.processed_submissions(FORM)
    assert "002_write_audit_csuite_env" in str(caught.value)


def test_no_database_is_unavailable_not_empty(monkeypatch):
    monkeypatch.setattr("clients.database.is_configured", lambda: False)

    with pytest.raises(audit.ProcessedHistoryUnavailable):
        audit.processed_submissions(FORM)


def test_a_query_failure_is_unavailable_not_empty(monkeypatch):
    monkeypatch.setattr(audit, "_csuite_env_column_exists", lambda: True)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)

    def boom(sql, params=None, fetch=True):
        raise RuntimeError("connection reset")

    monkeypatch.setattr("clients.database.execute_query", boom)
    with pytest.raises(audit.ProcessedHistoryUnavailable):
        audit.processed_submissions(FORM)


# ---------------------------------------------------------------------------
# FIX 1 — the workflow acts on it
# ---------------------------------------------------------------------------

def test_an_already_processed_latest_submission_is_never_offered(monkeypatch):
    """The bug, end to end. Before this the reply was the review block for a
    donor who already had profile 21696."""
    reply, state = offer(monkeypatch, [submission("conv-genc")],
                         processed={"conv-genc": "21696"})

    assert reply == ("📭 Latest DAF submission (Genc) already processed: "
                     "profile 21696.")
    assert state["active"] is False, "nothing to confirm, so nothing is armed"
    assert "Shall I create" not in reply


def test_a_rerun_of_a_processed_submission_attempts_no_create(monkeypatch):
    """The reply is not the only thing that matters — no CSuite call may be
    made, and the workflow must not be left armed for a "yes"."""
    class Explodes(NoDuplicates):
        def create_individual_profile(self, **kwargs):
            raise AssertionError("no create may be attempted")

    reply, state = offer(monkeypatch, [submission("conv-genc")],
                         processed={"conv-genc": "21696"})

    assert state["step"] is None
    assert state["submission_data"] == {}
    # A "yes" now starts a fresh workflow rather than confirming anything.
    assert daf_workflow._handle_active_workflow(
        "yes", state, HubSpot([]), Explodes()).startswith("⚠️")


def test_all_five_processed_says_so_and_counts_the_rest(monkeypatch):
    subs = [submission(f"conv-{i}", first=f"Donor{i}") for i in range(5)]
    subs[0] = submission("conv-genc")
    reply, state = offer(
        monkeypatch, subs,
        processed={"conv-genc": "21696",
                   **{f"conv-{i}": str(21700 + i) for i in range(1, 5)}})

    assert reply.startswith("📭 Latest DAF submission (Genc) already "
                            "processed: profile 21696.")
    assert "other 4 fetched submissions" in reply
    assert state["active"] is False


def test_a_processed_newest_falls_through_to_the_next_one(monkeypatch):
    reply, state = offer(
        monkeypatch,
        [submission("conv-genc"), submission("conv-next", first="Next")],
        processed={"conv-genc": "21696"})

    assert "Next" in reply
    assert state["active"] is True
    assert state["submission_index"] == 1
    assert state["submission_data"]["first_name"] == "Next"
    assert "has already been processed" in reply, \
        "say why this is not the newest"
    assert "21696" in reply


def test_an_unreadable_history_offers_the_submission_with_a_warning(
        monkeypatch):
    """Fails toward offering: the duplicate guard still refuses a second
    profile, whereas hiding a submission loses a donor silently."""
    reply, state = offer(
        monkeypatch, [submission("conv-genc")],
        raises=audit.ProcessedHistoryUnavailable("no csuite_env column"))

    assert state["active"] is True
    assert "could not be checked" in reply
    assert "no csuite_env column" in reply
    assert "Shall I create" in reply


def test_a_submission_with_no_identifier_counts_as_unprocessed():
    assert _is_processed({"submission_id": ""}, {"": "21696"}) is False
    assert _is_processed({}, {}) is False
    assert _is_processed({"submission_id": "conv-genc"},
                         {"conv-genc": "21696"}) is True


def test_next_unprocessed_walks_forward():
    subs = [{"submission_id": "a"}, {"submission_id": "b"},
            {"submission_id": "c"}]
    done = {"a": "1", "b": "2"}

    assert _next_unprocessed(subs, done) == 2
    assert _next_unprocessed(subs, {}) == 0
    assert _next_unprocessed(subs, {}, start=2) == 2
    assert _next_unprocessed(subs, done, start=3) is None
    assert _next_unprocessed([], {}) is None


# ---------------------------------------------------------------------------
# FIX 2 — skip goes forward
# ---------------------------------------------------------------------------

def test_skip_offers_the_next_submission_instead_of_cancelling(monkeypatch):
    subs = [submission("conv-1", first="First"),
            submission("conv-2", first="Second")]
    _, state = offer(monkeypatch, subs)
    assert state["submission_index"] == 0

    reply = daf_workflow._handle_active_workflow("skip", state, HubSpot(subs),
                                                 NoDuplicates())

    assert "Second" in reply
    assert "Submission 2 of 2" in reply
    assert state["active"] is True, "skip is not cancel"
    assert state["submission_index"] == 1
    assert state["submission_data"]["first_name"] == "Second"


def test_skip_jumps_over_an_already_processed_submission(monkeypatch):
    subs = [submission("conv-1", first="First"),
            submission("conv-2", first="Second"),
            submission("conv-3", first="Third")]
    _, state = offer(monkeypatch, subs, processed={"conv-2": "21699"})

    reply = daf_workflow._handle_active_workflow("skip", state, HubSpot(subs),
                                                 NoDuplicates())

    assert "Third" in reply
    assert "Second" not in reply
    assert state["submission_index"] == 2


def test_skipping_the_last_one_ends_the_workflow_and_says_so(monkeypatch):
    subs = [submission("conv-1", first="Only")]
    _, state = offer(monkeypatch, subs)

    reply = daf_workflow._handle_active_workflow("skip", state, HubSpot(subs),
                                                 NoDuplicates())

    assert "No further unprocessed DAF submissions" in reply
    assert "last of the 1 fetched" in reply
    assert state["active"] is False


def test_cancel_still_cancels(monkeypatch):
    """Skip changed; cancel did not."""
    subs = [submission("conv-1"), submission("conv-2")]
    _, state = offer(monkeypatch, subs)

    reply = daf_workflow._handle_active_workflow("cancel", state,
                                                 HubSpot(subs),
                                                 NoDuplicates())

    assert reply == "👍 Workflow cancelled."
    assert state["active"] is False


# ---------------------------------------------------------------------------
# FIX 3 — the email is normalised on both sides of the comparison
# ---------------------------------------------------------------------------

class CaseSensitiveCSuite(CSuiteClient):
    """CSuite as measured: profile/list matches primary_email EXACTLY.

    Verified 2026-09-30 — the same address uppercased returns 0.
    """

    base_url = "https://amuslimcf.fcsuite.com/api/v2"

    def __init__(self):
        self.verify_writes = False
        self.write_budget = WriteBudget(5)
        self.stored = {}            # profile_id -> primary_email as stored
        self.created = []

    def _request(self, endpoint, data=None, audit_meta=None):
        data = data or {}
        if endpoint == "profile/create/individual":
            profile_id = 21696 + len(self.created)
            self.created.append(dict(data))
            self.stored[profile_id] = data.get("email")
            return {"success": True, "data": {"profile_id": profile_id}}
        if endpoint == "profile/display":
            key = int(data.get("profile_id") or 0)
            if key not in self.stored:
                return {"success": False, "errors": ["Profile not found"]}
            return {"success": True,
                    "data": {"profile_id": key,
                             "primary_email": self.stored[key]}}
        if endpoint == "profile/list":
            if "primary_email" not in data:
                return self._page(18797, [])
            wanted = str(data["primary_email"])
            rows = [{"profile_id": pid} for pid, stored in self.stored.items()
                    if stored == wanted]          # exact, case-sensitive
            return self._page(len(rows), rows)
        raise AssertionError(endpoint)

    @staticmethod
    def _page(count, rows):
        return {"success": True, "http_status": 200,
                "data": {"count": count, "results": rows}}


def live_state(email, sub_id="conv-genc"):
    state = state_dict()
    state.update({"active": True, "workflow_type": "daf", "type": "daf",
                  "step": "confirm", "form_id": FORM,
                  "submission_data": {"first_name": "Genc", "last_name": "A",
                                      "email": email,
                                      "submission_id": sub_id}})
    return state


def arm(monkeypatch):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TICKET_CLOSE_ENABLED", False)


def test_a_mixed_case_form_email_is_stored_lowercase(monkeypatch):
    arm(monkeypatch)
    csuite = CaseSensitiveCSuite()

    daf_workflow._step_create("yes", live_state("Genc.A@Example.Invalid"),
                              HubSpot([]), csuite)

    assert csuite.created[0]["email"] == "genc.a@example.invalid"


def test_the_guard_finds_that_profile_on_a_second_inquiry(monkeypatch):
    """The whole point. Sent as typed and searched lowercase, run two found
    nothing and made a second profile for the same donor."""
    arm(monkeypatch)
    csuite = CaseSensitiveCSuite()

    daf_workflow._step_create("yes", live_state("Genc.A@Example.Invalid"),
                              HubSpot([]), csuite)
    assert len(csuite.created) == 1

    # The donor enquires again, typing it differently this time.
    reply = daf_workflow._step_create("yes", live_state("GENC.A@example.INVALID"),
                                      HubSpot([]), csuite)

    assert len(csuite.created) == 1, "a second profile was created"
    assert "♻️ **Already in CSuite — no new profile created**" in reply
    assert "Profile 21696" in reply


def test_the_same_address_as_typed_is_still_found(monkeypatch):
    """Lowercasing must not break the ordinary case."""
    arm(monkeypatch)
    csuite = CaseSensitiveCSuite()

    daf_workflow._step_create("yes", live_state("plain@example.invalid"),
                              HubSpot([]), csuite)
    reply = daf_workflow._step_create("yes",
                                      live_state("plain@example.invalid"),
                                      HubSpot([]), csuite)

    assert len(csuite.created) == 1
    assert "♻️ **Already in CSuite — no new profile created**" in reply


# ---------------------------------------------------------------------------
# FIX 5 — the submitted time is readable
# ---------------------------------------------------------------------------

def test_the_real_timestamp_formats_as_a_date():
    assert format_submitted_at(GENC_MS) == "Tue Sep 29, 2026 2:02 PM ET"
    assert format_submitted_at(str(GENC_MS)) == "Tue Sep 29, 2026 2:02 PM ET"


def test_an_iso_string_formats_the_same_way():
    assert format_submitted_at("2026-09-29T18:02:33Z") == \
        "Tue Sep 29, 2026 2:02 PM ET"
    assert format_submitted_at("2026-09-29T18:02:33+00:00") == \
        "Tue Sep 29, 2026 2:02 PM ET"


def test_the_hour_is_not_zero_padded_and_noon_is_12():
    # 2026-09-29 16:05 UTC = 12:05 PM ET
    assert format_submitted_at(1790697900000) == "Tue Sep 29, 2026 12:05 PM ET"
    # 2026-09-29 13:05 UTC = 9:05 AM ET
    assert format_submitted_at(1790687100000) == "Tue Sep 29, 2026 9:05 AM ET"


@pytest.mark.parametrize("value,expected", [
    (None, "Unknown"),
    ("", "Unknown"),
    ("Unknown", "Unknown"),
    ({}, "Unknown"),
    ("not a date", "not a date"),
])
def test_an_unreadable_value_is_never_a_guess(value, expected):
    assert format_submitted_at(value) == expected


def test_the_review_block_shows_the_date_not_the_raw_number(monkeypatch):
    reply, _ = offer(monkeypatch, [submission("conv-genc")])

    assert "📅 **Submitted:** Tue Sep 29, 2026 2:02 PM ET" in reply
    assert str(GENC_MS) not in reply


def test_the_review_block_offers_skip_as_its_own_choice(monkeypatch):
    reply, _ = offer(monkeypatch, [submission("conv-1"), submission("conv-2")])

    assert 'Say *"Skip"* to move to the next submission' in reply
    assert 'Say *"Cancel"* to stop' in reply
