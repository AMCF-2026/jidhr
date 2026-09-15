"""Step 4a: the open-tickets report and export.

No network. HubSpot is a stub at the _post/_get seam so the search body,
the paging, and the owner lookup are all exercised as written.

Every fixture is invented.
"""

import csv
import io
from datetime import datetime, timedelta, timezone

import pytest

from clients.hubspot import HubSpotClient, is_hubspot_write
from config import Config
from intents import tickets
from scripts import tickets_export

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

DAF = "0"
ACH = "1637348066"
ENDOW = "1395576547"

DAF_NEW, DAF_CONTACTED, DAF_WAITING, DAF_CLOSED = "1", "2216593113", "3", "4"
ACH_SUBMITTED, ACH_WAITING, ACH_ENTERED = ("2611947255", "2611947256",
                                            "2611948218")
ENDOW_NEW = "2250175191"

OWNERS = {"160587283": "Nora Moorefield (n*@amuslimcf.org)",
          "160587284": "Kods Zouita (k*@amuslimcf.org)"}


def stamp(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def ticket(ticket_id, pipeline, stage, subject, created_days_ago,
           activity_days_ago=None, owner=None, source="FORM", daf_name=None):
    props = {
        "subject": subject, "hs_pipeline": pipeline,
        "hs_pipeline_stage": stage, "createdate": stamp(created_days_ago),
        "hs_lastmodifieddate": stamp(created_days_ago),
        "hs_lastactivitydate": stamp(activity_days_ago)
        if activity_days_ago is not None else None,
        "hubspot_owner_id": owner, "source_type": source,
        "daf_name": daf_name,
    }
    return {"id": str(ticket_id), "properties": props}


TICKETS = [
    ticket(1001, DAF, DAF_NEW, "DAF inquiry", 90, None, "160587283"),
    ticket(1002, DAF, DAF_CONTACTED, "Follow up on DAF paperwork", 40, 3,
           "160587283"),
    ticket(1003, DAF, DAF_NEW, "Question about grants", 10, None, None),
    ticket(1004, ACH, ACH_SUBMITTED, "ACH form submitted", 200, None,
           "160587284"),
    ticket(1005, ACH, ACH_WAITING, "ACH needs bank letter", 15, 15,
           "160587284"),
    ticket(1006, ENDOW, ENDOW_NEW, "Endowment for our masjid", 5, None,
           "999999999"),
]


# ---------------------------------------------------------------------------
# A HubSpot stub at the HTTP-method seam
# ---------------------------------------------------------------------------

class StubHubSpot:
    def __init__(self, pages=None, owners_payload=None, fail_page=None):
        self.client = HubSpotClient.__new__(HubSpotClient)
        self.client.access_token = "test-token"
        self.client.base_url = "https://api.example.invalid"
        self.client.headers = {}
        self.client._social_channels_cache = None
        HubSpotClient._owners_cache = None

        self.pages = pages if pages is not None else [TICKETS]
        self.owners_payload = owners_payload if owners_payload is not None \
            else {"results": [
                {"id": "160587283", "firstName": "Nora",
                 "lastName": "Moorefield", "email": "n*@amuslimcf.org"},
                {"id": "160587284", "firstName": "Kods",
                 "lastName": "Zouita", "email": "k*@amuslimcf.org"},
            ]}
        self.fail_page = fail_page
        self.posts = []
        self.gets = []

        self.client._post = self._post
        self.client._get = self._get

    def _post(self, endpoint, data=None):
        self.posts.append((endpoint, data))
        index = len(self.posts) - 1
        if self.fail_page is not None and index == self.fail_page:
            return {"error": "HubSpot returned 500"}
        page = self.pages[index] if index < len(self.pages) else []
        response = {"results": list(page), "total": sum(len(p) for p in
                                                        self.pages)}
        if index + 1 < len(self.pages):
            response["paging"] = {"next": {"after": f"cursor-{index + 1}"}}
        return response

    def _get(self, endpoint, params=None):
        self.gets.append((endpoint, params))
        if endpoint == "crm/v3/owners":
            return self.owners_payload
        return {"results": []}


class Actor:
    def __init__(self, email, user_id=1, role="staff"):
        self.email = email
        self.user_id = user_id
        self.role = role


class Ctx:
    def __init__(self, hubspot, email="carl@amuslimcf.org"):
        class Services:
            pass
        self.services = Services()
        self.services.hubspot = hubspot
        self.workflow_state = {}
        self.draft_state = {}
        self.actor = Actor(email)

    def is_allowed(self, roles):
        return self.actor.role in roles


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch):
    monkeypatch.setattr(tickets, "_now", lambda: NOW)


@pytest.fixture(autouse=True)
def clear_owner_cache():
    HubSpotClient._owners_cache = None
    yield
    HubSpotClient._owners_cache = None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def test_open_stages_match_probe_h14():
    assert Config.TICKET_OPEN_STAGES[DAF] == {"1", "2216593113",
                                              "2216593114", "3"}
    assert Config.TICKET_OPEN_STAGES[ACH] == {"2611947255", "2611947256"}
    assert DAF_CLOSED not in Config.TICKET_OPEN_STAGES[DAF]
    assert ACH_ENTERED not in Config.TICKET_OPEN_STAGES[ACH]


def test_every_pipeline_has_a_label_and_ach_has_the_note():
    for pipeline_id in Config.TICKET_OPEN_STAGES:
        assert pipeline_id in Config.TICKET_PIPELINE_LABELS
    assert "Entered Into CSuite" in Config.TICKET_PIPELINE_NOTES[ACH]
    assert DAF not in Config.TICKET_PIPELINE_NOTES


def test_stage_ids_are_unique_across_pipelines():
    seen = []
    for stages in Config.TICKET_OPEN_STAGES.values():
        seen.extend(stages)
    assert len(seen) == len(set(seen))


# ---------------------------------------------------------------------------
# fetch_open_tickets
# ---------------------------------------------------------------------------

def test_search_filters_on_the_configured_open_stages():
    hub = StubHubSpot()

    found, complete = hub.client.fetch_open_tickets()

    assert complete is True
    assert len(found) == 6
    endpoint, body = hub.posts[0]
    assert endpoint == "crm/v3/objects/tickets/search"
    filters = body["filterGroups"][0]["filters"]
    assert filters == [{"propertyName": "hs_pipeline_stage", "operator": "IN",
                        "values": sorted({s for ss in
                                          Config.TICKET_OPEN_STAGES.values()
                                          for s in ss})}]
    assert "hs_lastactivitydate" in body["properties"]
    assert "daf_name" in body["properties"]
    assert body["limit"] == 100


def test_search_is_a_read_and_not_audited():
    assert is_hubspot_write("POST", "crm/v3/objects/tickets/search") is False


def test_search_follows_paging():
    hub = StubHubSpot(pages=[TICKETS[:3], TICKETS[3:5], TICKETS[5:]])

    found, complete = hub.client.fetch_open_tickets()

    assert complete is True
    assert [t["id"] for t in found] == [t["id"] for t in TICKETS]
    assert len(hub.posts) == 3
    assert hub.posts[1][1]["after"] == "cursor-1"
    assert "after" not in hub.posts[0][1]


def test_a_failed_page_returns_what_it_had_and_complete_false():
    hub = StubHubSpot(pages=[TICKETS[:3], TICKETS[3:]], fail_page=1)

    found, complete = hub.client.fetch_open_tickets()

    assert complete is False
    assert len(found) == 3


def test_a_failed_first_page_is_empty_and_incomplete():
    hub = StubHubSpot(fail_page=0)
    assert hub.client.fetch_open_tickets() == ([], False)


def test_no_open_stages_means_no_call():
    hub = StubHubSpot()
    assert hub.client.fetch_open_tickets(open_stages=[]) == ([], True)
    assert hub.posts == []


def test_owners_are_labelled_and_cached_per_process():
    hub = StubHubSpot()

    first = hub.client.get_owners()
    second = hub.client.get_owners()

    assert first == OWNERS
    assert second is first
    assert len(hub.gets) == 1, "the second call must not hit HubSpot"


def test_owner_lookup_failure_is_empty_not_fatal():
    hub = StubHubSpot(owners_payload={"error": "HubSpot returned 500"})
    assert hub.client.get_owners() == {}
    assert HubSpotClient._owners_cache is None, "a failure is not cached"


# ---------------------------------------------------------------------------
# Per-ticket math
# ---------------------------------------------------------------------------

def test_age_and_idle_math():
    row = tickets.describe(TICKETS[1], OWNERS, now=NOW)

    assert row["age_days"] == 40
    assert row["touched"] is True
    assert row["idle_days"] == 3
    assert row["owner"] == "Nora Moorefield (n*@amuslimcf.org)"
    assert row["pipeline_label"] == "DAF Pipeline"
    assert row["stage_label"] == "Contacted/Emailed"


def test_never_touched_has_no_idle_not_a_zero():
    row = tickets.describe(TICKETS[0], OWNERS, now=NOW)

    assert row["touched"] is False
    assert row["idle_days"] is None
    assert row["last_activity"] is None
    assert row["age_days"] == 90


def test_unknown_owner_id_is_shown_as_an_id():
    row = tickets.describe(TICKETS[5], OWNERS, now=NOW)
    assert row["owner"] == "owner 999999999"


def test_unassigned_ticket_has_no_owner():
    row = tickets.describe(TICKETS[2], OWNERS, now=NOW)
    assert row["owner"] is None
    assert "unassigned" in tickets.ticket_line(row)


def test_describe_rejects_a_row_with_no_id():
    assert tickets.describe({"properties": {}}, OWNERS) is None
    assert tickets.describe("junk", OWNERS) is None


def test_a_future_or_missing_createdate_never_goes_negative():
    row = tickets.describe(ticket(1, DAF, DAF_NEW, "x", -3), OWNERS, now=NOW)
    assert row["age_days"] == 0
    row = tickets.describe({"id": "2", "properties": {}}, OWNERS, now=NOW)
    assert row["age_days"] is None


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

def report(hub=None, query="open tickets"):
    hub = hub or StubHubSpot()
    return tickets.handle(query, Ctx(hub.client))


def test_header_counts_and_never_touched_percent():
    out = report()
    # 6 tickets, 4 never touched (1001, 1003, 1004, 1006) -> 67%
    assert out.startswith("🎫 **6 open tickets across 3 pipelines — "
                          "4 never touched (67%)**")


def test_pipelines_are_ordered_most_open_first():
    out = report()
    daf = out.index("**DAF Pipeline**")
    ach = out.index("**ACH Setup**")
    endow = out.index("**Endowment Inquiry**")
    assert daf < ach < endow


def test_per_pipeline_line_has_counts_and_oldest_age():
    out = report()
    assert "**DAF Pipeline** — 3 open, 2 never touched, oldest 90d" in out
    assert "**ACH Setup** — 2 open, 1 never touched, oldest 200d" in out


def test_stages_are_listed_with_counts():
    out = report()
    assert "Stages: New 2, Contacted/Emailed 1" in out
    assert "Stages: Information Submitted 1, Waiting on Kods 1" in out


def test_ticket_lines_are_oldest_first_with_idle_or_never_touched():
    out = report()
    daf_block = out[out.index("**DAF Pipeline**"):out.index("**ACH Setup**")]
    lines = [l for l in daf_block.splitlines() if l.startswith("• #")]
    link = lambda i: f" · [open]({Config.HUBSPOT_TICKET_URL.format(ticket_id=i)})"
    assert lines == [
        "• #1001 · DAF inquiry · 90d old · never touched · "
        "Nora Moorefield (n*@amuslimcf.org)" + link("1001"),
        "• #1002 · Follow up on DAF paperwork · 40d old · 3d idle · "
        "Nora Moorefield (n*@amuslimcf.org)" + link("1002"),
        "• #1003 · Question about grants · 10d old · never touched · "
        "unassigned" + link("1003"),
    ]


def test_ach_setup_carries_the_entered_into_csuite_note():
    out = report()
    ach_block = out[out.index("**ACH Setup**"):
                    out.index("**Endowment Inquiry**")]
    assert "ℹ️" in ach_block
    assert "Entered Into CSuite" in ach_block
    daf_block = out[out.index("**DAF Pipeline**"):out.index("**ACH Setup**")]
    assert "ℹ️" not in daf_block


def test_only_ten_oldest_per_pipeline_then_and_n_more():
    many = [ticket(2000 + i, DAF, DAF_NEW, f"Ticket {i}", 100 - i)
            for i in range(14)]
    out = report(StubHubSpot(pages=[many]))

    assert out.count("• #") == 10
    assert "• ... and 4 more" in out
    assert "#2000 ·" in out and "#2013 ·" not in out


def test_partial_list_gets_the_banner_at_the_top():
    hub = StubHubSpot(pages=[TICKETS[:3], TICKETS[3:]], fail_page=1)
    out = report(hub)

    assert out.startswith("⚠️ HubSpot returned a partial list — counts below "
                          "are incomplete.")
    assert "3 open tickets" in out


def test_complete_list_has_no_banner():
    assert "partial" not in report().lower()


def test_no_open_tickets():
    out = report(StubHubSpot(pages=[[]]))
    assert out == "✅ No open tickets."


def test_report_is_deterministic():
    assert report() == report()


# ---------------------------------------------------------------------------
# Query variants
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("query, days", [
    ("tickets older than 30 days", 30),
    ("Tickets older than 2 weeks", 14),
    ("tickets older than 3 months", 90),
    ("tickets older than 45", 45),
    ("open tickets", None),
])
def test_older_than_parsing(query, days):
    assert tickets.parse_min_age_days(query) == days


def test_older_than_filters_by_age():
    out = report(query="tickets older than 30 days")

    assert "3 open tickets" in out
    assert "(older than 30 days)" in out
    assert "#1001" in out and "#1002" in out and "#1004" in out
    assert "#1003" not in out and "#1006" not in out


def test_tickets_for_owner_filters_by_owner_label():
    out = report(query="tickets for Kods")

    assert "2 open tickets across 1 pipeline" in out
    assert "#1004" in out and "#1005" in out
    assert "#1001" not in out


def test_tickets_for_unknown_owner_says_so():
    out = report(query="tickets for Nobody")
    assert "No open tickets (owned by 'Nobody')" in out
    assert "try a first name" in out


def test_older_than_is_not_mistaken_for_an_owner():
    assert tickets.parse_owner_filter("tickets older than 30 days") is None
    assert tickets.parse_owner_filter("tickets for Nora?") == "Nora"


@pytest.mark.parametrize("query", [
    "open tickets", "show me stale tickets", "ticket report please",
    "tickets older than 30 days", "tickets for Nora",
])
def test_triggers(query):
    assert tickets.can_handle(query)


def test_non_ticket_queries_are_not_claimed():
    assert not tickets.can_handle("fund balance for END0026")
    assert not tickets.can_handle("log my call with Ahmed")


def test_handler_is_registered_before_reports():
    from intents import HANDLER_CHAIN
    names = [name for name, _ in HANDLER_CHAIN]
    assert "tickets" in names
    assert names.index("tickets") < names.index("reports")


def test_report_makes_no_claude_call():
    hub = StubHubSpot()
    ctx = Ctx(hub.client)

    class Claude:
        def chat(self, **kwargs):
            raise AssertionError("the ticket report must not ask Claude")

    ctx.services.claude = Claude()
    assert "open tickets" in tickets.handle("open tickets", ctx)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def test_export_columns_and_values():
    rows = tickets_export.export_rows(TICKETS, OWNERS, now=NOW)

    assert list(rows[0].keys()) == list(tickets_export.COLUMNS)
    by_id = {r["id"]: r for r in rows}

    touched = by_id["1002"]
    assert touched["pipeline"] == "DAF Pipeline"
    assert touched["stage"] == "Contacted/Emailed"
    assert touched["age_days"] == 40
    assert touched["idle_days"] == 3
    assert touched["never_touched"] == "no"
    assert touched["owner"] == "Nora Moorefield (n*@amuslimcf.org)"
    assert touched["url"] == Config.HUBSPOT_TICKET_URL.format(ticket_id="1002")
    assert Config.HUBSPOT_PORTAL_ID in touched["url"]

    never = by_id["1001"]
    assert never["idle_days"] == ""
    assert never["last_activity"] == ""
    assert never["never_touched"] == "yes"


def test_export_sorts_by_pipeline_then_oldest_first():
    rows = tickets_export.export_rows(TICKETS, OWNERS, now=NOW)
    assert [(r["pipeline"], r["id"]) for r in rows] == [
        ("ACH Setup", "1004"), ("ACH Setup", "1005"),
        ("DAF Pipeline", "1001"), ("DAF Pipeline", "1002"),
        ("DAF Pipeline", "1003"),
        ("Endowment Inquiry", "1006"),
    ]


def test_export_writes_a_readable_csv(tmp_path):
    rows = tickets_export.export_rows(TICKETS, OWNERS, now=NOW)
    path = tmp_path / "t.csv"

    tickets_export.write_csv(str(path), rows, complete=True)

    with open(path, newline="", encoding="utf-8") as handle:
        read = list(csv.DictReader(handle))
    assert len(read) == 6
    assert read[0]["id"] == "1004"
    assert list(read[0].keys()) == list(tickets_export.COLUMNS)


def test_partial_export_is_marked_in_the_file(tmp_path):
    rows = tickets_export.export_rows(TICKETS[:2], OWNERS, now=NOW)
    path = tmp_path / "t.csv"

    tickets_export.write_csv(str(path), rows, complete=False)

    first_line = path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line.startswith("# PARTIAL")


def test_export_header_line():
    rows = tickets_export.export_rows(TICKETS, OWNERS, now=NOW)
    assert tickets_export.header_line(rows, True) == \
        "6 open tickets across 3 pipelines — 4 never touched (67%)"
    assert tickets_export.header_line(rows, False).startswith("⚠️ PARTIAL")


def test_export_main_end_to_end(tmp_path, monkeypatch):
    hub = StubHubSpot()
    monkeypatch.setattr(tickets_export, "HubSpotClient", lambda: hub.client)
    out = tmp_path / "open.csv"
    buffer = io.StringIO()
    monkeypatch.setattr("sys.stdout", buffer)

    code = tickets_export.main(["--out", str(out)])

    assert code == 0
    assert out.exists()
    assert "6 open tickets across 3 pipelines" in buffer.getvalue()


# ===========================================================================
# 4a-fix
# ===========================================================================

# ---------------------------------------------------------------------------
# 2. Activity that predates the ticket
# ---------------------------------------------------------------------------

PREDATED = ticket(3001, DAF, DAF_CONTACTED, "Old thread, new ticket",
                  created_days_ago=40, activity_days_ago=400,
                  owner="160587283")


def test_idle_is_capped_at_age_when_activity_predates_the_ticket():
    row = tickets.describe(PREDATED, OWNERS, now=NOW)

    assert row["age_days"] == 40
    assert row["touched"] is True
    assert row["idle_days"] == 40, "capped at the ticket's own age"
    assert row["activity_predates_ticket"] is True


def test_normal_activity_is_not_flagged_as_predating():
    row = tickets.describe(TICKETS[1], OWNERS, now=NOW)
    assert row["activity_predates_ticket"] is False
    assert row["idle_days"] == 3

    # Equal is not "before": activity on the day the ticket was created.
    same_day = ticket(3002, DAF, DAF_NEW, "x", 10, 10)
    row = tickets.describe(same_day, OWNERS, now=NOW)
    assert row["activity_predates_ticket"] is False
    assert row["idle_days"] == 10


def test_never_touched_is_never_flagged_as_predating():
    row = tickets.describe(TICKETS[0], OWNERS, now=NOW)
    assert row["activity_predates_ticket"] is False
    assert row["idle_days"] is None


def test_report_line_says_activity_predates_ticket():
    out = report(StubHubSpot(pages=[[PREDATED]]))
    assert ("#3001 · Old thread, new ticket · 40d old · "
            "≥ 40d idle (activity predates ticket) · "
            "Nora Moorefield (n*@amuslimcf.org) · [open](") in out
    assert "400d" not in out


def test_export_caps_idle_and_sets_the_flag_column():
    rows = tickets_export.export_rows([PREDATED, TICKETS[1]], OWNERS, now=NOW)
    by_id = {r["id"]: r for r in rows}

    assert by_id["3001"]["idle_days"] == 40
    assert by_id["3001"]["activity_predates_ticket"] == "true"
    assert by_id["1002"]["activity_predates_ticket"] == "false"
    assert "activity_predates_ticket" in tickets_export.COLUMNS


# ---------------------------------------------------------------------------
# 3. Tickets with no name in the subject
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", [
    "DAF Form Submission - ",
    "DAF Form Submission -",
    "  daf form submission - ",
    "Endowment Form Submission - ",
    "Form Submission",
    "New ticket created from form submission",
    "NEW TICKET CREATED FROM FORM SUBMISSION",
    "",
    "   ",
    None,
])
def test_nameless_subjects_are_replaced(raw):
    subject, nameless = tickets.clean_subject(raw, "DAF Pipeline")
    assert nameless is True
    assert subject == "DAF Pipeline (no name on ticket)"


@pytest.mark.parametrize("raw", [
    "DAF Form Submission - Aisha Testcase",
    "ACH form submitted",
    "Question about grants",
    "New ticket created from form submission by Aisha",
])
def test_named_subjects_are_kept(raw):
    subject, nameless = tickets.clean_subject(raw, "DAF Pipeline")
    assert nameless is False
    assert subject == raw


def test_header_counts_nameless_tickets():
    nameless = [
        ticket(4001, DAF, DAF_NEW, "DAF Form Submission - ", 30),
        ticket(4002, DAF, DAF_NEW, "New ticket created from form submission",
               20),
        ticket(4003, DAF, DAF_NEW, "DAF Form Submission - Someone Real", 10),
    ]
    out = report(StubHubSpot(pages=[nameless]))

    assert "2 tickets have no name in the subject" in out
    assert out.count("DAF Pipeline (no name on ticket)") == 2
    assert "#4003 · DAF Form Submission - Someone Real" in out


def test_one_nameless_ticket_is_singular():
    out = report(StubHubSpot(pages=[
        [ticket(4001, DAF, DAF_NEW, "DAF Form Submission - ", 30)]]))
    assert "1 ticket has no name in the subject" in out


def test_no_nameless_line_when_every_subject_has_a_name():
    assert "no name in the subject" not in report()


def test_export_uses_the_cleaned_subject():
    rows = tickets_export.export_rows(
        [ticket(4001, ACH, ACH_SUBMITTED, "DAF Form Submission - ", 30)],
        OWNERS, now=NOW)
    assert rows[0]["subject"] == "ACH Setup (no name on ticket)"


# ---------------------------------------------------------------------------
# 4. Summary file
# ---------------------------------------------------------------------------

def test_summary_rows_per_pipeline_and_stage():
    rows = tickets_export.export_rows(TICKETS, OWNERS, now=NOW)
    summary = tickets_export.summary_rows(rows)

    assert [list(r.keys()) for r in summary][0] == \
        list(tickets_export.SUMMARY_COLUMNS)
    assert summary == [
        {"pipeline": "ACH Setup", "stage": "Information Submitted",
         "count": 1, "never_touched": 1, "oldest_days": 200},
        {"pipeline": "ACH Setup", "stage": "Waiting on Kods",
         "count": 1, "never_touched": 0, "oldest_days": 15},
        {"pipeline": "DAF Pipeline", "stage": "New",
         "count": 2, "never_touched": 2, "oldest_days": 90},
        {"pipeline": "DAF Pipeline", "stage": "Contacted/Emailed",
         "count": 1, "never_touched": 0, "oldest_days": 40},
        {"pipeline": "Endowment Inquiry", "stage": "New",
         "count": 1, "never_touched": 1, "oldest_days": 5},
    ]


def test_summary_path_sits_beside_the_export():
    assert tickets_export.summary_path_for("tickets_open_2026-09-15.csv") == \
        "tickets_summary_2026-09-15.csv"
    assert tickets_export.summary_path_for("/tmp/x/tickets_open_a.csv") == \
        "/tmp/x/tickets_summary_a.csv"
    assert tickets_export.summary_path_for("/tmp/custom.csv") == \
        "/tmp/custom_summary.csv"


def test_export_main_writes_both_files(tmp_path, monkeypatch):
    hub = StubHubSpot()
    monkeypatch.setattr(tickets_export, "HubSpotClient", lambda: hub.client)
    out = tmp_path / "tickets_open_test.csv"
    monkeypatch.setattr("sys.stdout", io.StringIO())

    assert tickets_export.main(["--out", str(out)]) == 0

    summary = tmp_path / "tickets_summary_test.csv"
    assert summary.exists()
    with open(summary, newline="", encoding="utf-8") as handle:
        read = list(csv.DictReader(handle))
    assert len(read) == 5
    assert list(read[0].keys()) == list(tickets_export.SUMMARY_COLUMNS)


def test_export_files_are_gitignored():
    import os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, ".gitignore"), encoding="utf-8") as handle:
        ignored = handle.read().splitlines()
    assert "tickets_open_*.csv" in ignored
    assert "tickets_summary_*.csv" in ignored


# ===========================================================================
# 4a-fix-2: "my tickets" + links
# ===========================================================================

# Owners with REAL-looking (invented) addresses, since matching is on the
# address inside the label.
OWNERS_PAYLOAD = {"results": [
    {"id": "160587283", "firstName": "Nora", "lastName": "Moorefield",
     "email": "nora@amuslimcf.org"},
    {"id": "160587284", "firstName": "Kods", "lastName": "Zouita",
     "email": "kods@amuslimcf.org"},
    {"id": "500", "email": "bare@amuslimcf.org"},
]}


def personal(query="my tickets", email="nora@amuslimcf.org", pages=None):
    hub = StubHubSpot(pages=pages, owners_payload=OWNERS_PAYLOAD)
    return tickets.handle(query, Ctx(hub.client, email=email)), hub


# --- 1. triggers and owner resolution ------------------------------------

@pytest.mark.parametrize("query", [
    "my tickets", "My open tickets", "how many tickets do I have",
    "How many tickets do I have open?", "tickets assigned to me",
    "what am I waiting on", "What am I waiting on?",
])
def test_personal_phrases_are_claimed(query):
    assert tickets.can_handle(query)
    assert tickets.is_personal(query)


def test_owner_is_resolved_from_the_login_email_case_insensitively():
    out, _ = personal(email="NORA@AmuslimCF.org")

    # Nora owns 1001 (never touched) and 1002 (touched 3d ago).
    assert out.startswith("🎫 **You have 2 open tickets — 1 never touched**")
    assert "#1001" in out and "#1002" in out
    assert "#1004" not in out, "Kods's ticket"
    assert "#1003" not in out, "unassigned"


def test_personal_header_then_the_same_pipeline_layout():
    out, _ = personal(email="kods@amuslimcf.org")

    assert out.startswith("🎫 **You have 2 open tickets — 1 never touched**")
    assert "**ACH Setup** — 2 open, 1 never touched, oldest 200d" in out
    assert "Entered Into CSuite" in out
    assert "#1004" in out and "#1005" in out


def test_personal_singular():
    one = [TICKETS[1]]
    out, _ = personal(pages=[one])
    assert out.startswith("🎫 **You have 1 open ticket — 0 never touched**")


def test_non_owner_gets_the_refusal_and_no_ticket_fetch():
    out, hub = personal(email="volunteer@amuslimcf.org")

    assert out == ("ℹ️ volunteer@amuslimcf.org isn't a HubSpot ticket owner "
                   "— nothing assigned to you.")
    assert hub.posts == [], "no ticket search for someone with no tickets"
    assert "?" not in out, "never ask the user who they are"


def test_missing_actor_email_is_refused_not_guessed():
    hub = StubHubSpot(owners_payload=OWNERS_PAYLOAD)
    out = tickets.handle("my tickets", Ctx(hub.client, email=None))
    assert "isn't a HubSpot ticket owner" in out
    assert hub.posts == []


def test_owner_with_tickets_none_of_them_open():
    out, _ = personal(email="bare@amuslimcf.org")
    assert out == "✅ You have no open tickets."


def test_personal_view_ignores_a_typed_name():
    """"tickets for Kods" is an owner filter; "my tickets for Kods" is
    still MY tickets — identity comes from the login only."""
    out, _ = personal(query="my tickets for Kods", email="nora@amuslimcf.org")
    assert "You have 2 open tickets" in out
    assert "#1004" not in out


def test_personal_view_honours_older_than():
    out, _ = personal(query="my tickets older than 60 days",
                      email="nora@amuslimcf.org")
    assert "You have 1 open ticket (older than 60 days)" in out
    assert "#1001" in out and "#1002" not in out


@pytest.mark.parametrize("label, expected", [
    ("Nora Moorefield (nora@amuslimcf.org)", "nora@amuslimcf.org"),
    ("Nora Moorefield (NORA@AMUSLIMCF.ORG)", "nora@amuslimcf.org"),
    ("bare@amuslimcf.org", "bare@amuslimcf.org"),
    ("Nora Moorefield", None),
    ("owner 42", None),
    ("", None),
    (None, None),
])
def test_owner_email_from_label(label, expected):
    assert tickets.owner_email_from_label(label) == expected


def test_owner_id_for_email():
    owners = {"1": "Nora Moorefield (nora@amuslimcf.org)",
              "2": "kods@amuslimcf.org", "3": "No Address"}
    assert tickets.owner_id_for_email("Nora@amuslimcf.org", owners) == "1"
    assert tickets.owner_id_for_email("kods@amuslimcf.org", owners) == "2"
    assert tickets.owner_id_for_email("nobody@amuslimcf.org", owners) is None
    assert tickets.owner_id_for_email("", owners) is None
    assert tickets.owner_id_for_email("not-an-email", owners) is None
    assert tickets.owner_id_for_email("nora@amuslimcf.org", {}) is None


# --- 2 & 3. links --------------------------------------------------------

def test_ticket_url_is_the_record_form_built_from_the_portal_id():
    url = tickets.ticket_url("1002")
    assert url == ("https://app-na2.hubspot.com/contacts/"
                   f"{Config.HUBSPOT_PORTAL_ID}/record/0-5/1002")
    assert url == Config.HUBSPOT_TICKET_URL.format(ticket_id="1002")


@pytest.mark.parametrize("bad", [None, "", "  ", "None", "none", False])
def test_no_link_without_a_real_id(bad):
    assert tickets.ticket_url(bad) is None


def test_every_ticket_line_ends_with_a_markdown_link():
    out = report()
    lines = [l for l in out.splitlines() if l.startswith("• #")]
    assert lines, "no ticket lines rendered"
    for line in lines:
        ticket_id = line.split(" · ")[0][len("• #"):]
        assert line.endswith(
            f" · [open]({Config.HUBSPOT_TICKET_URL.format(ticket_id=ticket_id)})"
        ), line


def test_a_line_without_a_real_id_has_no_link():
    row = tickets.describe(TICKETS[0], OWNERS, now=NOW)
    row["id"] = "None"
    assert "[open](" not in tickets.ticket_line(row)


def test_export_url_matches_the_report_link():
    rows = tickets_export.export_rows(TICKETS, OWNERS, now=NOW)
    for row in rows:
        assert row["url"] == tickets.ticket_url(row["id"])
        assert "/record/0-5/" in row["url"]


def test_export_has_no_url_builder_of_its_own():
    import inspect
    source = inspect.getsource(tickets_export)
    assert "HUBSPOT_TICKET_URL" not in source
    assert "def ticket_url" not in source


# --- 4. routing ------------------------------------------------------------

def test_how_many_tickets_do_i_have_open_routes_to_tickets():
    from intents import route_intent

    hub = StubHubSpot(owners_payload=OWNERS_PAYLOAD)
    ctx = Ctx(hub.client, email="nora@amuslimcf.org")

    routed = route_intent("how many tickets do I have open", ctx)

    assert routed is not None, "fell through to the Claude fallback"
    name, handler = routed
    assert name == "tickets"
    assert "You have 2 open tickets" in handler("how many tickets do I have open",
                                                 ctx)


@pytest.mark.parametrize("query", [
    "my tickets", "tickets assigned to me", "what am I waiting on",
    "open tickets", "tickets older than 30 days",
])
def test_ticket_phrases_never_reach_the_fallback(query):
    from intents import route_intent

    hub = StubHubSpot(owners_payload=OWNERS_PAYLOAD)
    routed = route_intent(query, Ctx(hub.client, email="nora@amuslimcf.org"))
    assert routed is not None and routed[0] == "tickets"


def test_a_donor_role_cannot_reach_the_ticket_report():
    from intents import route_intent

    hub = StubHubSpot(owners_payload=OWNERS_PAYLOAD)
    ctx = Ctx(hub.client, email="nora@amuslimcf.org")
    ctx.actor.role = "donor"
    routed = route_intent("my tickets", ctx)
    assert routed is None or routed[0] != "tickets"
