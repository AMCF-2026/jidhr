"""intents/content.py — the email draft's structured fields.

The coded templates carry a preview line and an optional button, and
neither can be expressed in the body: the button is its own module and
the template hides it unless BOTH halves are set. So they are fields on
the draft, settable in chat, and shown before anything is saved.

No network. The HubSpot client raises on any attribute access, so a
single HTTP call fails the test rather than passing unnoticed.
"""

import types

import pytest

from clients import email_draft as ed
from intents import content
from intents.context import new_draft_state


class ExplodingHubSpot:
    def __getattr__(self, name):
        raise AssertionError(f"a HubSpot call escaped: {name}()")


class RecordingHubSpot:
    """Enough of the client for create_draft_email's happy path."""

    def __init__(self, stored="/jidhr_shell.html", mode=ed.DESIGN_MANAGER):
        self.calls = []
        self.stored = stored
        self.mode = mode

    def _post(self, endpoint, data=None):
        self.calls.append(("POST", endpoint))
        return {"id": "555"}

    def _get(self, endpoint, params=None):
        self.calls.append(("GET", endpoint))
        return {"id": "555", "emailTemplateMode": self.mode,
                "content": {"templatePath": self.stored}}

    def _delete(self, endpoint):
        self.calls.append(("DELETE", endpoint))
        return {"status_code": 204}


def make_ctx(hubspot=None, **draft):
    state = new_draft_state()
    state.update(active=True, type="email", subject="A subject",
                 body="<p>Some words.</p>")
    state.update(draft)
    return types.SimpleNamespace(
        draft_state=state,
        services=types.SimpleNamespace(hubspot=hubspot or ExplodingHubSpot()))


# ---------------------------------------------------------------------------
# Parsing the model's structured fields
# ---------------------------------------------------------------------------

FULL = """SUBJECT: Your Directory profile
PREVIEW: Two minutes to restore your listing
BUTTON_LABEL: Resubmit your profile
BUTTON_URL: https://amuslimcf.org/nonprofit-directory

BODY:
<p>Assalamu Alaikum,</p>
<p>Please resubmit.</p>"""


def test_every_structured_field_is_parsed():
    fields = content._parse_email_draft(FULL)
    assert fields["subject"] == "Your Directory profile"
    assert fields["preview_text"] == "Two minutes to restore your listing"
    assert fields["button_label"] == "Resubmit your profile"
    assert fields["button_url"] == \
        "https://amuslimcf.org/nonprofit-directory"
    assert fields["body"].startswith("<p>Assalamu Alaikum,</p>")


def test_missing_fields_default_to_empty_strings_not_none():
    """Empty string, so a missing CTA and a cleared CTA look identical."""
    fields = content._parse_email_draft(
        "SUBJECT: Hello\n\nBODY:\n<p>Words.</p>")
    assert fields["preview_text"] == ""
    assert fields["button_label"] == ""
    assert fields["button_url"] == ""
    assert fields["subject"] == "Hello"


def test_a_model_that_echoes_the_placeholder_is_treated_as_blank():
    """"[leave blank]" in a button label is how a button reads that."""
    fields = content._parse_email_draft(
        "SUBJECT: Hello\nPREVIEW: [preview line]\n"
        "BUTTON_LABEL: [leave blank]\nBUTTON_URL: none\n\nBODY:\n<p>Hi.</p>")
    assert fields["preview_text"] == ""
    assert fields["button_label"] == ""
    assert fields["button_url"] == ""


def test_a_missing_subject_still_falls_back():
    fields = content._parse_email_draft("BODY:\n<p>Words.</p>")
    assert fields["subject"] == "AMCF Update"


@pytest.mark.parametrize("label, url", [
    ("Register", ""),                       # label, no destination
    ("", "https://amuslimcf.org"),          # destination, no label
    ("Register", "javascript:alert(1)"),    # unusable scheme
    ("Register", "amuslimcf.org"),          # no scheme at all
])
def test_half_a_button_is_no_button(label, url):
    """The template shows the button only when both are set."""
    fields = content._parse_email_draft(
        f"SUBJECT: S\nBUTTON_LABEL: {label}\nBUTTON_URL: {url}\n\n"
        "BODY:\n<p>Words.</p>")
    assert fields["button_label"] == ""
    assert fields["button_url"] == ""


def test_the_body_keeps_its_blank_lines():
    fields = content._parse_email_draft(FULL)
    assert "\n" in fields["body"]


# ---------------------------------------------------------------------------
# Chat commands
# ---------------------------------------------------------------------------

def test_button_command_sets_both_halves():
    ctx = make_ctx()
    reply = content._apply_email_field_command(
        "button: Resubmit your profile -> https://amuslimcf.org/x", ctx)

    assert ctx.draft_state["button_label"] == "Resubmit your profile"
    assert ctx.draft_state["button_url"] == "https://amuslimcf.org/x"
    assert "Resubmit your profile" in reply


@pytest.mark.parametrize("arrow", ["->", "=>", "→"])
def test_the_arrow_can_be_written_three_ways(arrow):
    ctx = make_ctx()
    content._apply_email_field_command(
        f"button: Go {arrow} https://amuslimcf.org", ctx)
    assert ctx.draft_state["button_url"] == "https://amuslimcf.org"


def test_an_unsafe_button_url_is_rejected_and_the_label_is_kept():
    """Only the address is unusable; the person said what they wanted."""
    ctx = make_ctx()
    reply = content._apply_email_field_command(
        "button: Register now -> javascript:alert(1)", ctx)

    assert ctx.draft_state["button_label"] == "Register now"
    assert not ctx.draft_state["button_url"]
    assert "not a usable button link" in reply
    assert "Register now" in reply


def test_no_button_clears_both():
    ctx = make_ctx(button_label="Register", button_url="https://x.org")
    reply = content._apply_email_field_command("no button", ctx)

    assert ctx.draft_state["button_label"] == ""
    assert ctx.draft_state["button_url"] == ""
    assert "removed" in reply.lower()


@pytest.mark.parametrize("phrase", ["no button", "No Button", "remove button",
                                    "drop button", "clear button",
                                    "  no button  "])
def test_the_ways_of_saying_no_button(phrase):
    ctx = make_ctx(button_label="Register", button_url="https://x.org")
    assert content._apply_email_field_command(phrase, ctx) is not None
    assert ctx.draft_state["button_label"] == ""


def test_preview_command_sets_the_preview_text():
    ctx = make_ctx()
    reply = content._apply_email_field_command(
        "preview: Two minutes to restore your listing", ctx)

    assert ctx.draft_state["preview_text"] == \
        "Two minutes to restore your listing"
    assert "Two minutes" in reply


@pytest.mark.parametrize("query", ["make it shorter", "save this",
                                   "add a button somewhere", "preview",
                                   "button", "buttons are nice"])
def test_anything_else_is_not_a_field_command(query):
    """A refinement must still reach Claude, and a save must still save."""
    assert content._apply_email_field_command(query, make_ctx()) is None


def test_no_button_is_not_mistaken_for_a_save():
    """"no button" contains no save word, but the ordering is what matters.

    The field command runs first, so a draft cannot be saved by a message
    that was only adjusting a field.
    """
    ctx = make_ctx(hubspot=ExplodingHubSpot(),
                   button_label="Register", button_url="https://x.org")
    reply = content._handle_draft_conversation("no button", ctx)
    assert "removed" in reply.lower()
    assert ctx.draft_state["active"] is True


# ---------------------------------------------------------------------------
# The save reply lists what is about to be sent
# ---------------------------------------------------------------------------

def test_the_save_reply_lists_every_field(monkeypatch):
    ctx = make_ctx(hubspot=RecordingHubSpot(),
                   preview_text="Two minutes to restore your listing",
                   button_label="Resubmit", button_url="https://x.org/y")
    reply = content._save_email_draft("save this", ctx, apply=True)

    assert "`standard`" in reply
    assert "Two minutes to restore your listing" in reply
    assert "Resubmit" in reply and "https://x.org/y" in reply
    assert "Date bar:" in reply


def test_the_save_reply_says_no_button_when_there_is_none():
    ctx = make_ctx(hubspot=RecordingHubSpot())
    reply = content._save_email_draft("save this", ctx, apply=True)
    assert "**NO BUTTON**" in reply


def test_a_giving_circle_save_names_the_template_it_used():
    ctx = make_ctx(hubspot=RecordingHubSpot(stored="/Giving_Circle.html"))
    reply = content._save_email_draft("save to the giving circle template",
                                      ctx, apply=True)
    assert "`giving_circle`" in reply


# ---------------------------------------------------------------------------
# apply=True, and the two failures that must not look like success
# ---------------------------------------------------------------------------

def test_the_chat_save_path_now_actually_applies():
    client = RecordingHubSpot()
    ctx = make_ctx(hubspot=client)
    reply = content._handle_draft_conversation("save this", ctx)

    assert ("POST", "marketing/v3/emails") in client.calls
    assert "Saved to HubSpot" in reply
    assert ctx.draft_state["active"] is False


def test_an_unreachable_audit_store_says_not_saved_and_makes_no_call():
    """Never a silent downgrade to a dry run.

    The person asked for a save; "nothing happened" has to say so.
    """
    from clients.audit import AuditUnavailable

    def refuse(*args, **kwargs):
        raise AuditUnavailable("DATABASE_URL is not set")

    ctx = make_ctx(hubspot=ExplodingHubSpot())
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ed, "create_draft_email", refuse)
        reply = content._save_email_draft("save this", ctx, apply=True)

    assert "Draft not saved: audit row could not be reserved" in reply
    assert "NO BUTTON" in reply, "the payload summary is still shown"
    assert "Dry run" not in reply
    # The draft survives, so nothing the person wrote is lost.
    assert ctx.draft_state["active"] is True


def test_a_template_that_did_not_attach_reports_the_archived_draft():
    client = RecordingHubSpot(stored="@hubspot/email/dnd/plain_text.html",
                              mode="DRAG_AND_DROP")
    ctx = make_ctx(hubspot=client)
    reply = content._save_email_draft("save this", ctx, apply=True)

    assert ("DELETE", "marketing/v3/emails/555") in client.calls
    assert "plain_text" in reply
    assert "555" in reply
    assert ctx.draft_state["active"] is True


def test_an_empty_body_is_refused_before_any_call():
    ctx = make_ctx(hubspot=ExplodingHubSpot(), body="<div></div>")
    reply = content._save_email_draft("save this", ctx, apply=True)
    assert "empty" in reply.lower()


def test_the_step_6_constraint_failure_names_its_own_cause():
    """The 2026-09-23 first-save attempt, reproduced.

    write_audit had CHECK (status IN ('success','failed','skipped')), so
    reserving a row with status 'attempted' raised. The write was
    correctly refused — but the reply said "Failed to save email",
    because the client returned an error dict and create_draft_email
    then raised RuntimeError about a missing id. The cause has to be
    named: that message sent someone looking at HubSpot when the problem
    was a Postgres constraint.
    """
    import psycopg2
    from clients.hubspot import HubSpotClient

    sent = []

    def constrained_store(sql, params=None, fetch=True):
        raise psycopg2.errors.CheckViolation(
            'new row for relation "write_audit" violates check constraint '
            '"write_audit_status_check"')

    def sender(url, **kwargs):
        sent.append(url)
        return None

    client = HubSpotClient()
    client.access_token = "test-token"
    ctx = make_ctx(hubspot=client)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("clients.database.execute_query", constrained_store)
        patch.setattr("clients.database.is_configured", lambda: True)
        patch.setattr("clients.hubspot.requests.post", sender)
        reply = content._save_email_draft("save this", ctx, apply=True)

    assert "Draft not saved: audit row could not be reserved" in reply
    assert "write_audit_status_check" in reply, "the constraint is named"
    assert "Failed to save email" not in reply, "the generic branch fired"
    assert sent == [], "the request went out despite having no audit row"
    assert ctx.draft_state["active"] is True


# ---------------------------------------------------------------------------
# What the prompts ask for, and what survives the allowlist
# ---------------------------------------------------------------------------

# A reply in the shape the prompts now demand: real headings, no span,
# no style, secondary CTAs inline, one primary CTA in the fields.
MULTI_SECTION = """SUBJECT: Round One Voting Begins This Weekend
PREVIEW: Voting opens Saturday. Here is who is on the ballot.
BUTTON_LABEL: Cast your vote
BUTTON_URL: https://www.grapevine.org/giving-circle/zjrhaGG

BODY:
<h2>Round one opens Saturday</h2>
<p>Members choose three of the eight nominees. The
<a href="https://amuslimcf.org/donors/giving-circles/nominees/">full nominee
list</a> has each organisation's summary.</p>
<h3>How the ballot works</h3>
<ul><li>Three votes each</li><li>Closes Tuesday</li></ul>
<h2>Also this month</h2>
<p>The <a href="https://amuslimcf.org/events/">events page</a> has the
Directory webinar, and <a href="https://ispu.org/islamophobia/introduction/">
ISPU's report</a> is out.</p>"""

FIXTURE_URLS = {
    "https://www.grapevine.org/giving-circle/zjrhaGG",
    "https://amuslimcf.org/donors/giving-circles/nominees/",
    "https://amuslimcf.org/events/",
    "https://ispu.org/islamophobia/introduction/",
}


def test_a_multi_section_reply_survives_parse_and_sanitise():
    """The shape the prompts ask for is the shape that survives.

    Before the body rules, AMCF newsletters carried their headings as
    <span style="font-size:18px">, which the allowlist strips — so every
    section heading arrived as ordinary body text.
    """
    import re

    fields = content._parse_email_draft(MULTI_SECTION)
    clean = ed.sanitize_body_html(fields["body"])

    assert len(re.findall(r"<h2[ >]", clean)) >= 2
    assert "<h3" in clean
    assert "<span" not in clean
    assert "style=" not in clean

    # Every URL in the fixture reaches the output, in the body or the button.
    found = set(re.findall(r'href="([^"]+)"', clean))
    found.add(fields["button_url"])
    assert FIXTURE_URLS <= found, f"lost: {FIXTURE_URLS - found}"

    # Exactly one primary CTA, and it is in the fields, not the body.
    assert fields["button_label"] == "Cast your vote"
    assert fields["button_url"] == "https://www.grapevine.org/giving-circle/zjrhaGG"
    assert fields["button_url"] not in clean, "the primary CTA leaked into the body"


def test_the_body_rules_are_in_both_prompts():
    """The initial draft and the refinement must agree.

    A refinement that forgets the rules rewrites a compliant body into a
    non-compliant one, and the loss only shows up after sending.
    """
    import inspect
    source = inspect.getsource(content)
    for rule in ("Section headings are real <h2>",
                 "No <span>, no style attributes",
                 "Exactly ONE primary call to action"):
        assert source.count(rule) == 2, f"{rule!r} is not in both prompts"


def test_a_styled_heading_still_arrives_as_body_text():
    """Why the rule exists, pinned as a test."""
    clean = ed.sanitize_body_html(
        '<p><span style="font-size:18px; color:#5a9366">'
        '<strong>Round one opens</strong></span></p>')
    assert "<h2" not in clean
    assert clean == "<p><strong>Round one opens</strong></p>"


# ---------------------------------------------------------------------------
# Scope: a brief with sections keeps all of them
# ---------------------------------------------------------------------------

# Seven sections, the shape a real AMCF newsletter arrives in. The
# 2026-09-23 dry run compressed a 27-line brief to two sections, which is
# faithful to the links and not to the scope: a newsletter with eight
# items is an email with eight sections, and the person who wrote the
# missing one does not find out until it has gone out.
SECTION_TITLES = [
    "Round one opens Saturday",
    "Meet the nominees",
    "Greater Giving Summit",
    "Directory webinar",
    "AFP Muslim Affinity Group",
    "AmeriCorps openings",
    "SAID Fellowship",
]
SECTION_URLS = [
    "https://amuslimcf.org/donors/giving-circles/nominees/",
    "https://amuslimcf.org/events/grow-your-nonprofits-reach-directory-webinar/",
    "https://amuslimcf.org/events/afp-muslim-affinity-group-fall-2026/",
    "https://www.americorps.gov/",
    "https://www.saidfellowship.com/",
]

MANY_SECTIONS = "\n".join(
    ["SUBJECT: A full week at AMCF",
     "PREVIEW: Voting, nominees, and five more things",
     "BUTTON_LABEL: Cast your vote",
     "BUTTON_URL: https://www.grapevine.org/giving-circle/zjrhaGG",
     "", "BODY:"]
    + [f"<h2>{title}</h2><p>Something about it.</p>"
       for title in SECTION_TITLES[:2]]
    + [f'<h2>{SECTION_TITLES[i + 2]}</h2><p>See '
       f'<a href="{url}">the page</a>.</p>'
       for i, url in enumerate(SECTION_URLS)])


def test_every_section_in_the_brief_survives_as_its_own_heading():
    import re

    fields = content._parse_email_draft(MANY_SECTIONS)
    clean = ed.sanitize_body_html(fields["body"])

    headings = re.findall(r"<h2>(.*?)</h2>", clean)
    assert len(headings) == len(SECTION_TITLES), \
        f"{len(SECTION_TITLES)} sections in, {len(headings)} out"
    assert headings == SECTION_TITLES, "sections were reordered or merged"

    found = set(re.findall(r'href="([^"]+)"', clean))
    assert set(SECTION_URLS) <= found, f"lost: {set(SECTION_URLS) - found}"


def test_the_scope_rule_is_in_both_prompts():
    import inspect
    source = inspect.getsource(content)
    for rule in ("Keep EVERY section",
                 "applies ONLY to a brief with no sections"):
        assert source.count(rule) == 2, f"{rule!r} is not in both prompts"


# ---------------------------------------------------------------------------
# The repetition guard is advisory
# ---------------------------------------------------------------------------

def test_the_repetition_guard_cannot_prevent_a_save():
    """It informs; it does not gate.

    The note is built when a draft is generated, not when one is saved,
    and a topic covered four times in six weeks is a reason for a person
    to think again — not a reason for Jidhr to refuse.
    """
    import inspect

    source = inspect.getsource(content._save_email_draft)
    assert "_repetition_note" not in source
    assert "find_topic_matches" not in source

    # And it holds in practice, with the guard loudly claiming a match.
    client = RecordingHubSpot()
    ctx = make_ctx(hubspot=client)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(content, "_repetition_note",
                      lambda topic: "⚠️ covered 9× in the last six weeks")
        reply = content._save_email_draft("save this", ctx, apply=True)

    assert "Saved to HubSpot" in reply
    assert ("POST", "marketing/v3/emails") in client.calls


def test_a_broken_repetition_guard_cannot_break_a_draft():
    """Its own docstring promises this; pin it."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(content, "find_topic_matches",
                      lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db")))
        assert content._repetition_note("anything") is None
