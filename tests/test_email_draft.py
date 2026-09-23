"""clients/email_draft.py — the payload shape two live probes settled.

No network. The HubSpot client is a fake that records what it was asked
to do, so the one test that matters most here — a dry run makes zero HTTP
calls — can assert on the absence of calls rather than on a mock's mood.

Every address, name and event in this file is invented.
"""

import json

import pytest

from clients import email_draft as ed


# ---------------------------------------------------------------------------
# A HubSpot that records instead of calling
# ---------------------------------------------------------------------------

class FakeHubSpot:
    """Answers create/read/delete from fixtures and records every call."""

    def __init__(self, stored_path="/jidhr_shell.html",
                 mode=ed.DESIGN_MANAGER, email_id="123", create=None):
        self.calls = []
        self.stored_path = stored_path
        self.mode = mode
        self.email_id = email_id
        self._create = create

    def _post(self, endpoint, data=None):
        self.calls.append(("POST", endpoint, data))
        if self._create is not None:
            return self._create
        return {"id": self.email_id}

    def _get(self, endpoint, params=None):
        self.calls.append(("GET", endpoint, params))
        return {"id": self.email_id,
                "emailTemplateMode": self.mode,
                "content": {"templatePath": self.stored_path}}

    def _delete(self, endpoint):
        self.calls.append(("DELETE", endpoint, None))
        return {"status_code": 204}

    @property
    def endpoints(self):
        return [(method, endpoint) for method, endpoint, _ in self.calls]


BODY = "<p>Hello.</p><p>A <a href=\"https://amuslimcf.org\">link</a>.</p>"


def payload_for(template="standard", **kwargs):
    kwargs.setdefault("body_html", BODY)
    kwargs.setdefault("date_bar", "September 30, 2026")
    kwargs.setdefault("preview_text", "What's inside")
    return ed.build_email_payload(template, **kwargs)


# ---------------------------------------------------------------------------
# The payload shape — what the two probes proved
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("key, path", [
    ("standard", "/jidhr_shell.html"),
    ("giving_circle", "/Giving_Circle.html"),
    ("socal", "/SoCal.html"),
])
def test_template_path_goes_inside_content_and_never_at_the_top(key, path):
    """The one-nesting-level difference between probe 1 and probe 2.

    Top level returned 201 and silently substituted plain_text
    (399921857254). Inside `content` it attached (399908795086).
    """
    payload = payload_for(key)
    assert payload["content"]["templatePath"] == path
    assert "templatePath" not in payload, \
        "templatePath at the top level is silently discarded by HubSpot"


@pytest.mark.parametrize("key", sorted(ed.EMAIL_TEMPLATES))
def test_all_three_templates_emit_the_same_six_slots(key):
    widgets = payload_for(key)["content"]["widgets"]
    assert set(widgets) == set(ed.WIDGET_SLOTS) | {ed.BODY_MODULE}


def test_the_five_widgets_carry_a_scalar_and_the_module_carries_html():
    widgets = payload_for()["content"]["widgets"]
    for slot in ed.WIDGET_SLOTS:
        assert set(widgets[slot]) == {"body"}
        assert set(widgets[slot]["body"]) == {"value"}
    module = widgets[ed.BODY_MODULE]
    assert module["type"] == "module"
    assert set(module["body"]) == {"html"}


def test_show_separator_is_a_real_boolean_not_a_string():
    """HubSpot stored `true`, not "true", in probe 2."""
    widgets = payload_for()["content"]["widgets"]
    assert widgets["show_separator"]["body"]["value"] is True
    off = payload_for(show_separator=False)["content"]["widgets"]
    assert off["show_separator"]["body"]["value"] is False


def test_the_values_land_in_their_slots():
    payload = payload_for(date_bar="September 30, 2026",
                          preview_text="What's inside",
                          button_label="Register",
                          button_url="https://amuslimcf.org/x")
    widgets = payload["content"]["widgets"]
    assert widgets["date_bar"]["body"]["value"] == "September 30, 2026"
    assert widgets["preview_text"]["body"]["value"] == "What's inside"
    assert widgets["button_label"]["body"]["value"] == "Register"
    assert widgets["button_url"]["body"]["value"] == "https://amuslimcf.org/x"


def test_an_unknown_template_key_raises():
    with pytest.raises(ed.UnknownTemplate) as caught:
        payload_for("newsletter")
    assert "newsletter" in str(caught.value)
    # The message names what IS available, so the caller can fix it.
    for key in ed.EMAIL_TEMPLATES:
        assert key in str(caught.value)


def test_a_button_label_without_a_usable_url_is_refused():
    """The template renders the button only when both are set.

    Half a button is a silent omission, which is worse than an error.
    """
    with pytest.raises(ValueError):
        payload_for(button_label="Register", button_url="")
    with pytest.raises(ValueError):
        payload_for(button_label="Register", button_url="javascript:alert(1)")
    # Neither alone is a problem.
    assert payload_for(button_url="https://amuslimcf.org")


def test_the_payload_is_json_serialisable():
    json.dumps(payload_for())


# ---------------------------------------------------------------------------
# The sanitiser
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tag", sorted(ed.ALLOWED_TAGS - {"br", "a"}))
def test_allowed_tags_survive(tag):
    out = ed.sanitize_body_html(f"<{tag}>text</{tag}>")
    assert out == f"<{tag}>text</{tag}>"


def test_br_and_links_survive():
    assert ed.sanitize_body_html("<p>a<br>b</p>") == "<p>a<br>b</p>"
    assert ed.sanitize_body_html('<a href="https://x.org">go</a>') == \
        '<a href="https://x.org">go</a>'
    assert ed.sanitize_body_html('<a href="mailto:a@b.org">mail</a>') == \
        '<a href="mailto:a@b.org">mail</a>'


@pytest.mark.parametrize("raw, expected", [
    # The tag goes, the words stay — losing a sentence is harder to
    # notice than losing a wrapper.
    ("<div><p>kept</p></div>", "<p>kept</p>"),
    ("<span>kept</span>", "kept"),
    ("<table><tr><td>kept</td></tr></table>", "kept"),
    ("<h1>kept</h1>", "kept"),
    # Styling attributes go; the element stays.
    ('<p style="color:#00a4bd">kept</p>', "<p>kept</p>"),
    ('<p onclick="x()">kept</p>', "<p>kept</p>"),
    ('<p class="x" id="y">kept</p>', "<p>kept</p>"),
    ('<a href="https://x.org" style="color:red">go</a>',
     '<a href="https://x.org">go</a>'),
])
def test_layout_and_styling_are_stripped_but_the_words_remain(raw, expected):
    assert ed.sanitize_body_html(raw) == expected


def test_script_loses_its_contents_too():
    out = ed.sanitize_body_html("<p>before</p><script>evil()</script>"
                                "<p>after</p>")
    assert "evil" not in out
    assert out == "<p>before</p><p>after</p>"


@pytest.mark.parametrize("href", [
    "javascript:alert(1)", "JavaScript:alert(1)", "data:text/html,x",
    "vbscript:x", "  javascript:alert(1)  ",
])
def test_an_unsafe_link_scheme_is_dropped_but_the_text_is_kept(href):
    out = ed.sanitize_body_html(f'<p><a href="{href}">click</a></p>')
    assert "javascript" not in out.lower()
    assert "data:" not in out
    assert "click" in out


def test_an_unbalanced_tag_cannot_leak():
    out = ed.sanitize_body_html("<p>one<p>two")
    assert out.count("<p>") == out.count("</p>")


@pytest.mark.parametrize("raw", ["", "   ", None, "<div></div>",
                                 "<script>evil()</script>", "<br>",
                                 "<p></p>"])
def test_an_empty_result_raises(raw):
    with pytest.raises(ed.EmptyBody):
        ed.sanitize_body_html(raw)


def test_what_was_stripped_is_logged(caplog):
    with caplog.at_level("INFO"):
        ed.sanitize_body_html('<div><table><p style="x">kept</p></table></div>')
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "div" in logged and "table" in logged and "p[style]" in logged


def test_the_body_reaching_the_payload_is_the_sanitised_one():
    payload = payload_for(body_html='<div><p style="color:red">hi</p></div>')
    assert payload["content"]["widgets"][ed.BODY_MODULE]["body"]["html"] == \
        "<p>hi</p>"


# ---------------------------------------------------------------------------
# create_draft_email
# ---------------------------------------------------------------------------

def test_a_dry_run_makes_no_http_call_at_all(capsys):
    client = FakeHubSpot()
    result = ed.create_draft_email(payload_for(), client=client)

    assert result is None
    assert client.calls == [], f"a dry run called HubSpot: {client.endpoints}"
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "/jidhr_shell.html" in out


def test_apply_creates_a_draft_and_verifies_the_stored_template():
    client = FakeHubSpot(email_id="999")
    back = ed.create_draft_email(payload_for(), apply=True, client=client)

    assert back["id"] == "999"
    assert client.endpoints == [
        ("POST", "marketing/v3/emails"),
        ("GET", "marketing/v3/emails/999"),
    ]
    # Never schedules, never publishes.
    assert not any("publish" in endpoint or "schedule" in endpoint
                   for _, endpoint in client.endpoints)


def test_a_plain_text_substitution_archives_the_draft_and_raises():
    """Probe 1's failure, caught instead of shipped.

    HubSpot answers 201 and stores a different template; the draft must
    not be left sitting in the portal where someone could send it.
    """
    client = FakeHubSpot(email_id="777",
                         stored_path="@hubspot/email/dnd/plain_text.html",
                         mode="DRAG_AND_DROP")
    with pytest.raises(ed.TemplateDidNotAttach) as caught:
        ed.create_draft_email(payload_for(), apply=True, client=client)

    assert ("DELETE", "marketing/v3/emails/777") in client.endpoints
    assert "plain_text" in str(caught.value)
    assert "777" in str(caught.value)


def test_the_right_path_with_the_wrong_mode_also_archives_and_raises():
    """DRAG_AND_DROP means HubSpot resolved a default, not the coded shell."""
    client = FakeHubSpot(email_id="778", mode="DRAG_AND_DROP")
    with pytest.raises(ed.TemplateDidNotAttach):
        ed.create_draft_email(payload_for(), apply=True, client=client)
    assert ("DELETE", "marketing/v3/emails/778") in client.endpoints


def test_a_create_that_returns_no_id_raises_before_any_read():
    client = FakeHubSpot(create={"error": "Bad Request"})
    with pytest.raises(RuntimeError) as caught:
        ed.create_draft_email(payload_for(), apply=True, client=client)
    assert "Bad Request" in str(caught.value)
    assert client.endpoints == [("POST", "marketing/v3/emails")]
