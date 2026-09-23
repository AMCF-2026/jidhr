"""
Marketing Email Drafts
======================
Builds a HubSpot marketing email from a repo-owned body and one of the
three AMCF coded templates, and creates it as a DRAFT. Never schedules,
never sends.

The division, set on 2026-09-23 (docs/DECISIONS.md): the chrome —
header, logo, colours, fonts, footer, socials — lives in the HubSpot
coded templates, where staff edit it without a deploy. Jidhr supplies
five things and no styling at all: body_html, date_bar, preview_text,
and an optional button_label/button_url pair.

    payload = build_email_payload("standard", body_html=body,
                                  date_bar="September 30, 2026",
                                  preview_text="What's inside")
    create_draft_email(payload)                 # dry run, no HTTP
    create_draft_email(payload, apply=True)     # creates the draft

Why templatePath sits inside `content` and never at the top level
-----------------------------------------------------------------
Measured on 2026-09-23 with two probes, both since archived:

    probe 1  399921857254  templatePath at the TOP level, as HubSpot's
                           own create-email reference shows it. HubSpot
                           answered 201 and stored
                           content.templatePath = '@hubspot/email/dnd/
                           plain_text.html', emailTemplateMode =
                           DRAG_AND_DROP. The path was silently
                           discarded — not rejected, so nothing could
                           fall back or retry.
    probe 2  399908795086  the same payload with templatePath moved
                           into content. Stored
                           content.templatePath = '/jidhr_shell.html',
                           emailTemplateMode = DESIGN_MANAGER.

One nesting level was the whole difference, and it is why every email
built through the API for the previous fortnight arrived with default
portal styling. create_draft_email verifies both fields after creating
the draft rather than trusting the 201, because a 201 is exactly what
the broken shape returned.
"""

import html
import json
import logging
from html.parser import HTMLParser

logger = logging.getLogger(__name__)

# The three coded templates, by the path HubSpot stores. Read from the
# portal on 2026-09-23 and mirrored byte-for-byte into templates/email/.
# All three expose the same six slots; the slot contract is what lets one
# payload builder serve all of them.
EMAIL_TEMPLATES = {
    "standard": "/jidhr_shell.html",
    "giving_circle": "/Giving_Circle.html",
    "socal": "/SoCal.html",
}

# The six slots, exactly as the templates declare them and as HubSpot
# stored them in probe 2. The first five are template-level widgets
# holding a scalar at body.value; the sixth is a module whose body is a
# bag of that module's own fields, and @hubspot/email_body has one field,
# `html`.
BODY_MODULE = "email_template_main_email_body"
WIDGET_SLOTS = ("preview_text", "date_bar", "button_label", "button_url",
                "show_separator")

# What HubSpot must report back for the template to have actually
# attached. DRAG_AND_DROP here means it fell back to a default.
DESIGN_MANAGER = "DESIGN_MANAGER"

# Tags a generated body may use. Everything else is layout or styling,
# and layout and styling belong to the template.
ALLOWED_TAGS = frozenset(
    ("p", "br", "ul", "ol", "li", "a", "strong", "em", "b", "i", "h2", "h3",
     "img"))

# Per-tag attribute allowlist. Empty means "no attributes at all", which
# is the case for every tag but two.
ALLOWED_ATTRS = {
    "a": frozenset(("href",)),
    # Dimensions are kept because an image without them reflows the
    # whole email while it loads in Outlook.
    "img": frozenset(("src", "alt", "width", "height")),
}

# Attributes that decide where a tag points, rather than how it looks.
# If one of these fails its scheme check the ELEMENT goes, not just the
# attribute: an <a> with no href is still its own text, but an <img>
# with no src is a broken image icon, and its alt would be left reading
# as a sentence in the middle of the body. An image is not a sentence.
REQUIRED_URL_ATTRS = {"img": "src"}

# Tags whose CONTENT is dropped along with the tag. Everything else that
# is not allowed has its tag removed and its text kept, because silently
# losing a sentence is harder to notice than losing a <div>.
DROP_CONTENT_TAGS = frozenset(("script", "style", "head", "title"))

VOID_TAGS = frozenset(("br", "img"))

# An image may only be loaded over https. http would break the padlock
# in every modern client, and any other scheme is not an image at all.
IMAGE_SCHEMES = ("https://",)

# Schemes a link may use. A javascript: href in a marketing email is the
# one injection this allowlist exists to stop.
SAFE_SCHEMES = ("http://", "https://", "mailto:")


class UnknownTemplate(KeyError):
    """A template key that is not in EMAIL_TEMPLATES."""


class EmptyBody(ValueError):
    """The body was empty, or contained nothing that survived the allowlist."""


class TemplateDidNotAttach(RuntimeError):
    """HubSpot stored a different template than the one requested.

    Raised after the draft has already been archived — see
    create_draft_email. This is probe 1's failure, caught instead of
    shipped.
    """


# ---------------------------------------------------------------------------
# Sanitising
# ---------------------------------------------------------------------------

def safe_href(value, schemes=SAFE_SCHEMES) -> str | None:
    """A URL, or None if its scheme is not one of `schemes`."""
    text = (value or "").strip()
    if text.lower().startswith(tuple(schemes)):
        return text
    return None


class _Sanitiser(HTMLParser):
    """Rewrites a fragment down to ALLOWED_TAGS, keeping the words."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.text_seen = False
        self.stripped = set()
        self._open = []          # allowed tags still open, for closing
        self._suppress = 0       # depth inside a drop-content tag

    # -- tags --------------------------------------------------------
    def handle_starttag(self, tag, attrs):
        if tag in DROP_CONTENT_TAGS:
            self._suppress += 1
            self.stripped.add(tag)
            return
        if self._suppress:
            return
        if tag not in ALLOWED_TAGS:
            # The tag goes, its words stay.
            self.stripped.add(tag)
            return

        kept = []
        allowed = ALLOWED_ATTRS.get(tag, frozenset())
        required = REQUIRED_URL_ATTRS.get(tag)
        have_required = False

        for name, value in attrs:
            name = (name or "").lower()
            if name.startswith("on") or name == "style" or name not in allowed:
                if name:
                    self.stripped.add(f"{tag}[{name}]")
                continue
            if tag == "a" and name == "href":
                value = safe_href(value)
                if value is None:
                    self.stripped.add("a[href:unsafe-scheme]")
                    continue
            if name == required:
                value = safe_href(value, IMAGE_SCHEMES)
                if value is None:
                    # The whole element goes — see REQUIRED_URL_ATTRS.
                    self.stripped.add(f"{tag}[{required}:not-https]")
                    return
                have_required = True
            kept.append(f' {name}="{html.escape(value or "", quote=True)}"')

        if required and not have_required:
            self.stripped.add(f"{tag}[no-{required}]")
            return

        if tag in VOID_TAGS:
            self.out.append(f"<{tag}{''.join(kept)}>")
            return
        self.out.append(f"<{tag}{''.join(kept)}>")
        self._open.append(tag)

    def handle_startendtag(self, tag, attrs):
        # Routed through handle_starttag so a self-closed <img /> meets
        # the same src check as an open one.
        if tag in VOID_TAGS:
            self.handle_starttag(tag, attrs)
        elif tag not in ALLOWED_TAGS:
            self.stripped.add(tag)

    def handle_endtag(self, tag):
        if tag in DROP_CONTENT_TAGS:
            self._suppress = max(0, self._suppress - 1)
            return
        if self._suppress or tag in VOID_TAGS:
            return
        if tag in ALLOWED_TAGS and tag in self._open:
            # Close back to it, so unbalanced input cannot leak an open tag.
            while self._open:
                open_tag = self._open.pop()
                self.out.append(f"</{open_tag}>")
                if open_tag == tag:
                    break

    # -- text --------------------------------------------------------
    def handle_data(self, data):
        if self._suppress or not data:
            return
        self.out.append(html.escape(data, quote=False))
        if data.strip():
            self.text_seen = True

    def has_content(self) -> bool:
        """Text, or an image. An email that is one banner is still an email."""
        return self.text_seen or "<img" in "".join(self.out)

    def result(self) -> str:
        while self._open:
            self.out.append(f"</{self._open.pop()}>")
        return "".join(self.out).strip()


def sanitize_body_html(body_html) -> str:
    """A generated body reduced to ALLOWED_TAGS. Raises EmptyBody if nothing
    is left.

    Tables, `div`, `style` attributes, `on*` handlers and `script` are
    removed. Not because the input is untrusted — Jidhr writes it — but
    because the templates own every pixel of layout and styling, and a
    stray inline colour is how an email ends up half AMCF green and half
    HubSpot teal. What was stripped is logged by name: a body that
    silently loses its formatting is the failure this repo keeps meeting.
    """
    if body_html is None:
        raise EmptyBody("body_html is None")

    parser = _Sanitiser()
    parser.feed(str(body_html))
    parser.close()
    cleaned = parser.result()

    if parser.stripped:
        logger.info("body_html: stripped %s",
                    ", ".join(sorted(parser.stripped)))

    if not parser.has_content():
        raise EmptyBody(
            "body_html has no text once tags are stripped — refusing to "
            f"build an email with an empty body (input was {len(str(body_html))} "
            "chars)")
    return cleaned


# ---------------------------------------------------------------------------
# The payload
# ---------------------------------------------------------------------------

def _widget(value) -> dict:
    return {"body": {"value": value}}


def build_email_payload(template: str, body_html: str, date_bar: str,
                        preview_text: str, button_label: str = "",
                        button_url: str = "", name: str = None,
                        subject: str = None, show_separator: bool = True
                        ) -> dict:
    """The exact shape probe 2 proved, for one of the three templates.

    `templatePath` goes inside `content` and NEVER at the top level. See
    the module docstring: top-level is what HubSpot's own reference
    shows, it returns 201, and it silently substitutes
    @hubspot/email/dnd/plain_text.html (probe 399921857254). Inside
    `content` it attaches (probe 399908795086).
    """
    try:
        template_path = EMAIL_TEMPLATES[template]
    except KeyError:
        raise UnknownTemplate(
            f"{template!r} is not a known email template. Available: "
            f"{', '.join(sorted(EMAIL_TEMPLATES))}") from None

    body = sanitize_body_html(body_html)
    subject = subject if subject is not None else (preview_text or "")
    label, url = (button_label or "").strip(), (button_url or "").strip()
    if label and not safe_href(url):
        # Half a button renders as nothing in the template's {% if %},
        # which would be a silent omission.
        raise ValueError(
            "button_label was given without a usable button_url "
            f"({url!r}) — the template shows the button only when both "
            "are set, so this would silently render no button")

    return {
        "name": name if name is not None else subject,
        "subject": subject,
        "content": {
            "templatePath": template_path,
            "widgets": {
                "preview_text": _widget(preview_text or ""),
                "date_bar": _widget(date_bar or ""),
                "button_label": _widget(label),
                "button_url": _widget(url),
                "show_separator": _widget(bool(show_separator)),
                BODY_MODULE: {"type": "module", "body": {"html": body}},
            },
        },
    }


# ---------------------------------------------------------------------------
# Creating the draft
# ---------------------------------------------------------------------------

def create_draft_email(payload: dict, *, apply: bool = False, client=None):
    """Create the draft. Dry run by default: prints the payload, no HTTP.

    With apply=True it POSTs, reads the email back, and checks that
    HubSpot stored the template that was asked for. If it did not, the
    draft is archived immediately and TemplateDidNotAttach is raised —
    leaving a wrongly-templated draft in the portal is how the previous
    fortnight's emails got sent.

    Never sets a schedule and never publishes. The result is a DRAFT and
    a person sends it.
    """
    requested = (payload.get("content") or {}).get("templatePath")

    if not apply:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print(f"\nDRY RUN — no HubSpot call made. templatePath "
              f"{requested!r} would be set inside `content`. "
              "Pass apply=True to create the draft.")
        return None

    if client is None:
        from clients.hubspot import HubSpotClient
        client = HubSpotClient()

    created = client._post("marketing/v3/emails", payload)
    email_id = (created or {}).get("id")
    if not email_id:
        raise RuntimeError(
            f"HubSpot did not return an email id: "
            f"{(created or {}).get('error') or created}")

    # A 201 is not evidence. Probe 1 returned one.
    back = client._get(f"marketing/v3/emails/{email_id}")
    stored = ((back or {}).get("content") or {}).get("templatePath")
    mode = (back or {}).get("emailTemplateMode")

    if stored != requested or mode != DESIGN_MANAGER:
        archived = client._delete(f"marketing/v3/emails/{email_id}")
        logger.error(
            "email %s asked for %s and got %s (mode %s) — archived: %s",
            email_id, requested, stored, mode, archived)
        raise TemplateDidNotAttach(
            f"HubSpot stored templatePath={stored!r} mode={mode!r} when "
            f"{requested!r} / {DESIGN_MANAGER} was requested. Draft "
            f"{email_id} has been archived. This is the probe-1 failure: "
            "the create returned 2xx and substituted a default template.")

    logger.info("email %s created as a draft on %s", email_id, stored)
    return back
