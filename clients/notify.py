"""One seam for telling a human that something stopped.

Why this exists
---------------
Every guard in the registrations and contacts paths stops the run and
writes its reason to run_log. That is enough while a person is watching the
reply they just triggered, and useless the moment a run is unattended: the
go-live readiness report found no notifier of any kind, so a stopped
background run was discoverable only by typing "contacts status" or
querying run_log by hand.

Rules this module lives by
--------------------------
* It can NEVER fail a run. Every error is caught and logged, the timeout is
  short, and notify() returns a result rather than raising. A notifier that
  can take down a donation sync is worse than no notifier.
* It can never DELAY a run much either — one HTTP call with a 5-second cap.
* No personal data. Callers pass masked addresses and counts; this module
  does not reach for anything itself, and it redacts any address that slips
  through, the same way clients/audit and the contact-create path do.
* Unset is a no-op, logged ONCE. A deployment with no webhook configured
  must not log a warning per guard, per run, forever.
"""

import logging
import re

logger = logging.getLogger(__name__)

# One HTTP call, and a short one. A guard that already decided to stop is
# not improved by waiting 30 seconds to say so.
TIMEOUT_SECONDS = 5

# Every subject starts with one of these, so the mail can be filtered and
# searched. Applied HERE rather than at the call sites: a prefix a caller
# has to remember is a prefix one caller will forget, and the one it
# forgets is the alert nobody's filter catches.
#
# Suggested Gmail filter: subject:([Jidhr ) -> never send to spam, label
# "Jidhr". Same-domain authenticated mail should inbox anyway, but a filter
# makes that certain rather than likely.
ALERT_PREFIX = "[Jidhr ALERT]"
TEST_PREFIX = "[Jidhr TEST]"
# The daily heartbeat. A run that succeeded still sends one, because a
# MISSING email is how anybody knows the schedule died — silence from a
# cron job is indistinguishable from a cron job that never fired.
OK_PREFIX = "[Jidhr OK]"

# `kind` is reserved in notify()'s keyword arguments — it chooses the
# prefix and is never printed as a context line.
KIND_ALERT = "alert"
KIND_TEST = "test"
KIND_OK = "ok"

# Submission port, STARTTLS. Gmail accepts 587 (explicit TLS) and 465
# (implicit TLS), and they need DIFFERENT client calls: smtplib.SMTP then
# starttls() for 587, smtplib.SMTP_SSL for 465. Getting that pairing wrong
# does not raise anything readable — it hangs until the timeout — and this
# module swallows its own errors by design, so the symptom would be an
# email that never arrives and no clue why. Both are supported, chosen by
# port, rather than one being a trap.
DEFAULT_SMTP_PORT = 587
IMPLICIT_TLS_PORTS = (465,)

# Belt over the callers' braces: they pass masked addresses, and anything
# that gets through here is redacted rather than sent.
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")

# "Nothing is configured" is said once per process, not once per guard.
_warned_unconfigured = False


def redact(text) -> str:
    """Any email address replaced by a placeholder."""
    return _EMAIL_RE.sub("<address redacted>", str(text or ""))


def _targets():
    """(slack_webhook_url or None, email_to or None), read at call time."""
    import os

    slack = (os.environ.get("NOTIFY_SLACK_WEBHOOK_URL") or "").strip()
    email = (os.environ.get("NOTIFY_EMAIL_TO") or "").strip()
    return (slack or None), (email or None)


def configured() -> bool:
    slack, email = _targets()
    return bool(slack or email)


def smtp_port() -> int:
    """The submission port, defaulting to 587.

    A value that will not parse falls back rather than raising: the caller
    is an error path, and failing to report a failure is worse than
    reporting it over the default port.
    """
    import os

    raw = (os.environ.get("NOTIFY_SMTP_PORT") or "").strip()
    try:
        port = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_SMTP_PORT
    return port if port > 0 else DEFAULT_SMTP_PORT


def settings() -> list:
    """[(env var name, set?, shown value)] — for "test notify".

    So the names never have to be read out of the source again, and so a
    test email that does not arrive can be diagnosed from chat. The
    password is reported as set or unset and NEVER shown.
    """
    import os

    out = []
    for name in ("NOTIFY_EMAIL_TO", "NOTIFY_SMTP_HOST", "NOTIFY_SMTP_PORT",
                 "NOTIFY_SMTP_USER", "NOTIFY_SMTP_PASSWORD",
                 "NOTIFY_EMAIL_FROM", "NOTIFY_SLACK_WEBHOOK_URL"):
        raw = (os.environ.get(name) or "").strip()
        if name == "NOTIFY_SMTP_PASSWORD":
            shown = "set (not shown)" if raw else "—"
        elif name == "NOTIFY_SLACK_WEBHOOK_URL":
            shown = "set (not shown)" if raw else "— (Slack off)"
        elif name == "NOTIFY_SMTP_PORT":
            shown = raw or f"— (defaults to {DEFAULT_SMTP_PORT})"
        else:
            shown = raw or "—"
        out.append((name, bool(raw), shown))
    return out


def subject_line(subject, kind=KIND_ALERT) -> str:
    """The subject, prefixed exactly once.

    Idempotent: a subject that already carries a prefix is re-prefixed
    rather than double-prefixed, so a caller passing one through twice
    cannot produce "[Jidhr ALERT] [Jidhr ALERT] ...".
    """
    text = str(subject or "").strip()
    for existing in (ALERT_PREFIX, TEST_PREFIX, OK_PREFIX):
        while text.startswith(existing):
            text = text[len(existing):].strip()
    prefix = {KIND_TEST: TEST_PREFIX, KIND_OK: OK_PREFIX}.get(
        kind, ALERT_PREFIX)
    return f"{prefix} {text}".strip()


def notify(subject, body, kind=KIND_ALERT, **context) -> dict:
    """Tell a human. Returns what happened; never raises.

    `body` is the reason, in plain words. `context` becomes `key: value`
    lines — run_log id, path, event, counts. Keep it to ids, counts and
    MASKED addresses; anything that looks like an address is redacted
    before it is sent.

    `kind` picks the subject prefix and is never printed as a line.
    """
    global _warned_unconfigured

    subject = subject_line(subject, kind)
    slack, email = _targets()
    result = {"configured": bool(slack or email), "sent": [], "failed": []}

    if not result["configured"]:
        if not _warned_unconfigured:
            logger.warning(
                "notify() called but neither NOTIFY_SLACK_WEBHOOK_URL nor "
                "NOTIFY_EMAIL_TO is set — notifications are off. This is "
                "logged once per process.")
            _warned_unconfigured = True
        return result

    text = redact(_format(subject, body, context))

    if slack:
        if _send_slack(slack, text):
            result["sent"].append("slack")
        else:
            result["failed"].append("slack")
    if email:
        if _send_email(email, redact(str(subject)), text):
            result["sent"].append("email")
        else:
            result["failed"].append("email")
    return result


def _format(subject, body, context) -> str:
    """Plain text. No markup.

    Read in a mail client, so there is nothing to render it: the Slack
    `*bold*` this used to emit arrived as literal asterisks in email, and
    Slack reads perfectly well without them.
    """
    lines = [subject, "", str(body or "").strip()]
    if context:
        lines.append("")
        for key in sorted(context):
            value = context[key]
            if value is None or value == [] or value == {}:
                continue
            lines.append(f"{key}: {value}")
    lines += ["", "— Jidhr"]
    return "\n".join(lines)


def _send_slack(url, text) -> bool:
    try:
        import requests

        response = requests.post(url, json={"text": text},
                                 timeout=TIMEOUT_SECONDS)
        if 200 <= getattr(response, "status_code", 0) < 300:
            return True
        logger.warning("notify: Slack returned HTTP %s",
                       getattr(response, "status_code", "?"))
        return False
    except Exception as e:
        # Swallowed on purpose. See the module docstring.
        logger.warning("notify: Slack post failed: %s: %s",
                       type(e).__name__, e)
        return False


def _send_email(to_address, subject, text) -> bool:
    """Email is sent only if an SMTP host is configured as well.

    Kept deliberately simple: there is no mail client in this repo, and
    adding a dependency for a notifier would make the notifier the riskiest
    thing in the process. With NOTIFY_EMAIL_TO set and no SMTP host, the
    address is logged as unreachable rather than silently dropped.
    """
    import os
    import smtplib
    from email.message import EmailMessage

    host = (os.environ.get("NOTIFY_SMTP_HOST") or "").strip()
    if not host:
        logger.warning("notify: NOTIFY_EMAIL_TO is set but NOTIFY_SMTP_HOST "
                       "is not, so no email was sent")
        return False
    try:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = (os.environ.get("NOTIFY_EMAIL_FROM")
                           or to_address)
        message["To"] = to_address
        message.set_content(text)
        port = smtp_port()
        user = os.environ.get("NOTIFY_SMTP_USER") or ""
        password = os.environ.get("NOTIFY_SMTP_PASSWORD") or ""
        if port in IMPLICIT_TLS_PORTS:
            opener, upgrade = smtplib.SMTP_SSL, False
        else:
            opener, upgrade = smtplib.SMTP, True
        with opener(host, port, timeout=TIMEOUT_SECONDS) as smtp:
            if upgrade:
                smtp.starttls()
            if user and password:
                smtp.login(user, password)
            smtp.send_message(message)
        return True
    except Exception as e:
        logger.warning("notify: email send failed: %s: %s",
                       type(e).__name__, e)
        return False


def safe_notify(subject, body, kind=KIND_ALERT, **context) -> dict:
    """notify(), with even a programming error in here caught.

    Guards call THIS. notify() already swallows transport failures; this
    swallows the rest, so no call site has to wrap it.
    """
    try:
        return notify(subject, body, kind=kind, **context)
    except Exception as e:
        logger.error("notify() itself failed: %s: %s", type(e).__name__, e)
        return {"configured": False, "sent": [], "failed": ["notify"]}
