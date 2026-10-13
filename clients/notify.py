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


def notify(subject, body, **context) -> dict:
    """Tell a human. Returns what happened; never raises.

    `context` is appended as `key: value` lines — run_log id, path, event,
    counts. Keep it to ids, counts and masked addresses.
    """
    global _warned_unconfigured

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
    lines = [f"*{subject}*", "", str(body or "").strip()]
    if context:
        lines.append("")
        for key in sorted(context):
            value = context[key]
            if value is None or value == [] or value == {}:
                continue
            lines.append(f"• {key}: {value}")
    lines.append("")
    lines.append("— Jidhr")
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
        port = int(os.environ.get("NOTIFY_SMTP_PORT") or 587)
        user = os.environ.get("NOTIFY_SMTP_USER") or ""
        password = os.environ.get("NOTIFY_SMTP_PASSWORD") or ""
        with smtplib.SMTP(host, port, timeout=TIMEOUT_SECONDS) as smtp:
            smtp.starttls()
            if user and password:
                smtp.login(user, password)
            smtp.send_message(message)
        return True
    except Exception as e:
        logger.warning("notify: email send failed: %s: %s",
                       type(e).__name__, e)
        return False


def safe_notify(subject, body, **context) -> dict:
    """notify(), with even a programming error in here caught.

    Guards call THIS. notify() already swallows transport failures; this
    swallows the rest, so no call site has to wrap it.
    """
    try:
        return notify(subject, body, **context)
    except Exception as e:
        logger.error("notify() itself failed: %s: %s", type(e).__name__, e)
        return {"configured": False, "sent": [], "failed": ["notify"]}
