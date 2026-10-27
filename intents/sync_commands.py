"""
Jidhr Sync Commands
===================
Handles sync operations: donations, events, newsletter, and sync-all.

Chat-surface trigger for the sync/ package: matches the sync phrases,
invokes the requested sync, and formats the result for the user.
"""

import logging
from sync import (DonationSyncDisabled, NewsletterSyncDisabled,
                  run_donation_sync, run_newsletter_sync)
from sync import event_apply

logger = logging.getLogger(__name__)

from intents import anchors


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# Nothing is donor-facing yet; every handler is staff-and-above.
ALLOWED_ROLES = frozenset({"admin", "staff"})

# ---------------------------------------------------------------------------
# Trigger phrases (exact substring matches)
# ---------------------------------------------------------------------------

DONATION_SYNC_PHRASES = ['sync donations', 'sync donation', 'update donations']
EVENT_SYNC_PHRASES = ['sync events', 'update events']  # "sync event [name]" → events.py attendee sync
NEWSLETTER_SYNC_PHRASES = ['sync newsletter', 'sync newsletters', 'update newsletter', 'sync subscriptions']
# Deliberately NOT in ALL_SYNC_PHRASES: "sync all" must not reach the
# registrations sync. Phase 1 writes nothing, but the day it does, a
# three-word message should not be what starts it.
REGISTRATION_SYNC_PHRASES = ['sync registrations', 'sync registration']

# Repairing registration_map rows for writes HubSpot already accepted. A
# separate verb from "sync" on purpose: it sends nothing to HubSpot, and it
# must not be reachable by anything that means "sync everything".
REGISTRATION_RECONCILE_PHRASES = ['reconcile registrations',
                                  'reconcile registration']

# Reporting a background apply. "status run N" is here because that is what
# the lost-response message tells people to say.
REGISTRATION_STATUS_PHRASES = ['registrations status', 'registration status',
                               'status run']

# Creating HubSpot contacts for registrants who have none. A separate verb
# from "sync" because it writes to a different object and registers nobody.
CONTACT_CREATE_PHRASES = ['create contacts', 'create contact']
CONTACT_STATUS_PHRASES = ['contacts status', 'contact status']

# Proving the notifier arrives. Staff/admin like everything in this module
# (ALLOWED_ROLES above) — it sends a message to a channel, so it is not a
# thing for a donor-facing caller to be able to trigger.
NOTIFY_TEST_PHRASES = ['test notify', 'test notification',
                       'test notifications']

# Running the scheduled chain by hand, so the 06:15 behaviour can be
# checked without waiting for 06:15. It executes the HANDLER, not a
# reimplementation of it, so what it proves is what the cron will do.
SCHEDULED_RUN_PHRASES = ['run scheduled sync now', 'run scheduled sync',
                         'run the scheduled sync']

# Tying DAF submissions to follow-up work done before tasks carried a
# submission id. Writes ONLY write_audit, never CSuite or HubSpot.
DAF_RECONCILE_PHRASES = ['reconcile daf tasks', 'reconcile daf task']
# The same reconcile for any Config.INQUIRY_FORMS row: "reconcile inquiries
# <key> [dry run]". "reconcile daf tasks" is an exact alias for the daf row.
INQUIRY_RECONCILE_PHRASES = ['reconcile inquiries', 'reconcile inquiry']
# The one-off past-event backfill (V5.92). Its own verb: it is not a sync,
# and nothing that means "sync everything" may reach it.
EVENT_BACKFILL_PHRASES = ['backfill events', 'backfill event']
EVENT_MAP_RECONCILE_PHRASES = ['reconcile event map']
ALL_SYNC_PHRASES = ['sync all', 'sync everything', 'run all syncs']


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, **kwargs) -> bool:
    """Check if query is a sync command."""
    # Anchored, not scanned: the trigger must LEAD the message.
    # See intents/anchors.py for the two production failures.
    q = query.lower().strip()
    if anchors.yields_to_content(query):
        return False
    return (
        anchors.anchored(q, DONATION_SYNC_PHRASES) or
        anchors.anchored(q, EVENT_SYNC_PHRASES) or
        anchors.anchored(q, NEWSLETTER_SYNC_PHRASES) or
        anchors.anchored(q, REGISTRATION_SYNC_PHRASES) or
        anchors.anchored(q, REGISTRATION_RECONCILE_PHRASES) or
        anchors.anchored(q, REGISTRATION_STATUS_PHRASES) or
        anchors.anchored(q, CONTACT_CREATE_PHRASES) or
        anchors.anchored(q, CONTACT_STATUS_PHRASES) or
        anchors.anchored(q, NOTIFY_TEST_PHRASES) or
        anchors.anchored(q, SCHEDULED_RUN_PHRASES) or
        anchors.anchored(q, DAF_RECONCILE_PHRASES) or
        anchors.anchored(q, INQUIRY_RECONCILE_PHRASES) or
        anchors.anchored(q, EVENT_BACKFILL_PHRASES) or
        anchors.anchored(q, EVENT_MAP_RECONCILE_PHRASES) or
        q in ALL_SYNC_PHRASES
    )


def handle(query: str, ctx) -> str:
    """
    Route to the appropriate sync operation.

    Args:
        query: The user's message
        ctx: RequestContext (not used directly, but keeps the interface
             consistent across all intent modules)

    Returns:
        Formatted result string
    """
    q = query.lower().strip()

    if any(p in q for p in DAF_RECONCILE_PHRASES):
        return _reconcile_daf_tasks(q)

    if any(p in q for p in INQUIRY_RECONCILE_PHRASES):
        return _reconcile_inquiries(q)

    if any(p in q for p in EVENT_BACKFILL_PHRASES):
        return _backfill_events(q)

    if any(p in q for p in EVENT_MAP_RECONCILE_PHRASES):
        return _reconcile_event_map(q)

    if any(p in q for p in SCHEDULED_RUN_PHRASES):
        return _run_scheduled_now()

    if any(p in q for p in NOTIFY_TEST_PHRASES):
        return _test_notify()

    if any(p in q for p in CONTACT_STATUS_PHRASES):
        return _contact_status(q)

    if any(p in q for p in CONTACT_CREATE_PHRASES):
        return _create_contacts(q)

    if any(p in q for p in REGISTRATION_STATUS_PHRASES):
        return _registration_status(q)

    # Before the registrations sync: "reconcile registrations" contains
    # none of the sync phrases, but the ordering should not depend on that.
    if any(p in q for p in REGISTRATION_RECONCILE_PHRASES):
        return _reconcile_registrations(q)

    # Before the event phrases: "sync registrations" contains neither, but
    # keeping it first makes the ordering independent of that.
    if any(p in q for p in REGISTRATION_SYNC_PHRASES):
        return _sync_registrations(q)

    if any(p in q for p in DONATION_SYNC_PHRASES):
        return _sync_donations(q)

    if any(p in q for p in EVENT_SYNC_PHRASES):
        return _sync_events(q)

    if any(p in q for p in NEWSLETTER_SYNC_PHRASES):
        return _sync_newsletter(q)

    if q in ALL_SYNC_PHRASES:
        return _run_all_syncs()

    return "❌ Unrecognised sync command."


# ---------------------------------------------------------------------------
# Individual sync handlers
# ---------------------------------------------------------------------------

def _sync_donations(query_lower: str) -> str:
    """Run donation sync (CSuite → HubSpot).

    A plain "sync donations" WRITES, and is gated on
    CSUITE_DONATION_SYNC_ENABLED. "sync donations dry run" is not gated — it
    makes no HubSpot writes, and it is what the decision to enable should rest
    on. The gate itself lives in DonationSync.sync, because "sync all" reaches
    the same code without coming through here.
    """
    dry_run = 'dry run' in query_lower or 'test' in query_lower
    # A preview was always a 500-profile sample, because quick was wired to
    # dry_run. "full" unwires them: it pages every profile and every donation
    # and writes nothing, which is the only way to see the real counts before
    # turning the flag on.
    full = dry_run and 'full' in query_lower
    if full:
        # Not run here. A full preview reads ~18,800 profiles and ~26,600
        # donations, then searches HubSpot once per donor with an email —
        # 7,604 of them. HubSpot caps the Search API at 5 requests per second
        # across all object types and this paces at 4 to stay clear of it, so
        # the HubSpot half alone takes ~32 minutes (7,604 / 4 = 1,901s). Even
        # pacing right at the cap it would be ~25. gunicorn kills a request at
        # 180s, so the worker dies about a tenth of the way in, taking its
        # other in-flight requests with it, and the work is thrown away.
        logger.info("full donation preview requested; handing over to the CLI")
        return DONATION_FULL_PREVIEW_REPLY
    logger.info("Running donation sync (dry_run=%s, full=%s)...",
                dry_run, full)

    try:
        results = run_donation_sync(dry_run=dry_run,
                                    quick=dry_run and not full,
                                    resolve_shown=LINK_ROWS_SHOWN
                                    if dry_run else 0)
        return _format_donation_sync_results(results, dry_run)
    except DonationSyncDisabled as e:
        # Not an error line. Nothing failed and nothing was attempted, and a
        # ❌ would send someone looking for a fault that is not there.
        logger.info("donation sync is off: %s", e)
        return ("⏸️ **Donation sync is turned off.**\n\n"
                "No HubSpot contact was read or changed. It writes five "
                "properties per matched contact, including "
                "`csuite_profile_id`, which the DAF duplicate guard reads — "
                "so it stays off until that is scoped.\n\n"
                "• Say *\"sync donations dry run\"* to preview it safely "
                "(500 profiles, 500 donations, no writes).\n"
                "• Set `CSUITE_DONATION_SYNC_ENABLED=true` to run it for "
                "real.")
    except Exception as e:
        logger.error(f"Donation sync error: {e}")
        return f"❌ Donation sync failed: {e}"


# The words that make an event sync WRITE. Everything else previews.
#
# It used to be the other way round: "sync events" wrote, and only "dry run"
# or "test" in the message held it back. A one-line chat message is a thin
# thing to hang eleven marketing events on, and the dry run is free.
EVENT_APPLY_PHRASE = "apply"


def _event_limit(query_lower: str):
    """The record cap for a live run: "limit N", or "no limit", or the
    default. Updates count toward it as well as creates."""
    import re

    if "no limit" in query_lower or "unlimited" in query_lower:
        return None
    found = re.search(r"limit\s+(\d+)", query_lower)
    if found:
        return max(int(found.group(1)), 0)
    return event_apply.CHAT_DEFAULT_LIMIT


def _event_options(query_lower: str) -> dict:
    """The brakes a chat message can ask for.

    Defaults are the cautious ones: creates are future-only, and anything
    plan() flagged for review is withheld whatever the message says. "no
    limit" lifts the record cap, never the withholding.
    """
    import re

    return {
        "updates_only": "updates only" in query_lower,
        # Deliberately NOT exposed in chat: future_only. A chat message is
        # one line, and "create 74 marketing events for things that already
        # happened" is not a decision one line should be able to make. The
        # CLI has --past-creates for that.
        # Past creates need their id naming, one at a time. On 2026-10-06
        # that was 74 of 77 creates, 73 of them archived in CSuite.
        "include_ids": re.findall(r"include\s+(\d+)", query_lower),
    }


# Writing needs the word. Everything else previews — the opposite of the
# event sync's original default, and for the same reason: a one-line chat
# message is a thin thing to hang a write on.
REGISTRATION_APPLY_PHRASE = "apply"

# A live run always has a cap. There is no phrase for "all of them".
REGISTRATION_DEFAULT_LIMIT = 1


def _registration_limit(query_lower: str):
    """The record cap for a live run. "no limit" is deliberately NOT
    honoured: it returns None, which the sync refuses."""
    import re

    if "no limit" in query_lower or "unlimited" in query_lower:
        return None
    found = re.search(r"limit\s+(\d+)", query_lower)
    if found:
        return max(int(found.group(1)), 0)
    return REGISTRATION_DEFAULT_LIMIT


def _non_marketing_shown() -> int:
    from sync.contact_create import NON_MARKETING_SHOWN
    return NON_MARKETING_SHOWN


def _new_contact_marketing_note() -> str:
    from sync.contact_create import NEW_CONTACT_MARKETING_NOTE
    return NEW_CONTACT_MARKETING_NOTE


def backfill_options(query_lower: str) -> dict:
    """The chat form of scripts/event_backfill.py's flags.

    "backfill events since 2026-01-01 include archived assume eastern apply
    limit 10 exclude csuite-1161, csuite-1264". Both flags also accept their
    --dashed spelling. Without "since <date>" there is no run.
    """
    import re

    from sync.event_backfill import DEFAULT_LIMIT

    since = re.search(r"\bsince\s+(\d{4}-\d{2}-\d{2})", query_lower)
    limit = re.search(r"\blimit\s+(\d+)", query_lower)
    exclude = re.search(r"(?:--)?\bexclude\s+((?:csuite-)?\d+"
                        r"(?:\s*,\s*(?:csuite-)?\d+)*)", query_lower)
    return {
        "since": since.group(1) if since else None,
        "include_archived": bool(re.search(r"include[\s-]+archived",
                                           query_lower)),
        "assume_eastern": bool(re.search(r"assume[\s-]+eastern",
                                         query_lower)),
        "apply": bool(re.search(r"\bapply\b", query_lower)),
        "limit": int(limit.group(1)) if limit else DEFAULT_LIMIT,
        "exclude": exclude.group(1) if exclude else "",
    }


def _backfill_events(query_lower: str) -> str:
    """Preview, or create, past CSuite events in HubSpot. Create-only."""
    from sync import event_backfill as backfill

    options = backfill_options(query_lower)
    if not options["since"]:
        return ('❓ From when? Say *"backfill events since 2026-01-01"* '
                "(add *include archived* and/or *assume eastern* to release "
                "those events, and *apply* to create them). Nothing was "
                "read.")
    try:
        out = backfill.run(**options)
    except backfill.Refused as e:
        return f"🛑 **Event backfill did not run.**\n\n{e}"
    except Exception as e:
        logger.error("event backfill failed: %s", e, exc_info=True)
        return f"❌ Event backfill failed: {e}"
    return backfill.report(out)


def _reconcile_event_map(query_lower: str) -> str:
    """Preview, or with "apply" write, the pending event_map rows that
    HubSpot already has. Map-only (sync/event_map_reconcile.py)."""
    import re

    from sync import event_map_reconcile as recon

    try:
        out = recon.run(apply=bool(re.search(r"\bapply\b", query_lower)))
    except recon.Refused as e:
        return f"🛑 **Event map reconcile did not run.**\n\n{e}"
    except Exception as e:
        logger.error("event map reconcile failed: %s", type(e).__name__,
                     exc_info=True)
        return f"❌ Event map reconcile failed: {type(e).__name__}"
    return recon.report(out)


def _reconcile_daf_tasks(query_lower: str) -> str:
    """Tie DAF submissions to work already done. Dry run when asked.

    "reconcile daf tasks dry run" reads and proposes. "reconcile daf tasks"
    records the links. Either way NOTHING goes to CSuite or HubSpot — the
    live run writes write_audit rows only, and the links are what make a
    handled returning donor stop being offered again.

    An exact alias for "reconcile inquiries daf": same run, same reply.
    """
    return _run_inquiry_reconcile("daf", "dry run" in query_lower,
                                  "reconcile daf tasks")


def _reconcile_inquiries(query_lower: str) -> str:
    """"reconcile inquiries <key> [dry run]" for any Config.INQUIRY_FORMS row."""
    from config import Config

    rest = query_lower
    for phrase in INQUIRY_RECONCILE_PHRASES:
        if phrase in rest:
            rest = rest.split(phrase, 1)[1]
            break
    words = rest.replace("dry run", " ").split()
    keys = [row["key"] for row in Config.INQUIRY_FORMS]
    if not words or words[0] not in keys:
        return ("❓ Which form? Say *\"reconcile inquiries <form> dry "
                "run\"*, where <form> is one of: "
                + ", ".join(f"`{k}`" for k in keys) + ". Nothing was read.")
    key = words[0]
    return _run_inquiry_reconcile(key, "dry run" in query_lower,
                                  f"reconcile inquiries {key}")


def _inquiry_switch_refusal(row):
    """The reply when this form's own create switch is off, else None.

    The reconcile writes nothing to CSuite or HubSpot, but its dry run reads
    both and its live run writes write_audit link rows. A form that is
    switched off is off for this too: endowment's kill switch must not be
    walked round through a different command.
    """
    from intents.daf_workflow import endowment_refusal, form_switch

    if row["key"] == "endowment":
        return endowment_refusal()
    if not form_switch(row, "create_flag"):
        return (f"⏸️ {row['label']} inquiries are turned off "
                f"({row['create_flag']}), so nothing was read and nothing "
                "was linked.")
    return None


def _run_inquiry_reconcile(key: str, dry_run: bool, command: str) -> str:
    from intents.daf_workflow import handled_endpoints, inquiry_form
    from sync import daf_reconcile as dr

    row = inquiry_form(key)
    label = row["label"]
    refusal = _inquiry_switch_refusal(row)
    if refusal:
        return refusal
    try:
        out = dr.run(dry_run=dry_run, form_id=row["form_id"])
    except dr.ReconcileRefused as e:
        return f"🛑 **Nothing was linked.**\n\n{e}"
    except Exception as e:
        logger.error("%s reconcile failed: %s", label, e, exc_info=True)
        return f"❌ {label} reconcile failed: {e}"

    after = None
    if not dry_run and out.get("written"):
        # Read back, not added up: the acceptance is that the processed
        # count rises by exactly the number linked, and that is a claim
        # about what processed_submissions now SEES.
        try:
            from clients.audit import processed_submissions

            endpoints = handled_endpoints(row)
            seen = (processed_submissions(out["form_id"]) if endpoints is None
                    else processed_submissions(out["form_id"],
                                               endpoints=endpoints))
            after = sum(1 for sid in out.get("fetched_ids") or []
                        if sid in seen)
        except Exception as e:
            logger.warning("could not re-read processed history: %s", e)
    return _format_daf_reconcile(out, after, label=label, command=command)


def _format_daf_reconcile(out: dict, after=None, label: str = "DAF",
                          command: str = "reconcile daf tasks") -> str:
    from sync import daf_reconcile as dr
    from sync.contact_create import mask_email

    dry_run = out.get("dry_run", True)
    title = ("Reconcile DAF tasks" if command == "reconcile daf tasks"
             else f"Reconcile {label} inquiries")
    head = (f"🧪 **{title} — PREVIEW** (nothing was written)"
            if dry_run else f"✅ **{title} — LINKED**")
    links = out.get("links") or []
    lines = [head, "",
             f"📊 **{out.get('fetched', 0)}** newest {label} submissions read "
             f"(page 1 only):",
             f"• **{out.get('processed_before', 0)}** already handled",
             f"• **{out.get('unprocessed', 0)}** not handled",
             f"• **{len(links)}** of those tie to work already done"]

    if links:
        lines += ["", ("➡️ **Would link:**" if dry_run else "✍️ **Linked:**"),
                  "", "| submission | submitted | reason | profile | task | "
                      "via |", "|---|---|---|---|---|---|"]
        for link in links:
            lines.append(
                f"| `{link['submission_id'][:8]}` | "
                f"{_daf_date(link.get('submitted_at'))} | "
                f"{link['reason']} | `{link.get('profile') or '—'}` | "
                f"`{link.get('task') or '—'}` | "
                f"{('`' + link['linked_from'][:8] + '`') if link.get('linked_from') else '—'} |")

    unlinked = out.get("unlinked") or []
    if unlinked:
        from collections import Counter

        lines += ["", f"📋 **{len(unlinked)} still unhandled** — NOT linked, "
                      f"a person decides:"]
        for why, n in Counter(u["why"].split(":")[0]
                              for u in unlinked).most_common():
            lines.append(f"• **{n}** — {why}")
        # Munira works the DAF list. Another form's list is not hers.
        heading = ("**The list for Munira**" if label == "DAF"
                   else f"**The {label} list**")
        lines += ["", f"{heading} (newest first; masked):", "",
                  "| submitted | address | why |", "|---|---|---|"]
        for u in unlinked:
            lines.append(f"| {_daf_date(u.get('submitted_at'))} | "
                         f"`{mask_email(u.get('email'))}` | "
                         f"{u['why'].split(':')[0]} |")
        lines.append("")
        lines.append("Not sent anywhere — copy it from here if it is wanted.")

    lines += ["", f"📞 {out.get('csuite_reads', 0)} CSuite read(s), 1 HubSpot "
                  f"read", "✍️ **0 CSuite writes, 0 HubSpot writes.**"]
    if dry_run:
        lines.append(f'Say *"{command}"* to record these links — '
                     'write_audit only. Run '
                     '`migrations/007_write_audit_jidhr_target.sql` first.')
    else:
        lines.append(f"📒 **{len(out.get('written') or [])}** link row(s) "
                     f"written to `write_audit`, and nothing else.")
        if after is not None:
            before = out.get("processed_before", 0)
            lines.append(f"Handled among the newest {out.get('fetched', 0)}: "
                         f"**{before} → {after}** (read back from the "
                         f"processed history, not added up).")
    return "\n".join(lines)


def _daf_date(value) -> str:
    from intents.daf_workflow import _submitted_moment

    moment = _submitted_moment(value)
    return f"{moment:%Y-%m-%d}" if moment else "—"


def _run_scheduled_now() -> str:
    """Run the scheduled chain by hand, through the real handler.

    Calls jobs.handlers.registrations_sync.handle, not a copy of it, so
    what this proves is what the 06:15 cron will do — including the email.
    Honours the current flags: with MODE=preview this is a dry run end to
    end and writes nothing.
    """
    from jobs.handlers import registrations_sync as handler
    from jobs.runner import JobFailed
    from sync import scheduled

    if not scheduled.scheduled_enabled():
        return ("⏸️ **The scheduled sync is disarmed.**\n\n"
                "`SCHEDULED_SYNC_ENABLED` is not `true`, so the 06:15 cron "
                "logs \"disabled\" and exits — no CSuite call, no HubSpot "
                "call, no email. That is the shipped default.\n\n"
                "• Set `SCHEDULED_SYNC_ENABLED=true` to arm it.\n"
                "• `SCHEDULED_SYNC_MODE` is currently **"
                f"{scheduled.scheduled_mode()}** — only the exact word "
                "`live` writes anything, and a live run ALSO needs "
                "`REGISTRATIONS_SYNC_ENABLED=true`.")

    mode = scheduled.scheduled_mode()
    logger.info("running the scheduled sync chain by hand (mode=%s)", mode)
    try:
        out = handler.handle({"payload": {}})
    except JobFailed as failed:
        out = failed.result or {}
        out.setdefault("stopped", str(failed))
    except Exception as e:
        logger.error("the scheduled chain could not be run: %s", e,
                     exc_info=True)
        return f"❌ Could not run the scheduled sync: {e}"

    return _format_scheduled_results(out, by_hand=True)


def _format_scheduled_results(out: dict, by_hand=False) -> str:
    from clients import notify as notifier
    from sync import scheduled

    mode = out.get("mode") or scheduled.scheduled_mode()
    stopped = out.get("stopped")
    if stopped:
        head = f"🛑 **Scheduled sync ({mode}) — STOPPED**"
    elif mode == scheduled.MODE_LIVE:
        head = "✅ **Scheduled sync (live) — DONE**"
    else:
        head = "🧪 **Scheduled sync (preview) — nothing was written**"

    lines = [head, ""]
    if by_hand:
        lines += ["Run by hand through the real job handler, so this is "
                  "exactly what 06:15 will do.", ""]
    lines.append("```")
    lines.extend(scheduled.summary_lines(out))
    lines.append("```")

    sent = ", ".join(out.get("notified") or []) or "nothing"
    # From the constants, not a literal: a prefix spelled out here could
    # drift from the one actually sent, and the reply would then tell Carl
    # to filter for something that never arrives.
    prefix = notifier.ALERT_PREFIX if stopped else notifier.OK_PREFIX
    lines += ["", f"📣 One email sent via **{sent}** with the **{prefix}** "
                  f"prefix. Exactly one goes out per enabled run — OK on "
                  f"success, ALERT on a stop, never both and never "
                  f"neither."]
    if out.get("notify_failed"):
        lines.append(f"⚠️ Delivery failed via "
                     f"**{', '.join(out['notify_failed'])}** — the run is "
                     f"unaffected, but nobody was told.")
    return "\n".join(lines)


def _notify_settings_table() -> str:
    """Which notifier variables are set, and what they resolve to.

    Printed by "test notify" so the variable NAMES never have to be read
    out of the source, and so a test email that does not arrive can be
    diagnosed from chat. The password is reported as set or unset and is
    never shown.
    """
    from clients import notify as notifier

    lines = ["**Notifier settings** (the password is never shown):", "",
             "| variable | value |", "|---|---|"]
    for name, _is_set, shown in notifier.settings():
        lines.append(f"| `{name}` | {shown} |")
    port = notifier.smtp_port()
    mode = ("implicit TLS (`SMTP_SSL`)" if port in notifier.IMPLICIT_TLS_PORTS
            else "STARTTLS (`SMTP` then `starttls`)")
    lines.append("")
    lines.append(f"Port **{port}** uses {mode}. Both 587 and 465 work; they "
                 f"need different client calls, which is chosen by the port "
                 f"rather than assumed.")
    return "\n".join(lines)


def _test_notify() -> str:
    """Send one notification, and say exactly what happened.

    A notifier nobody has seen arrive is a notifier nobody should rely on.
    This is the only way to check it without breaking something on purpose.
    """
    from clients import notify as notifier

    if not notifier.configured():
        return ("⏸️ **Notifications are off.**\n\n"
                "Neither `NOTIFY_SLACK_WEBHOOK_URL` nor `NOTIFY_EMAIL_TO` "
                "is set, so a stopped run would be recorded in "
                "`hubsync.run_log` and told to nobody.\n\n"
                "• For email: `NOTIFY_EMAIL_TO` **and** "
                "`NOTIFY_SMTP_HOST`.\n"
                "• For Slack: `NOTIFY_SLACK_WEBHOOK_URL`.\n\n"
                + _notify_settings_table() + "\n\n"
                "Guards still refuse and still stop runs without it — they "
                "just cannot tell you.")

    result = notifier.safe_notify(
        "test notification",
        "This is a test, sent from the Jidhr chat. Nothing is wrong and "
        "nothing was written. If you can read this, a stopped sync can "
        "reach you.",
        kind=notifier.KIND_TEST,
        path="test notify",
        run_log="n/a",
    )
    sent = ", ".join(result.get("sent") or []) or "nothing"
    failed = ", ".join(result.get("failed") or [])
    lines = [f"📣 **Test notification sent via: {sent}.**", "",
             f"Look for the subject **`{notifier.TEST_PREFIX} test "
             f"notification`**. Failure alerts use "
             f"**`{notifier.ALERT_PREFIX}`** and the daily heartbeat uses "
             f"**`{notifier.OK_PREFIX}`**, so a filter on "
             f"**`subject:Jidhr`** catches all three.", "",
             "(Not `subject:([Jidhr )` — Gmail ignores the brackets, and "
             "the unmatched one breaks the query.)", ""]
    if failed:
        lines += [f"⚠️ Failed: **{failed}** — see the logs for the reason. "
                  f"A failed notification can never fail a run, so this is "
                  f"the only place it shows."]
    if result.get("sent"):
        lines.append("If it did NOT arrive, check spam first, then the "
                     "settings below — the send itself reported success, "
                     "which is all the code can ever know.")
    lines.append("")
    lines.append(_notify_settings_table())
    return "\n".join(lines)


def _create_contacts(query_lower: str) -> str:
    """Preview, or start, contact creation for withheld registrants."""
    from sync import contact_create as cc
    from sync import contact_jobs as jobs
    from sync import registrations as reg

    live = REGISTRATION_APPLY_PHRASE in query_lower
    limit = _registration_limit(query_lower) if live else None
    asked = _registration_event(query_lower)
    scope = {}
    if asked:
        try:
            # The CONTACTS resolver, not the registrations one: an id in
            # CONTACTS_ONLY_EVENT_IDS is held for registrations and allowed
            # here, because a contact existing is the whole point of it.
            scope["event_ids"] = (cc.resolve_requested_event(asked),)
        except reg.EventRefused as e:
            return ("🛑 **That event cannot be synced.**\n\n"
                    f"{e}\n\n"
                    '• Say *"create contacts dry run"* to preview the whole '
                    "mapped scope.")

    if live:
        try:
            run_id = jobs.start_apply(limit=limit, **scope)
        except reg.RegistrationsSyncDisabled as e:
            return ("⏸️ **Registrations sync is turned off.**\n\n"
                    f"{e}")
        except cc.LimitRequired as e:
            return f"🛑 **No limit, no run.**\n\n{e}"
        except jobs.ApplyAlreadyRunning as e:
            return f"⏳ **One run at a time.**\n\n{e}"
        except cc.ContactCreateStopped as e:
            return f"🛑 **Nothing was started.**\n\n{e}"
        except Exception as e:
            logger.error("could not start the contact create: %s", e,
                         exc_info=True)
            return f"❌ Could not start contact creation: {e}"
        scoped = f" for event `{asked}`" if asked else ""
        return (f"🚀 **Contact creation started**{scoped} — "
                f"`run_log {run_id}`, limit **{limit}**.\n\n"
                f"It runs in the background. Each contact is a HubSpot read "
                f"then a write, so a long run takes minutes and the HTTP "
                f"response would be discarded before it finished.\n\n"
                f'• Say *"contacts status"* for progress and the full '
                f"report.\n"
                f"• **Do not send it again** — one at a time is enforced, "
                f"and a resend is how one person becomes two contacts.\n"
                f"• Nobody is registered by this. Run *\"sync registrations "
                f"dry run\"* afterwards to see them queued.")

    try:
        results = cc.run(dry_run=True, limit=None, **scope)
        if asked:
            results["scoped_event"] = asked
        return _format_contact_results(results)
    except Exception as e:
        logger.error("Contact create preview failed: %s", e, exc_info=True)
        return f"❌ Contact create preview failed: {e}"


def _contact_status(query_lower: str) -> str:
    from sync import contact_jobs as jobs

    run_id = _registration_run_id(query_lower)
    try:
        state = jobs.status(run_id)
    except jobs.RunNotFound as e:
        return f"🤷 **No such run.**\n\n{e}"
    except Exception as e:
        logger.error("could not read the contact-create status: %s", e,
                     exc_info=True)
        return f"❌ Could not read the run status: {e}"

    if not state.get("found"):
        return ("🤷 **No contact creation has been run yet.**\n\n"
                '• Say *"create contacts dry run"* to preview one.')

    run = state["run_id"]
    seconds = int(state.get("seconds") or 0)
    started = state.get("started_at")
    when = f"{started:%Y-%m-%d %H:%M:%S} UTC" if started else "unknown"

    if state["status"] == "running":
        writes = state.get("writes_so_far")
        wrote = ("unknown — write_audit could not be read"
                 if writes is None else f"**{writes}** so far")
        return (f"⏳ **Contact creation `run_log {run}` is still "
                f"running.**\n\n"
                f"• Started {when}, **{seconds}s** ago\n"
                f"• HubSpot contact creates attempted: {wrote}\n\n"
                f'• Say *"contacts status"* again in a minute.\n'
                f"• **Do not start another run** — it would be refused.")

    head = (f"📒 **Contact creation `run_log {run}` finished** "
            f"({state['status']}) — started {when}, took **{seconds}s**.")
    summary = state.get("summary")
    if not isinstance(summary, dict):
        return (f"{head}\n\n• **{state.get('record_count', 0)}** "
                f"per-record row(s) in `run_log.outcomes`"
                + (f"\n• Error: {state['error_summary']}"
                   if state.get("error_summary") else ""))
    return f"{head}\n\n" + _format_contact_results(summary)


def _format_contact_results(results: dict) -> str:
    """Counts, and masked addresses only — never a full address in chat."""
    if results.get("error"):
        return f"❌ **Contact creation stopped.**\n\n{results['error']}"

    dry_run = results.get("dry_run", True)
    if dry_run:
        head = "🧪 **Create contacts — PREVIEW** (nothing was written)"
    elif results.get("stopped") or results.get("failed"):
        head = "🛑 **Create contacts — STOPPED**"
    else:
        head = "✅ **Create contacts — APPLIED**"

    lines = [head, ""]
    if results.get("scoped_event"):
        lines.append(f"🎯 Scoped to event `{results['scoped_event']}` only.")
    # Every row accounted for, in one chain that adds up on its face:
    #   rows = excluded + duplicates + unique
    #   unique = already + to create
    # "14 rows, 13 after dedupe, 0 duplicates dropped" was the version that
    # did not, and the missing row was one with no email address.
    excluded = results.get("excluded", 0)
    reasons = results.get("excluded_reasons") or {}
    reason_text = (" — " + ", ".join(f"{n} {why}"
                                     for why, n in sorted(reasons.items()))
                   if reasons else "")
    lines += [
        f"📊 **Across {results.get('events_read', 0)} event(s):**",
        f"• **{results.get('registrant_rows', 0)}** registrant row(s) in "
        f"CSuite",
        f"   = **{excluded}** excluded{reason_text}",
        f"   + **{results.get('duplicates_dropped', 0)}** duplicate(s) "
        f"dropped (same address twice on one event)",
        f"   + **{results.get('unique_emails', 0)}** unique address(es)",
        f"• of those **{results.get('unique_emails', 0)}**:",
        f"   • **{results.get('already_contacts', 0)}** already have a "
        f"HubSpot contact",
        f"   • **{results.get('to_create', 0)}** have none and would get "
        f"one — **{results.get('with_name', 0)}** of those have a first or "
        f"last name in CSuite, **{results.get('with_company', 0)}** an "
        f"organisation"]

    if results.get("contacts_only_events"):
        lines.append(
            "• contacts-only (a sign-up list, not an event — a contact is "
            "created and NOTHING is registered): "
            + ", ".join(f"`{e}`"
                        for e in results["contacts_only_events"]))

    if not results.get("reconciles", True):
        lines += ["", "⚠️ **These numbers do not add up, so do not trust "
                      "them.** The breakdown is meant to be checkable on "
                      "its face; this one is not:"]
        for problem in results.get("reconcile_problems") or []:
            lines.append(f"   • {problem}")
        lines.append("   Nothing was written. This is a bug in the count, "
                     "not a reason to apply anyway.")

    already = results.get("already_contacts", 0)
    if already:
        lines += ["", f"📧 **Of the {already} who already have a contact:** "
                      f"**{results.get('marketing', 0)}** marketing, "
                      f"**{results.get('non_marketing', 0)}** "
                      f"NON-marketing, "
                      f"**{results.get('marketing_unknown', 0)}** unknown.",
                  "   HubSpot's \"All Emails\" segment is \"Email is "
                  "known\", so being in it is NOT the same as being "
                  "emailed — a non-marketing contact is in the segment and "
                  "does not receive the newsletter."]
        rows = results.get("non_marketing_rows") or []
        if rows:
            shown = rows[:_non_marketing_shown()]
            lines += ["", "   **The non-marketing ones**, to find in "
                          "HubSpot:", "",
                      "| event | contact | address (masked) |",
                      "|---|---|---|"]
            for row in shown:
                lines.append(f"| `{row.get('event_date_id')}` | "
                             f"`{row.get('hubspot_contact_id') or '—'}` | "
                             f"`{row.get('email_masked', '***')}` |")
            if len(rows) > len(shown):
                lines.append(f"… and **{len(rows) - len(shown)}** more")

    if results.get("held_events"):
        lines.append(
            f"• held and skipped entirely: "
            + ", ".join(f"`{e}`" for e in results['held_events'])
            + f" — {reg_held_reason()}")

    if results.get("to_create"):
        lines += ["", f"ℹ️ {_new_contact_marketing_note()}"]

    lines += ["", "🔒 **Properties sent: email, firstname, lastname"
                  "** (plus company where CSuite has one) **and nothing "
                  "else.** No marketing status, no subscription status, no "
                  "list membership — newsletter consent is a separate "
                  "decision, and `hs_marketable_status` is read-only to the "
                  "API in any case.",
              "🔎 Each address is re-checked in HubSpot immediately before "
              "its create, and a 409 is read as \"already a contact\" — so "
              "an existing person is recorded, never duplicated."]

    rows = []
    for event in results.get("events") or []:
        for record in event.get("to_create") or []:
            rows.append(record)
    if dry_run and rows:
        lines += ["", f"➡️ **The {len(rows)} to create**, in send order:", "",
                  "| event | address (masked) | CSuite profile | name? |",
                  "|---|---|---|---|"]
        for record in rows[:REGISTRATION_REVIEW_ROWS_SHOWN * 2]:
            props = record.get("properties") or {}
            named = "yes" if (props.get("firstname")
                              or props.get("lastname")) else "NO"
            lines.append(f"| `{record['event_date_id']}` | "
                         f"`{record.get('email_masked', '***')}` | "
                         f"`{record.get('csuite_profile_id') or '—'}` | "
                         f"{named} |")
        if len(rows) > REGISTRATION_REVIEW_ROWS_SHOWN * 2:
            lines.append(f"… and **{len(rows) - REGISTRATION_REVIEW_ROWS_SHOWN * 2}** more")

    if not dry_run:
        lines += ["", f"• **{results.get('created', 0)}** contact(s) created",
                  f"• **{results.get('existing', 0)}** already existed — "
                  f"recorded, not duplicated"]
        if results.get("deferred"):
            lines.append(f"• **{results['deferred']}** deferred — the limit "
                         f"of {results.get('limit')} was reached")
        if results.get("unknown"):
            lines.append(f"• **{results['unknown']}** ambiguous — may have "
                         f"landed, NOT retried")
        if results.get("failed"):
            lines.append(f"• **{results['failed']}** failed — see below")

    review = results.get("review_rows") or []
    if review:
        lines += ["", "🔎 **Needs a human:**"]
        for event_id, why in review[:REGISTRATION_REVIEW_ROWS_SHOWN]:
            lines.append(f"   `{event_id}` — {str(why)[:110]}")

    lines += ["", f"📞 {results.get('csuite_calls', 0)} CSuite call(s), "
                  f"{results.get('hubspot_calls', 0)} HubSpot read call(s)"]
    if dry_run:
        lines.append("✍️ **0 HubSpot writes — nothing was created.**")
        lines.append('Say *"create contacts apply limit N"* to create them. '
                     "Nobody is registered by that — the next registrations "
                     "sync does the registering.")
    else:
        ids = results.get("write_audit_ids") or []
        trail = (" (write_audit " + ", ".join(str(i) for i in ids) + ")"
                 if ids else "")
        lines.append(f"✍️ **{results.get('writes_attempted', 0)} create(s) "
                     f"attempted**{trail}")
        if results.get("stopped"):
            lines += ["", f"🛑 {results['stopped']}"]
        lines.append('Run *"sync registrations dry run"* to see them queued '
                     "for registration.")
    if results.get("run_logged"):
        # Backlog fix: the contact dry run was the only one of these that
        # did not say where to find its own record.
        lines += ["", f"📒 Recorded as `hubsync.run_log` id "
                      f"{results.get('run_id')} with "
                      f"{results.get('unique_emails', 0)} per-record input "
                      f"row(s) — masked addresses, never addresses."]
    return "\n".join(lines)


def _start_registration_apply(limit, scope, asked) -> str:
    """Start the apply in the background and answer with its run_log id."""
    from sync import registration_jobs as jobs
    from sync import registrations as reg

    try:
        run_id = jobs.start_apply(limit=limit, **scope)
    except reg.RegistrationsSyncDisabled as e:
        return ("⏸️ **Registrations sync is turned off.**\n\n"
                f"{e}\n\n"
                '• Say *"sync registrations dry run"* to preview it.')
    except reg.LimitRequired as e:
        return f"🛑 **No limit, no run.**\n\n{e}"
    except jobs.ApplyAlreadyRunning as e:
        return ("⏳ **One apply at a time.**\n\n"
                f"{e}")
    except reg.RegistrationWriteStopped as e:
        return f"🛑 **Nothing was started.**\n\n{e}"
    except Exception as e:
        logger.error("could not start the registrations apply: %s", e,
                     exc_info=True)
        return f"❌ Could not start the registrations apply: {e}"

    scoped = f" for event `{asked}`" if asked else ""
    return (f"🚀 **Registrations apply started**{scoped} — "
            f"`run_log {run_id}`, limit **{limit}**.\n\n"
            f"It runs in the background, so this reply does not wait for it. "
            f"A ~30-record apply took 318 seconds, and the HTTP response "
            f"would be discarded before then.\n\n"
            f'• Say *"status run {run_id}"* for progress, and again when it '
            f"finishes for the full report.\n"
            f"• **Do not send the apply again** — one at a time is enforced, "
            f"and a resend is how one registration becomes two.")


def _registration_run_id(query_lower: str):
    """The run id in "status run 30", or None for the latest apply."""
    import re

    found = re.search(r"\brun\s+(\d+)", query_lower)
    return int(found.group(1)) if found else None


def _registration_status(query_lower: str) -> str:
    from sync import registration_jobs as jobs

    run_id = _registration_run_id(query_lower)
    try:
        state = jobs.status(run_id)
    except jobs.RunNotFound as e:
        return f"🤷 **No such run.**\n\n{e}"
    except Exception as e:
        logger.error("could not read the registrations run status: %s", e,
                     exc_info=True)
        return f"❌ Could not read the run status: {e}"

    if not state.get("found"):
        return ("🤷 **No registrations apply has been run yet.**\n\n"
                '• Say *"sync registrations dry run"* to preview one.')

    return _format_registration_status(state)


def _format_registration_status(state: dict) -> str:
    run_id = state["run_id"]
    seconds = int(state.get("seconds") or 0)
    started = state.get("started_at")
    when = f"{started:%Y-%m-%d %H:%M:%S} UTC" if started else "unknown"

    if state["status"] == "running":
        writes = state.get("writes_so_far")
        wrote = ("unknown — write_audit could not be read"
                 if writes is None else f"**{writes}** so far")
        return (f"⏳ **Registrations apply `run_log {run_id}` is still "
                f"running.**\n\n"
                f"• Started {when}, **{seconds}s** ago\n"
                f"• HubSpot attendance writes attempted: {wrote}\n\n"
                f"Each write is verified with up to three reads over ~10s, "
                f"so a record takes a few seconds.\n\n"
                f'• Say *"status run {run_id}"* again in a minute.\n'
                f"• **Do not start another apply** — it would be refused, "
                f"and resending is how one registration becomes two.")

    summary = state.get("summary")
    head = (f"📒 **Registrations apply `run_log {run_id}` finished** "
            f"({state['status']}) — started {when}, took **{seconds}s**.")
    if not isinstance(summary, dict):
        # A row written before this hotfix, or a run that died before it
        # could store its report.
        return (f"{head}\n\n"
                f"• **{state.get('record_count', 0)}** per-record row(s) in "
                f"`run_log.outcomes`\n"
                + (f"• Error: {state['error_summary']}\n"
                   if state.get("error_summary") else "")
                + "\nThis run stored no full report — it predates the "
                  "background apply, so read `run_log.outcomes` directly.")
    return f"{head}\n\n" + _format_registration_results(summary)


def _reconcile_registrations(query_lower: str) -> str:
    """Repair registration_map rows for writes HubSpot already accepted.

    Dry run unless "apply" is said. This never writes to HubSpot in either
    mode — the HubSpot write is the thing that already happened.
    """
    from sync import registration_reconcile as rc

    live = REGISTRATION_APPLY_PHRASE in query_lower
    asked = _registration_event(query_lower)
    scope = {"event_ids": (asked,)} if asked else {}
    logger.info("Reconciling registrations (apply=%s, event=%s)...",
                live, asked or "all with a 2xx write")
    try:
        results = rc.run(dry_run=not live, **scope)
        return _format_reconcile_results(results, asked)
    except Exception as e:
        logger.error("Registrations reconcile error: %s", e, exc_info=True)
        return f"❌ Registrations reconcile failed: {e}"


def _format_reconcile_results(results: dict, asked=None) -> str:
    if results.get("error"):
        return f"❌ **Reconcile stopped.**\n\n{results['error']}"

    dry_run = results.get("dry_run", True)
    proposals = results.get("proposals") or []
    head = ("🧪 **Registrations reconcile — PREVIEW** (nothing written)"
            if dry_run else "✅ **Registrations reconcile — APPLIED**")
    lines = [head, ""]
    if asked:
        lines.append(f"🎯 Scoped to event `{asked}` only.")
    lines += [
        f"📊 Scanned **{results.get('events_scanned', 0)}** event(s) with a "
        f"2xx attendance write in `write_audit`:",
        f"• **{results.get('confirmed', 0)}** confirmed by HubSpot as a "
        f"landed registration (REGISTERED, ATTENDED or NO_SHOW) and "
        f"missing a `synced` row",
        f"• **{results.get('already_synced', 0)}** already recorded `synced` "
        f"— nothing to do",
        f"• **{results.get('not_in_hubspot', 0)}** not held by HubSpot, so "
        f"NOT recorded (these would be sent again by the next run, which is "
        f"correct — nothing landed)"]

    if proposals:
        lines += ["", ("➡️ **Would insert/update:**" if dry_run
                       else "✍️ **Written:**"), "",
                  "| event | contact | email (hashed) | was | becomes | "
                  "last_state | write_audit |",
                  "|---|---|---|---|---|---|---|"]
        for p in proposals:
            lines.append(f"| `{p['event_date_id']}` | "
                         f"`{p['hubspot_contact_id']}` | "
                         f"`{p['email_sha1']}` | {p['current_status']} | "
                         f"{p['new_status']} | "
                         f"{p.get('last_state') or '—'} | "
                         f"{p['write_audit_id']} |")
        if dry_run:
            lines += ["", 'Say *"reconcile registrations apply"* to write '
                          "these rows. No HubSpot write is made either way "
                          "— the registration is already there."]
        else:
            lines.append("")
            lines.append(f"📒 **{results.get('rows_written', 0)}** "
                         f"registration_map row(s) written.")
            if results.get("failed_writes"):
                lines.append(f"⚠️ **{results['failed_writes']}** could not "
                             f"be written — see the table.")
    else:
        lines += ["", "Nothing to reconcile."]

    states = results.get("landed_states") or {}
    if states:
        lines += ["", "HubSpot holds these as: " + ", ".join(
            f"**{n}** {state}" for state, n in sorted(states.items()))]
    if results.get("cancelled"):
        lines.append(f"• **{results['cancelled']}** are CANCELLED in "
                     f"HubSpot, so they are NOT recorded as registered.")

    # Every row nobody has confirmed, named whether or not this run could
    # resolve it. A row HubSpot cannot confirm is the one most worth
    # printing, and it used to be invisible.
    unresolved = results.get("unresolved") or []
    if unresolved:
        lines += ["", f"⚠️ **{len(unresolved)}** registration_map row(s) "
                      f"still unresolved:", "",
                  "| event | contact | status | last_state | audit | age |",
                  "|---|---|---|---|---|---|"]
        for row in unresolved[:REGISTRATION_REVIEW_ROWS_SHOWN]:
            age = int(row.get("age_seconds") or 0)
            age_text = (f"{age // 86400}d" if age >= 86400
                        else f"{age // 3600}h" if age >= 3600
                        else f"{age // 60}m")
            lines.append(
                f"| `{row.get('csuite_eventdate_id')}` | "
                f"`{row.get('hubspot_contact_id') or '—'}` | "
                f"{row.get('status')} | {row.get('last_state') or '—'} | "
                f"{row.get('write_audit_id') or '—'} | {age_text} |")
        if len(unresolved) > REGISTRATION_REVIEW_ROWS_SHOWN:
            lines.append(f"… and "
                         f"**{len(unresolved) - REGISTRATION_REVIEW_ROWS_SHOWN}"
                         f"** more")

    skipped = results.get("events_skipped") or []
    if skipped:
        lines += ["", "🔎 **Skipped:**"]
        for event_id, why in skipped[:REGISTRATION_REVIEW_ROWS_SHOWN]:
            lines.append(f"   `{event_id}` — {str(why)[:110]}")

    lines += ["", f"📞 {results.get('csuite_calls', 0)} CSuite call(s), "
                  f"{results.get('hubspot_calls', 0)} HubSpot read call(s)",
              "✍️ **0 HubSpot writes — reconcile never writes to HubSpot.**"]
    return "\n".join(lines)


def reg_held_reason() -> str:
    """The one wording for a held event, from the sync module.

    Imported lazily and not copied: a report that says something different
    from what the sync recorded is a report nobody can reconcile with the
    run log.
    """
    from sync.registrations import HELD_REASON
    return HELD_REASON


def _registration_event(query_lower: str):
    """The one event id asked for, or None for the whole phase-1 scope.

    "sync registrations apply event 1463" narrows the run to that event.
    The limit is unaffected — a scoped run still caps, because an event
    with 41 registrant rows is not a smaller blast radius than eleven
    events with one each.
    """
    import re

    found = re.search(r"\bevent\s+(\d+)", query_lower)
    return found.group(1) if found else None


def _sync_registrations(query_lower: str) -> str:
    """Preview, or apply, the registrations sync."""
    from sync import registrations as reg

    live = REGISTRATION_APPLY_PHRASE in query_lower
    limit = _registration_limit(query_lower) if live else None
    asked = _registration_event(query_lower)
    scope = {}
    if asked:
        try:
            scope["event_ids"] = (reg.resolve_requested_event(asked),)
        except reg.EventRefused as e:
            return ("🛑 **That event cannot be synced.**\n\n"
                    f"{e}\n\n"
                    '• Say *"sync registrations dry run"* to preview the '
                    "whole mapped scope.")
    if live:
        # The run does NOT belong to this HTTP request. run_log 30 took 318
        # seconds and Railway discards a response after 300 — see
        # sync/registration_jobs.py.
        return _start_registration_apply(limit, scope, asked)

    logger.info("Running registrations preview (limit=%s, event=%s)...",
                limit, asked or "all mapped")
    try:
        results = reg.run(dry_run=True, limit=limit, **scope)
        if asked:
            results["scoped_event"] = asked
        return _format_registration_results(results)
    except reg.RegistrationsSyncDisabled as e:
        return ("⏸️ **Registrations sync is turned off.**\n\n"
                f"{e}\n\n"
                '• Say *"sync registrations dry run"* to preview it.')
    except reg.LimitRequired as e:
        return ("🛑 **No limit, no run.**\n\n"
                f"{e}")
    except reg.RegistrationWriteStopped as e:
        return ("🛑 **Registrations run stopped before writing.**\n\n"
                f"{e}")
    except Exception as e:
        logger.error("Registrations sync error: %s", e, exc_info=True)
        return f"❌ Registrations sync failed: {e}"


# How many events in review the preview names before summarising.
REGISTRATION_REVIEW_ROWS_SHOWN = 10


# interactionDateTime = min(event start, run time). Two rules, so the report
# has to say WHICH, not just that the value is an assumption. The old note
# said "is the EVENT START" unconditionally, which stopped being true the
# moment the clamp went in — and was already misleading for the six
# phase-1 events that start in the future.
_INTERACTION_RULE_LABELS = {
    "event_start": "event start",
    "run_time": "run time (event is in the future)",
}


def _registration_moment(ms) -> str:
    """Unix milliseconds as a readable UTC moment, or an em dash."""
    if ms in (None, ""):
        return "—"
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(
            int(ms) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError, OSError, OverflowError):
        return "—"


def _interaction_rule_note(results: dict, shown=None) -> str:
    """Which rule set interactionDateTime, and how many records got each.

    `shown` is the size of the previewed queue: in a dry run the rules are
    only known for the records in the table, and reporting those counts as
    if they covered the whole plan would overstate what was measured.
    """
    counts = results.get("interaction_rules") or {}
    starts = counts.get("event_start", 0)
    runs = counts.get("run_time", 0)
    if not (starts or runs):
        return ""
    scope = f"of the {shown} shown" if shown is not None else "record(s)"
    return (f"🕒 `interactionDateTime` is **min(event start, run time)** — "
            f"{scope}: **{starts}** used the EVENT START (already past), "
            f"**{runs}** used the RUN TIME (the event has not happened yet, "
            f"so its start would claim a registration that has not "
            f"occurred). HubSpot documents the field as when the contact "
            f"subscribed; CSuite records no registration time, so this is "
            f"an assumption either way — but never one in the future.")


def _format_registration_results(results: dict) -> str:
    """Counts in the units they are measured in.

    A registration is a (person, event) pair, not a person: dedupe runs per
    event, so somebody at two events is two registrations. Reporting one
    number for both units is how 83 + 20 came to be read against 100.
    """
    if results.get("error"):
        return f"❌ **Registrations preview stopped.**\n\n{results['error']}"

    dry_run = results.get("dry_run", True)
    # APPLIED only when every attempted write verified. A run that stopped,
    # or had a failure, or attempted more than it verified, is STOPPED —
    # "✅ APPLIED" over a 400 is the shape this whole body of work exists to
    # remove.
    attempted = results.get("writes_attempted", 0)
    verified = results.get("registered", 0)
    clean = (not results.get("stopped") and not results.get("failed")
             and attempted == verified)
    if dry_run:
        head = "🧪 **Registrations — PREVIEW** (nothing was written)"
    elif clean:
        head = "✅ **Registrations — APPLIED**"
    elif results.get("unverified") and not results.get("failed"):
        # A third outcome, and it is neither of the other two. The write got
        # a 2xx and may well be in HubSpot — calling that "failed" is what
        # sent write_audit 80's registration to the review pile while the
        # UI showed it registered the whole time.
        head = "⚠️ **Registrations — UNVERIFIED** (the write may have landed)"
    else:
        head = "🛑 **Registrations — STOPPED**"
    lines = [head, "",
             f"📊 **Across {results.get('events_read', 0)} event(s):**",
             f"• **{results.get('registrant_rows', 0)}** registrant rows in "
             f"CSuite",
             f"• **{results.get('unique_emails', 0)}** registrations after "
             f"dedupe by email within each event "
             f"({results.get('duplicates_dropped', 0)} duplicate(s) dropped)",
             f"• **{results.get('would_register', 0)}** would be sent as "
             f"REGISTERED, by contact id",
             f"• **{results.get('withheld', 0)}** withheld — no HubSpot "
             f"contact for that address, and none would be created",
             f"• **{results.get('already', 0)}** already registered by an "
             f"earlier run"]

    if results.get("scoped_event"):
        lines.insert(2, f"🎯 Scoped to event `{results['scoped_event']}` "
                        f"only, by name in the command.")

    ended = results.get("ended_events") or []
    if ended:
        lines += ["", f"🕗 **{results.get('ended_records', 0)}** of those are "
                      f"on events that have already ENDED — HubSpot records "
                      f"a registration on an ended event as a **no-show**, "
                      f"not as a registration. They are sent anyway: which "
                      f"events and who registered matter more than "
                      f"attendance, and no attendance data is ever sent."]
        for event_id, count in ended[:REGISTRATION_REVIEW_ROWS_SHOWN]:
            lines.append(f"   `{event_id}` — **{count}** record(s) will "
                         f"appear as no-shows")
        if len(ended) > REGISTRATION_REVIEW_ROWS_SHOWN:
            lines.append(f"   … and "
                         f"**{len(ended) - REGISTRATION_REVIEW_ROWS_SHOWN}** "
                         f"more ended event(s)")

    if results.get("unverified_prior"):
        lines.append(
            f"• **{results['unverified_prior']}** held back from an earlier "
            f"run that could not be verified — NOT resent, because the "
            f"attendance endpoint has no idempotency key and a resend is "
            f"how one registration becomes two. Say *\"reconcile "
            f"registrations\"* to check them against HubSpot.")

    # Held events: counted on their own line, never inside "would be sent".
    if results.get("held"):
        held_events = results.get("held_events") or []
        lines.append(
            f"• **{results['held']}** {reg_held_reason()} — "
            f"event(s) {', '.join(f'`{e}`' for e in held_events)}. "
            f"Nothing is sent for these, and they are not in the count "
            f"above.")

    if results.get("non_marketing"):
        lines += ["", f"⚠️ **{results['non_marketing']}** of those contacts "
                      "are deliberately NON-marketing. They would be "
                      "registered, and `hs_marketable_status` is never "
                      "touched — it is read-only to the API, so this sync "
                      "cannot change who may be emailed."]

    queue = results.get("first_sends") or []
    if dry_run and queue:
        lines += ["", f"➡️ **The next {len(queue)} to be sent**, in send "
                      "order (event, then contact id):", "",
                  "| event | name | starts | contact | marketing | "
                  "interactionDateTime | rule |",
                  "|---|---|---|---|---|---|---|"]
        for entry in queue:
            name = str(entry.get("event_name") or "—")
            lines.append(
                f"| `{entry['event_date_id']}` | "
                f"{name[:40]} | "
                f"{_registration_moment(entry.get('event_start_ms'))} | "
                f"`{entry['hubspot_contact_id']}` | "
                f"{'yes' if entry.get('marketing') else 'NO'} | "
                f"{_registration_moment(entry.get('interaction_at'))} | "
                f"{_INTERACTION_RULE_LABELS.get(entry.get('interaction_rule'), '—')} |")
        lines.append("A limit of 1 sends the first row.")
        note = _interaction_rule_note(results, len(queue))
        if note:
            lines.append(note)

    rows = results.get("review_rows") or []
    if rows:
        lines += ["", "🔎 **Needs a human — nothing planned for these "
                      "events:**"]
        for event_id, why in rows[:REGISTRATION_REVIEW_ROWS_SHOWN]:
            lines.append(f"   `{event_id}` — {str(why)[:110]}")
        if len(rows) > REGISTRATION_REVIEW_ROWS_SHOWN:
            lines.append(f"   … and "
                         f"**{len(rows) - REGISTRATION_REVIEW_ROWS_SHOWN}** "
                         "more; the count above is the total")

    if not dry_run:
        lines += ["", f"• **{verified}** registered and verified in HubSpot"]
        deferred = results.get("deferred", 0)
        if deferred:
            lines.append(f"• **{deferred}** deferred — the limit of "
                         f"{results.get('limit')} was reached")
        elif results.get("stopped"):
            # "0 deferred — the limit was reached" said two contradictory
            # things: nothing was held back, and the cap stopped it. A run
            # that stops before the cap has deferred nothing.
            lines.append("• **0** deferred — the run stopped before the "
                         f"limit of {results.get('limit')} was reached")
        states = results.get("landed_states") or {}
        if states:
            shown = ", ".join(f"**{n}** {state}"
                              for state, n in sorted(states.items()))
            lines.append(f"• verified in HubSpot as: {shown} — NO_SHOW is "
                         f"what an ended event records, and is stored as "
                         f"`last_state`")
        if results.get("cancelled"):
            lines.append(
                f"• **{results['cancelled']}** came back CANCELLED in "
                f"HubSpot — recorded `review`, not `synced`. The POST "
                f"landed, but nobody is registered.")
        if results.get("unverified"):
            lines.append(
                f"• **{results['unverified']}** returned 2xx but could NOT "
                f"be verified — recorded `unverified`, not resent. The write "
                f"may have landed; say *\"reconcile registrations\"* to "
                f"check HubSpot and record it.")
        if results.get("failed"):
            lines.append(f"• **{results['failed']}** failed — see the stop "
                         f"reason below")
        note = _interaction_rule_note(results)
        if note:
            lines.append("   " + note)

    lines += ["", f"📞 {results.get('csuite_calls', 0)} CSuite call(s), "
                  f"{results.get('hubspot_calls', 0)} HubSpot read call(s)"]
    if dry_run:
        lines.append("✍️ **0 HubSpot writes — nothing was sent.**")
    else:
        ids = results.get("write_audit_ids") or []
        trail = (" (write_audit " + ", ".join(str(i) for i in ids) + ")"
                 if ids else " (no write_audit ids — the audit could not be "
                             "read back)")
        lines.append(f"✍️ **{results.get('writes_attempted', 0)} HubSpot "
                     f"write(s) attempted, {results.get('registered', 0)} "
                     f"verified**{trail}")

    if results.get("stopped"):
        # The reason already ends with what was and was not written; adding
        # a second sentence saying it again was how the old report said
        # "Nothing further was written" twice.
        lines += ["", f"🛑 **Stopped:** {results['stopped']}",
                  "Every registration that did land is in "
                  "`registration_map`; every attempt is in `write_audit`."]

    if not results.get("migration_applied"):
        lines += ["", "⚠️ `hubsync.registration_map` does not exist, so this "
                      "run has no memory of earlier ones: everything reads "
                      "as new, and no cancellation could be inferred even if "
                      "phase 1 tried. Apply "
                      "`migrations/005_registration_map.sql`."]
    if results.get("run_logged"):
        lines += ["", f"📒 Recorded as `hubsync.run_log` id "
                      f"{results.get('run_id')} with "
                      f"{results.get('unique_emails', 0)} per-record input "
                      f"row(s) — hashed addresses, never addresses."]
    return "\n".join(lines)


def _sync_events(query_lower: str) -> str:
    """Plan an event sync, and write only if the message says to.

    One implementation, shared with scripts/event_sync.py — sync/events.py was
    retired on 2026-10-06. It had its own payload shape, no notion of an
    update, a dedup check that called a HubSpot endpoint which 404s for every
    id, and a different externalAccountId from the module whose job was to own
    that value.
    """
    apply = EVENT_APPLY_PHRASE in query_lower
    limit = _event_limit(query_lower) if apply else None
    logger.info("Running event sync (apply=%s, limit=%s)...", apply, limit)

    options = _event_options(query_lower)
    try:
        results = event_apply.run(dry_run=not apply, limit=limit, **options)
        return _format_event_sync_results(results)
    except Exception as e:
        logger.error(f"Event sync error: {e}", exc_info=True)
        return f"❌ Event sync failed: {e}"


def _sync_newsletter(query_lower: str) -> str:
    """Run newsletter sync (CSuite → HubSpot)."""
    logger.info("Running newsletter sync...")
    dry_run = 'dry run' in query_lower or 'test' in query_lower

    try:
        results = run_newsletter_sync(dry_run=dry_run, quick=dry_run)
        return _format_newsletter_sync_results(results, dry_run)
    except NewsletterSyncDisabled as e:
        logger.info("newsletter sync is off: %s", e)
        return ("⏸️ **Newsletter sync is turned off.**\n\n"
                "No HubSpot subscription was read or changed. It POSTs a "
                "subscription change per opted-in CSuite profile, which is a "
                "change to a real person's communication preferences.\n\n"
                "• Say *\"sync newsletter dry run\"* to preview it safely.\n"
                "• Set `CSUITE_NEWSLETTER_SYNC_ENABLED=true` to run it for "
                "real.")
    except Exception as e:
        logger.error(f"Newsletter sync error: {e}")
        return f"❌ Newsletter sync failed: {e}"


# ---------------------------------------------------------------------------
# Sync-all
# ---------------------------------------------------------------------------

def _run_all_syncs() -> str:
    """Run all sync operations sequentially."""
    responses = []

    try:
        donation_results = run_donation_sync(dry_run=False)
        responses.append(f"✅ Donations: {donation_results['updated']} updated")
    except DonationSyncDisabled:
        # "sync all" is the path that would have walked straight past a gate
        # placed in _sync_donations. Named distinctly so a skipped sync is not
        # read as a failed one, or as a sync that ran and found nothing.
        responses.append("⏸️ Donations: skipped — "
                         "CSUITE_DONATION_SYNC_ENABLED is off")
    except Exception as e:
        responses.append(f"❌ Donations: {e}")

    try:
        # Capped, like a chat live run: "sync all" is one line too.
        event_results = event_apply.run(
            dry_run=False, limit=event_apply.CHAT_DEFAULT_LIMIT)
        if event_results.get("error"):
            responses.append(f"❌ Events: {event_results['error']}")
        else:
            responses.append(
                f"✅ Events: {event_results['created']} created, "
                f"{event_results['updated']} updated, "
                f"{event_results['unchanged']} unchanged"
                + (f", {event_results['review']} need a human"
                   if event_results.get("review") else ""))
    except Exception as e:
        responses.append(f"❌ Events: {e}")

    try:
        newsletter_results = run_newsletter_sync(dry_run=False)
        responses.append(f"✅ Newsletter: {newsletter_results['subscribed']} subscribed")
    except NewsletterSyncDisabled:
        # Same reason as donations: "sync all" is the path that walks past a
        # gate placed in the chat handler.
        responses.append("⏸️ Newsletter: skipped — "
                         "CSUITE_NEWSLETTER_SYNC_ENABLED is off")
    except Exception as e:
        responses.append(f"❌ Newsletter: {e}")

    return "✅ **All Syncs Complete**\n\n" + "\n".join(responses)


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

# What chat says instead of attempting a full preview inline. The numbers are
# measured, not estimated: 18,823 profiles and 26,597 donations in the mirror
# on 2026-10-06, of which 7,604 donor profiles carry an email.
DONATION_FULL_PREVIEW_REPLY = (
    "📚 **A full preview has to run from the command line.**\n\n"
    "It reads ~18,800 CSuite profiles and ~26,600 donations, then searches "
    "HubSpot once per donor with an email — about 7,604 searches. HubSpot "
    "caps its Search API at **5 requests per second** and this paces at 4 to "
    "stay clear of it, so that half alone takes **~32 minutes**, and a web "
    "request is killed at 180 seconds. Run from chat it would die about a "
    "tenth of the way in, take the worker's other requests down with it, and "
    "throw away everything it had read.\n\n"
    "```\n"
    "python scripts/donation_preview.py --full --out donation_preview.md\n"
    "```\n\n"
    "It writes nothing, prints progress as it goes, and uses the same planner "
    "and the same report as this command.\n\n"
    '• Say *"sync donations dry run"* for the 500-row sample, which does run '
    "here.")

# Roughly how many CSuite profiles a full preview pages, so the hint can state
# the cost before someone asks for it. Measured 18,797 on 2026-10-01 via the
# unfiltered profile/list total; it only has to be the right order of
# magnitude, and the full run reports what it actually read.
DONATION_PROFILE_ESTIMATE = 18800

# Kept in step with sync.donations.SAMPLE_SIZE, imported rather than repeated.
from sync.donations import SAMPLE_SIZE as DONATION_SAMPLE_SIZE  # noqa: E402

# How many shared-email contacts the donation report prints before summarising.
SHARED_EMAIL_ROWS_SHOWN = 10

# How many "needs a human" rows the event report prints before summarising.
EVENT_REVIEW_ROWS_SHOWN = 10

# How many withheld rows the event report prints before summarising.
EVENT_WITHHELD_ROWS_SHOWN = 10

# How many link rows the dry-run table shows. The counts above it are the real
# totals; this caps only what is printed, so a long run stays readable.
LINK_ROWS_SHOWN = 25


def _format_donation_sync_results(results: dict, dry_run: bool) -> str:
    prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""
    verb = "would be updated" if dry_run else "updated"

    response = f"""{prefix}✅ **Donation Sync Complete**

📊 **Results:**
• **{results['updated']}** contacts {verb} with donation data
• **{results['skipped_no_email']}** profiles skipped (no email in CSuite)
• **{results['skipped_not_found']}** profiles skipped (not found in HubSpot)
• **{results['errors']}** errors

💡 Fields: `lifetime_giving`, `last_donation_date`, `last_donation_amount`, `donation_count`, `csuite_profile_id`"""

    response += _format_link_outcomes(results, dry_run)

    if dry_run:
        # It used to say "Run `sync donations` without 'dry run' for full
        # sync", which was an instruction that no longer works and never
        # should have: a plain "sync donations" is refused unless the flag is
        # set, and telling someone to type it invites them to read the
        # refusal as a fault.
        if results.get("partial") and results.get("sampled", True):
            response += (
                f"\n\n🛑 *PARTIAL READ — the sample stopped early "
                f"({results.get('partial_reason') or 'reason not recorded'}). "
                "Not even a sample of the size asked for.*")
        elif results.get("sampled", True):
            response += (
                f"\n\n⚡ *Sampled: {DONATION_SAMPLE_SIZE} profiles, "
                f"{DONATION_SAMPLE_SIZE} donations — not the whole database, "
                f"so these counts are not totals.*\n"
                "💡 *For the real figures — the shared-email and stale-link "
                "counts cannot be read off a sample — run "
                "`python scripts/donation_preview.py --full`. That reads "
                f"roughly {DONATION_PROFILE_ESTIMATE:,} profiles and takes "
                "tens of minutes, which is why it is not a chat command. It "
                "writes nothing.*")
        elif results.get("partial"):
            # The one thing this footer must never do. A sweep that stopped
            # short still has a profiles_read figure, and before 2026-10-06 a
            # failed page read as the end of the data — so "the whole
            # database, so these counts are totals" would have been printed
            # over a read that a 429 cut off.
            response += (
                f"\n\n🛑 *PARTIAL READ — {results.get('profiles_read', 0):,} "
                f"profiles and {results.get('donations_read', 0):,} donations "
                f"were read before CSuite stopped answering "
                f"({results.get('partial_reason') or 'reason not recorded'}). "
                "**These counts are NOT totals** and the missing rows are "
                "not estimated. Re-run when CSuite is answering.*")
        else:
            response += (
                f"\n\n📚 *Full preview: {results.get('profiles_read', 0):,} "
                f"profiles and {results.get('donations_read', 0):,} donations "
                f"read — the whole database, so these counts are totals.*")
        response += (
            "\n🔒 *A live run needs `CSUITE_DONATION_SYNC_ENABLED=true`. "
            "Without it `sync donations` is refused and nothing is written.*")

    return response


def _format_link_outcomes(results: dict, dry_run: bool) -> str:
    """What happened to csuite_profile_id, and the per-contact table.

    Separated out because it is the part a person has to read before enabling
    the sync: `csuite_profile_id` is the field the DAF duplicate guard trusts,
    and this sync is the other thing that writes it.
    """
    keys = ("link_written", "link_unchanged", "link_conflict", "link_differs",
            "link_stale", "link_unverifiable", "shared_email")
    if not any(key in results for key in keys):
        return ""            # an older result dict; nothing to add

    labels = [
        ("link_written", "written (contact had none)"),
        ("link_unchanged", "already correct — not rewritten"),
        ("link_conflict", "**left alone — points at a different live "
                          "profile**"),
        ("link_stale", "**left alone — stored id is not in CSuite**"),
        ("link_unverifiable", "left alone — CSuite would not confirm the "
                              "stored id"),
        ("link_differs", "left alone — differs from the stored id, which this "
                         "preview did not check"),
    ]
    lines = ["", "",
             "🔗 **csuite_profile_id**  "
             "*(the four donation fields are written either way)*"]
    for key, label in labels:
        count = results.get(key, 0)
        if count:
            lines.append(f"• **{count}** {label}")
    if not any(results.get(key) for key, _ in labels):
        lines.append("• nothing to link")

    if results.get("link_stale"):
        lines.append("   ⚠️ A stale id is NOT repointed — overwriting it "
                     "destroys the only record of what it pointed at.")

    shared = results.get("shared_email_rows") or []
    if results.get("shared_email"):
        lines += ["", f"👥 **{results['shared_email']} contact(s) claimed by "
                      f"more than one CSuite profile — nothing was written "
                      f"to them**"]
        for row in shared[:SHARED_EMAIL_ROWS_SHOWN]:
            current = row.get("current") or "none"
            lines.append(f"• `{row['contact_id']}` ← profiles "
                         f"{', '.join(row['profiles'])} "
                         f"(stored link: {current})")
        if len(shared) > SHARED_EMAIL_ROWS_SHOWN:
            lines.append(f"• … and **{len(shared) - SHARED_EMAIL_ROWS_SHOWN}** "
                         "more; the count above is the total")
        lines.append("   ⚠️ Totals are **not summed** — two profiles on one "
                     "address may be one person entered twice or two people "
                     "in a household, and this sync cannot tell. Merge or "
                     "separate them in CSuite.")

    rows = results.get("link_rows") or []
    if dry_run and rows:
        shown = rows[:LINK_ROWS_SHOWN]
        resolved = any("proposed_exists" in row for row in shown)
        lines += [
            "",
            f"**Proposed links — first {len(shown)} of {len(rows)}**"
            + ("  \n*(existence checked for the rows shown)*" if resolved
               else ""),
            "",
            "| contact | current | proposed | proposed in CSuite? | action |",
            "|---|---|---|---|---|",
        ]
        for row in shown:
            current = row.get("current") or "—"
            if row.get("current_exists"):
                current = f"{current} ({row['current_exists']})"
            lines.append(
                f"| `{row['contact_id']}` | {current} | "
                f"`{row['proposed']}` | "
                f"{row.get('proposed_exists', 'not checked')} | "
                f"{row.get('action', '')} |")
        if len(rows) > len(shown):
            lines.append("")
            lines.append(f"… and **{len(rows) - len(shown)}** more rows not "
                         "shown. The counts above are the full totals.")

    if results.get("profile_reads"):
        lines.append("")
        lines.append(f"*{results['profile_reads']} CSuite profile read(s) "
                     "were made to answer this.*")

    return "\n".join(lines)


def _format_event_sync_results(results: dict) -> str:
    """Six outcomes, six lines. None of them stands in for another.

    The old formatter reported `created` and `skipped_exists` and had no words
    for an update, for a record it deferred, for a create that came back
    without an id, or for one a person has to look at. A sync that can only
    say "created 7" cannot tell you it changed nothing, and that was true of
    it on every run after the first.
    """
    if results.get("error"):
        return f"❌ **Event sync stopped.**\n\n{results['error']}"

    dry_run = results.get("dry_run", True)
    head = ("🧪 **Event Sync — DRY RUN**" if dry_run
            else "✅ **Event Sync — APPLIED**")
    lines = [head, ""]

    verb = "would be " if dry_run else ""
    lines += [
        "📊 **Outcomes:**",
        f"• **{results.get('created', 0)}** {verb}created",
        f"• **{results.get('updated', 0)}** {verb}updated",
        f"• **{results.get('unchanged', 0)}** unchanged — nothing to send",
    ]
    lines.append(
        f"• **{results.get('withheld', 0)}** withheld — planned, counted, "
        f"and deliberately NOT written")
    if not dry_run:
        lines.append(
            f"• **{results.get('deferred', 0)}** deferred — the "
            f"`limit` was reached, so they were not attempted")
        lines.append(
            f"• **{results.get('unknown', 0)}** unknown — the create came "
            f"back with no id, so it may or may not have landed. "
            f"**Never retried**; the next run resolves it by lookup")
        lines.append(f"• **{results.get('failed', 0)}** failed")
    # `review` is an overlay: plan() appends to it IN ADDITION to a
    # record's create/update bucket, so it double-counts against the lines
    # above. Measured 2026-10-06, all 79 review entries were also creates
    # (73) or updates (6). Saying so is the difference between a reader
    # reconciling the numbers and a reader hunting for 19 missing records.
    lines.append(
        f"• **{results.get('review', 0)}** need a human — *also counted "
        f"above*, not a separate group. All of these are withheld.")
    lines.append(
        f"• **{results.get('skipped', 0)}** not syncable (no event date in "
        f"CSuite)")

    withheld = results.get("withheld_rows") or []
    if withheld:
        lines += ["", "🚫 **Withheld — nothing was written to these:**"]
        from collections import Counter
        kinds = Counter(
            "needs a human" if str(why).startswith("needs a human")
            else "starts in the past" if "starts in the past" in str(why)
            else "updates only" if "updates only" in str(why)
            else "other"
            for _id, why in withheld)
        for kind, count in kinds.most_common():
            lines.append(f"• **{count}** {kind}")
        for record_id, why in withheld[:EVENT_WITHHELD_ROWS_SHOWN]:
            lines.append(f"   `{record_id}` — {str(why)[:96]}")
        if len(withheld) > EVENT_WITHHELD_ROWS_SHOWN:
            lines.append(f"   … and **"
                         f"{len(withheld) - EVENT_WITHHELD_ROWS_SHOWN}** more; "
                         "the count above is the total")

    rows = results.get("review_rows") or []
    if rows:
        lines += ["", "🔎 **Needs a human:**"]
        for eventdate_id, name, reason in rows[:EVENT_REVIEW_ROWS_SHOWN]:
            label = (name or "(no name)")[:60]
            lines.append(f"• `{eventdate_id}` {label} — {reason}")
        if len(rows) > EVENT_REVIEW_ROWS_SHOWN:
            lines.append(f"• … and **{len(rows) - EVENT_REVIEW_ROWS_SHOWN}** "
                         "more; the count above is the total")

    if results.get("stopped"):
        lines += ["", f"🛑 **Stopped:** {results['stopped']}",
                  "Nothing further was created. Re-run after checking "
                  "HubSpot — the next run resolves the ambiguous record by "
                  "lookup rather than retrying it."]

    # Writes, stated rather than inferred. The report used to end with read
    # counts only, so a run that withheld everything read identically to one
    # that wrote nothing by luck — and the write_audit ids make the reply
    # checkable against the table instead of merely believable.
    if results.get("dry_run", True):
        lines += ["", "✍️ **0 HubSpot writes — nothing was sent.**"]
    else:
        attempted = results.get("writes_attempted", 0)
        succeeded = results.get("writes_succeeded", 0)
        ids = results.get("write_audit_ids") or []
        trail = (" (write_audit " + ", ".join(str(i) for i in ids) + ")"
                 if ids else " (no write_audit ids — the audit could not be "
                             "read back)")
        line = (f"✍️ **{attempted} HubSpot write(s) attempted, {succeeded} "
                f"succeeded**{trail}")
        if attempted and succeeded < attempted:
            line += f"  \n   ⚠️ {attempted - succeeded} did not succeed."
        lines += ["", line]

    notes = [(record_id, note) for record_id, note in
             (results.get("duration_notes") or [])]
    if notes:
        lines += ["", "🕒 **End times are assumed** — CSuite carries no end "
                      "time, so HubSpot's own duration was reapplied:"]
        for record_id, note in notes[:EVENT_WITHHELD_ROWS_SHOWN]:
            lines.append(f"   `{record_id}` — {note}")

    lines += ["", f"📞 {results.get('csuite_calls', 0)} CSuite call(s), "
                  f"{results.get('hubspot_calls', 0)} HubSpot read call(s), "
                  f"{results.get('event_dates_read', 0)} event date(s) read"]

    if dry_run:
        # Says what was actually written. The CLI printed "nothing was
        # written to HubSpot or to hubsync" while record_run had just
        # inserted a run_log row — a message that contradicted the code one
        # line above it.
        written = ("One `hubsync.run_log` row was written, recording that "
                   "this preview happened."
                   if results.get("run_logged")
                   else "Nothing was written anywhere — `hubsync` is not "
                        "available, so not even the run log.")
        lines += ["", f"ℹ️ **Nothing was written to HubSpot.** {written}", "",
                  'Say *"sync events apply"* to write it, '
                  f'capped at {event_apply.CHAT_DEFAULT_LIMIT} records — '
                  'add *"limit 20"* or *"no limit"* to change that.']
    if results.get("updates_only"):
        lines += ["", "ℹ️ **Updates only** — every create was withheld."]
    elif not results.get("future_only", True):
        lines += ["", "⚠️ **future_only is off** — past events can be "
                      "created."]
    if results.get("include_ids"):
        lines += ["", "ℹ️ Past creates allowed by id: "
                      + ", ".join(f"`{i}`" for i in results["include_ids"])]

    if results.get("limit") is not None and not results.get("dry_run", True):
        lines += ["", f"ℹ️ Capped at {results['limit']} record(s) this run. "
                      'Say *"no limit"* to lift it.']

    if not results.get("migration_applied"):
        lines += ["", "⚠️ `hubsync.event_map` does not exist, so this run had "
                      "no duplicate guard and no memory of previous runs. "
                      "Apply `migrations/001_hubsync_event_map.sql`."]

    return "\n".join(lines)


def _format_newsletter_sync_results(results: dict, dry_run: bool) -> str:
    prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""

    response = f"""{prefix}✅ **Newsletter Sync Complete**

📊 **Results:**
• **{results['subscribed']}** contacts {"would be subscribed" if dry_run else "subscribed"}
• **{results['already_subscribed']}** already subscribed
• **{results['skipped_not_found']}** not found in HubSpot
• **{results['errors']}** errors"""

    if dry_run:
        response += "\n\n⚡ *This dry run used sample data. Run `sync newsletter` without 'dry run' for full sync.*"

    return response