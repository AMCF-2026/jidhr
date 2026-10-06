"""Running an event sync: the map, the plan, the writes, the run log.

Lifted out of scripts/event_sync.py on 2026-10-06 so the chat command and the
CLI share one implementation. Before that there were two: this one, and
sync/events.py, which had its own payload shape, its own dedup rule (a HubSpot
endpoint that 404s for every id), no notion of an update, and a different
externalAccountId. The two could write to the same marketing event and
disagree about what it was keyed under.

CSuite is read-only here, enforced by sync.event_hubspot.read_only_endpoint.
HubSpot is never deleted from.

Requires hubsync.event_map and hubsync.run_log — migrations/001. Nothing here
creates them: a job that silently creates its own tables is a job nobody
reviewed the schema of.
"""

import json
import logging

from clients import database
from clients.hubspot import HubSpotClient
from sync import event_hubspot as eh

logger = logging.getLogger(__name__)

DEFAULT_ORGANIZER = "American Muslim Community Foundation"

# A chat-triggered live run writes at most this many records unless the
# message says otherwise. The CLI has no default cap because a person typing
# --apply has already read a dry-run report; a chat message is one line and
# might be a mistake.
CHAT_DEFAULT_LIMIT = 5

# Marketing events: POST here to create, PATCH <this>/<externalEventId> to
# update. Not the PUT-upsert shape clients/hubspot.create_marketing_event
# uses — the create/update split is what lets a run report "unchanged",
# which an upsert cannot.
CREATE_ENDPOINT = "marketing/v3/marketing-events"

_TABLES_SQL = """
    SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'hubsync'
       AND table_name IN ('event_map', 'run_log')
"""

_MAP_SQL = "SELECT * FROM hubsync.event_map"

_UPSERT_MAP_SQL = """
    INSERT INTO hubsync.event_map (
        csuite_eventdate_id, hubspot_event_id, external_event_id,
        content_hash, last_synced_at, status, last_error, review_reason)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (csuite_eventdate_id) DO UPDATE
       SET hubspot_event_id = COALESCE(EXCLUDED.hubspot_event_id,
                                       hubsync.event_map.hubspot_event_id),
           external_event_id = EXCLUDED.external_event_id,
           content_hash = EXCLUDED.content_hash,
           last_synced_at = EXCLUDED.last_synced_at,
           status = EXCLUDED.status,
           last_error = EXCLUDED.last_error,
           review_reason = EXCLUDED.review_reason,
           updated_at = NOW()
"""

_RUN_SQL = """
    INSERT INTO hubsync.run_log (
        job, applied, finished_at, csuite_calls, hubspot_calls,
        event_dates_read, created_count, updated_count, unchanged_count,
        skipped_count, review_count, failed_count, status, error_summary,
        outcomes)
    VALUES ('event_sync', %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s::jsonb)
    RETURNING id
"""


def migration_applied() -> bool:
    try:
        found = database.execute_query(_TABLES_SQL, (), fetch=True)
    except Exception:
        return False
    names = {r["table_name"] if isinstance(r, dict) else r[0] for r in found}
    return {"event_map", "run_log"} <= names


def load_map() -> dict:
    try:
        rows = database.execute_query(_MAP_SQL, (), fetch=True)
    except Exception:
        return {}
    return {str(r["csuite_eventdate_id"]): r for r in rows}


def plan(rows, existing_map, hubspot_events, organizer):
    """Decide what each event date needs. No writes, no HubSpot calls."""
    creates, updates, unchanged, skipped, review = [], [], [], [], []

    for row in rows:
        mapped = eh.map_event_date(row, organizer)
        if not mapped.csuite_eventdate_id:
            skipped.append((mapped, "no event_date_id"))
            continue
        if not mapped.syncable:
            skipped.append((mapped, mapped.review_reason))
            continue

        known = existing_map.get(mapped.csuite_eventdate_id)
        in_hubspot = hubspot_events.get(mapped.external_event_id)

        if known and str(known.get("status")) == "unknown":
            # An earlier create was ambiguous. Resolve by looking, never
            # by retrying: a retried create with no idempotency key is
            # how one event becomes two.
            if in_hubspot:
                updates.append((mapped, in_hubspot, "resolving 'unknown': "
                                "the earlier create did land"))
            else:
                creates.append((mapped, "resolving 'unknown': the earlier "
                                "create did not land"))
            continue

        if in_hubspot:
            hubspot_id = str(in_hubspot.get("objectId") or "")
            if known and known.get("content_hash") == mapped.content_hash:
                unchanged.append((mapped, hubspot_id))
            elif known:
                updates.append((mapped, in_hubspot, "content hash changed"))
            else:
                updates.append((mapped, in_hubspot,
                                "already in HubSpot, not in event_map — "
                                "adopting it"))
            continue

        if known and known.get("hubspot_event_id"):
            review.append((mapped, "event_map has a HubSpot id but HubSpot "
                           "does not list that externalEventId — needs a "
                           "person"))
            continue

        creates.append((mapped, "new"))

    for mapped, _reason in list(creates) + [(m, r) for m, _h, r in updates]:
        if mapped.review_reason:
            review.append((mapped, mapped.review_reason))

    return {"creates": creates, "updates": updates, "unchanged": unchanged,
            "skipped": skipped, "review": review}


def _save_map(mapped, hubspot_event_id, status, error=None):
    """Write the mapping row. Called IMMEDIATELY after a create returns.

    Immediately, because the window between HubSpot minting an id and us
    recording it is the window in which a crash produces a duplicate on
    the next run.
    """
    database.execute_query(_UPSERT_MAP_SQL, (
        mapped.csuite_eventdate_id,
        str(hubspot_event_id) if hubspot_event_id else None,
        mapped.external_event_id,
        mapped.content_hash,
        datetime.now(timezone.utc),
        status,
        str(error)[:500] if error else None,
        mapped.review_reason,
    ), fetch=False)


class FirstFailureStop(Exception):
    """A create failed or came back ambiguous, so the run stops here.

    Carries the outcomes recorded up to that point, because a partial run
    that reports nothing is worse than one that reports where it got to.
    """

    def __init__(self, outcomes, reason):
        super().__init__(reason)
        self.outcomes = outcomes
        self.reason = reason


def apply_plan(hubspot, result, limit=None) -> list:
    """Create and update in HubSpot. Never deletes. Returns outcomes.

    Stops on the FIRST create that fails or comes back ambiguous, and
    makes no further creates. With no idempotency key, an error is not
    evidence about what the next call will do — it may be a bad payload,
    a revoked scope, or a rate limit, and running eighty more creates to
    find out produces eighty more things to clean up by hand.

    `limit` caps how many records this run writes. Updates count toward
    it as well as creates: the point of a limit is to bound the blast
    radius of a run, and an update to the wrong event is not free.
    """
    outcomes = []
    written = 0

    def budget_left():
        return limit is None or written < limit

    for mapped, why in result["creates"]:
        if not budget_left():
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "deferred",
                             "why": f"--limit {limit} reached"})
            continue
        created = hubspot._post(CREATE_ENDPOINT, mapped.payload)
        event_id = (created or {}).get("objectId") or (created or {}).get("id")
        error = (created or {}).get("error")

        if event_id:
            _save_map(mapped, event_id,
                      "review" if mapped.review_reason else "synced")
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "created", "hubspot_id": str(event_id),
                             "why": why})
            written += 1
            continue

        # Ambiguous: no id came back. It may or may not have landed, and
        # there is no idempotency key to make a retry safe. Record
        # 'unknown', and STOP the run — see apply_plan's docstring.
        _save_map(mapped, None, "unknown", error or "no objectId in response")
        outcomes.append({"id": mapped.csuite_eventdate_id,
                         "outcome": "unknown",
                         "why": "create returned no id — NOT retried; the "
                                "next run resolves it by lookup",
                         "error": str(error)[:200] if error else None})
        raise FirstFailureStop(
            outcomes,
            f"create for {mapped.external_event_id} returned no id "
            f"({error or 'no objectId in response'}). Stopped before any "
            "further create.")

    for mapped, existing, why in result["updates"]:
        if not budget_left():
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "deferred",
                             "why": f"--limit {limit} reached"})
            continue
        hubspot_id = str(existing.get("objectId") or "")
        # PATCH by externalEventId is the documented update path for
        # marketing events; the objectId is stored for people, not used
        # as the write key.
        updated = hubspot._patch(
            f"{CREATE_ENDPOINT}/{mapped.external_event_id}", mapped.payload)
        error = (updated or {}).get("error")
        if error:
            _save_map(mapped, hubspot_id, "error", error)
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "failed", "why": why,
                             "error": str(error)[:200]})
        else:
            _save_map(mapped, hubspot_id,
                      "review" if mapped.review_reason else "synced")
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "updated", "hubspot_id": hubspot_id,
                             "why": why})
            written += 1

    # Unchanged rows still get their timestamp refreshed, so "last seen"
    # and "last changed" are different questions with different answers.
    for mapped, hubspot_id in result["unchanged"]:
        _save_map(mapped, hubspot_id,
                  "review" if mapped.review_reason else "synced")
        outcomes.append({"id": mapped.csuite_eventdate_id,
                         "outcome": "unchanged", "hubspot_id": hubspot_id})

    # Never written to HubSpot; recorded so a person can find them.
    for mapped, reason in result["skipped"]:
        if mapped.csuite_eventdate_id:
            _save_map(mapped, None, "review", None)
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "skipped", "why": reason})

    return outcomes


def summarise_outcomes(outcomes) -> list:
    from collections import Counter
    counts = Counter(o["outcome"] for o in outcomes)
    lines = ["", "--apply results:"]
    for name in ("created", "updated", "unchanged", "unknown", "failed",
                 "skipped"):
        if counts.get(name):
            lines.append(f"  {name:10} {counts[name]}")
    for o in outcomes:
        if o["outcome"] in ("unknown", "failed"):
            lines.append(f"  ! {o['id']}: {o.get('why')} "
                         f"{o.get('error') or ''}")
    return lines


def record_run(result, fetched, hs_calls, applied, outcomes=None) -> None:
    """One run_log row per run, dry ones included.

    A dry run is logged too: "we looked and would have done nothing" is
    worth being able to prove later.
    """
    from collections import Counter
    counts = Counter(o["outcome"] for o in (outcomes or []))
    try:
        database.execute_query(_RUN_SQL, (
            applied, fetched.calls, hs_calls, len(fetched.rows),
            counts.get("created", 0) if applied else len(result["creates"]),
            counts.get("updated", 0) if applied else len(result["updates"]),
            counts.get("unchanged", 0) if applied else len(result["unchanged"]),
            len(result["skipped"]), len(result["review"]),
            counts.get("failed", 0) + counts.get("unknown", 0),
            "complete", None,
            json.dumps(outcomes or [], default=str),
        ), fetch=True)
    except Exception as e:
        # A library module logs; it does not write to a CLI's stderr. The
        # run still happened, and failing it over bookkeeping would be worse.
        logger.error("could not write the run log: %s", e)




def run(hubspot=None, dry_run: bool = True, limit=None,
        organizer: str = DEFAULT_ORGANIZER, pace_ms=None) -> dict:
    """Plan an event sync and, unless `dry_run`, apply it.

    Returns a dict the caller can render: counts, outcomes, the plan itself,
    and `stopped` when apply_plan halted on an ambiguous create. Never raises
    for an ordinary failure — a read that fails comes back as `error`.
    """
    from clients.csuite import CSuiteClient

    out = {"dry_run": dry_run, "limit": limit, "organizer": organizer,
           "error": None, "stopped": None, "outcomes": [],
           "run_logged": False, "migration_applied": False,
           "created": 0, "updated": 0, "unchanged": 0, "deferred": 0,
           "unknown": 0, "failed": 0, "skipped": 0, "review": 0,
           "csuite_calls": 0, "hubspot_calls": 0, "event_dates_read": 0,
           "review_rows": [], "plan": None}

    have_tables = migration_applied()
    out["migration_applied"] = have_tables
    if not have_tables and not dry_run:
        # The map IS the duplicate guard. Writing without it is how one event
        # becomes two.
        out["error"] = ("hubsync.event_map and hubsync.run_log do not exist, "
                        "so a live run has no duplicate guard. Apply "
                        "migrations/001_hubsync_event_map.sql first.")
        return out

    hubspot = hubspot or HubSpotClient()
    fetched = eh.fetch_event_dates(CSuiteClient(), pace_ms=pace_ms)
    out["csuite_calls"] = fetched.calls
    out["event_dates_read"] = len(fetched.rows)
    if fetched.error:
        out["error"] = f"CSuite read failed: {fetched.error}"
        return out

    index, hs_calls, hs_error = eh.hubspot_index(hubspot)
    out["hubspot_calls"] = hs_calls
    if hs_error:
        # Without the index every event looks absent, and every absent event
        # looks like a create. Refusing is the only safe answer.
        out["error"] = (f"HubSpot marketing events could not be listed "
                        f"({hs_error}), so nothing can be told apart from a "
                        f"new event. Nothing was planned.")
        return out

    result = plan(fetched.rows, load_map() if have_tables else {}, index,
                  organizer)
    out["plan"] = result
    out["skipped"] = len(result["skipped"])
    out["review"] = len(result["review"])
    out["review_rows"] = [(m.csuite_eventdate_id, m.source_name, reason)
                          for m, reason in result["review"]]

    if dry_run:
        out["created"] = len(result["creates"])
        out["updated"] = len(result["updates"])
        out["unchanged"] = len(result["unchanged"])
        if have_tables:
            record_run(result, fetched, hs_calls, applied=False)
            out["run_logged"] = True
        return out

    try:
        outcomes = apply_plan(hubspot, result, limit=limit)
    except FirstFailureStop as stop:
        outcomes, out["stopped"] = stop.outcomes, stop.reason

    out["outcomes"] = outcomes
    for name in ("created", "updated", "unchanged", "deferred", "unknown",
                 "failed"):
        out[name] = sum(1 for o in outcomes if o.get("outcome") == name)
    record_run(result, fetched, hs_calls, applied=True, outcomes=outcomes)
    out["run_logged"] = True
    return out
