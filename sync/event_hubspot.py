"""
CSuite event dates -> HubSpot marketing events
==============================================
One way. CSuite is read-only here, enforced rather than assumed.

    python scripts/event_sync.py              # dry run: reads, maps, prints
    python scripts/event_sync.py --apply      # writes to HubSpot

What this module will not do
---------------------------
* **It cannot write to CSuite.** Every endpoint goes through
  `read_only_endpoint()`, which calls `is_csuite_write()` and raises
  `CSuiteWriteRefused` if it returns True. CSuite v2 signs the request
  body, so every call is an HTTP POST and the verb says nothing about
  what a call does — the endpoint name is the only signal, which is why
  the check is by name and why it happens before the request rather than
  in review.
* **It never deletes anything in HubSpot.** An event date that vanishes
  from CSuite, or is archived, is recorded `review` with a reason. The
  sync does not cancel, archive or delete the HubSpot event.
* **It never invents a time.** See `start_moment()`.

Why the duplicate guard is local
--------------------------------
HubSpot's `GET marketing/v3/marketing-events/external/{id}` returns 404
for every id on this portal, including ids that demonstrably exist
(verified 2026-09-30). So a create cannot be made idempotent by asking
HubSpot about one id. Instead the whole marketing-event list is read in
one call and indexed by `externalEventId`; that index plus the UNIQUE
constraint on `hubsync.event_map.csuite_eventdate_id` is the guard.

An ambiguous create — timeout, 5xx, or a 2xx with no id in the body —
is recorded `unknown` and **never retried**. The next run finds it in the
listing and resolves it, or does not find it and creates it once. Retrying
a create with no idempotency key is how one event becomes two.
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from clients.csuite import is_csuite_write
from clients.csuite_fetch import CallBudget, fetch_all

logger = logging.getLogger(__name__)

# The one CSuite endpoint this job reads. Single page: it has no offset
# parameter, so asking for a second page returns the same rows.
EVENT_DATES_ENDPOINT = "event/list/dates"

# Headroom, not an expectation. event/list/dates is one call.
CALL_BUDGET = 5

# AMCF operates on the US East Coast, and this is the same assumption
# clients/mirror_read.DISPLAY_TZ already makes for every timestamp a
# person reads. Used ONLY when start_time names no zone, and a record
# that needs it is flagged for review rather than trusted.
DEFAULT_TZ = ZoneInfo("America/New_York")

# externalEventId convention. A private app named "Irritable-Needle" has
# already written four events using it, so changing it would orphan them.
EXTERNAL_PREFIX = "csuite-"

# Sent on every create. HubSpot's documentation lists externalAccountId
# as required, and it is None on all seven events already in the portal —
# so whatever the prior sync did, it satisfied the API without one. Set
# explicitly anyway: it is the field that says which upstream system a
# marketing event came from, and "the previous app got away with it" is
# not a reason to leave it blank.
EXTERNAL_ACCOUNT_ID = "amuslimcf-csuite"

# The fields sent to HubSpot, and therefore the only fields whose change
# should trigger an update. goal_amount and available_seats move without
# HubSpot ever seeing them; hashing the whole record would mean updating
# HubSpot because a seat was sold.
HASHED_FIELDS = ("event_description", "event_date", "start_time", "location",
                 "archived", "event_id")

# Zone names CSuite's free-text start_time actually uses, measured across
# all 179 event dates on 2026-09-30. Mapped to real zones so an offset is
# computed rather than guessed.
ZONE_WORDS = {
    "ET": "America/New_York", "EST": "America/New_York",
    "EDT": "America/New_York",
    "CT": "America/Chicago", "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MT": "America/Denver", "MST": "America/Denver", "MDT": "America/Denver",
    "PT": "America/Los_Angeles", "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "UTC": "UTC", "GMT": "UTC",
}

# "3 pm", "7:30 pm", "12 noon", "10:00 am"
_TIME_RE = re.compile(
    r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*"
    r"(?P<meridiem>am|pm|noon|midnight)\b", re.IGNORECASE)


class CSuiteWriteRefused(RuntimeError):
    """A CSuite endpoint that changes something was about to be called.

    Raised before the request is built, never after. This job is one-way:
    the only thing it may do to CSuite is read.
    """


def read_only_endpoint(endpoint: str) -> str:
    """Return the endpoint, or raise if it is a write.

    Called on every CSuite endpoint before it is sent. The check is by
    NAME because CSuite signs the body, so every call is a POST and the
    verb carries no information — see clients/csuite.is_csuite_write.
    """
    if is_csuite_write(endpoint):
        raise CSuiteWriteRefused(
            f"{endpoint!r} is a CSuite write endpoint and this sync is "
            "read-only on CSuite. Nothing was sent.")
    return endpoint


# ---------------------------------------------------------------------------
# Mapping one event date
# ---------------------------------------------------------------------------

def external_id(event_date_id) -> str:
    return f"{EXTERNAL_PREFIX}{event_date_id}"


def event_title(row: dict) -> str:
    """The human title.

    NOT event_name: that belongs to the parent series and takes three
    values across all 179 dates ("Unassigned", "Event - Other",
    "Newsletters"). event_description carries the title a person would
    recognise.
    """
    for field_name in ("event_description", "event_name"):
        text = " ".join(str((row or {}).get(field_name) or "").split())
        if text:
            return text
    return ""


def parse_zone(start_time) -> ZoneInfo | None:
    """The timezone named in start_time, or None if none is.

    None is a real answer and is treated as one: the caller flags the
    record for review rather than picking a zone quietly.
    """
    text = str(start_time or "")
    for word, zone in ZONE_WORDS.items():
        if re.search(rf"\b{word}\b", text):
            return ZoneInfo(zone)
    return None


def parse_time(start_time) -> time | None:
    """The first clock time in start_time, or None.

    Only a clock time. "September 3rd" is a date, not a time, and
    returning None for it is what stops this job repeating the existing
    sync's mistake of writing that date as the event's start.
    """
    match = _TIME_RE.search(str(start_time or ""))
    if not match:
        return None
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    meridiem = match.group("meridiem").lower()
    if meridiem == "noon":
        return time(12, minute)
    if meridiem == "midnight":
        return time(0, minute)
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    if not (0 <= hour <= 23) or not (0 <= minute <= 59):
        return None
    return time(hour, minute)


def start_moment(row: dict) -> tuple:
    """(aware datetime or None, review reason or None).

    CSuite has no timezone field and no datetime field — event_date is a
    bare date and start_time is free text populated on 28 of 179 rows
    (verified 2026-09-30). So:

      no event_date          -> not syncable. 98 of 179 rows.
      time and zone given    -> exact, no review
      time but no zone       -> assume ET and SAY SO
      no time                -> midnight ET and SAY SO

    A time is never invented out of nothing. The existing sync put
    10:00 on an event whose source has no time at all, and put a date
    parsed out of start_time in place of event_date on another; both are
    in the discovery report. Midnight is visibly a placeholder in a way
    that 10:00 is not.
    """
    day_text = str((row or {}).get("event_date") or "")[:10]
    try:
        day = datetime.strptime(day_text, "%Y-%m-%d").date()
    except ValueError:
        return None, "no event_date in CSuite — cannot be a marketing event"

    raw = (row or {}).get("start_time")
    clock = parse_time(raw)
    zone = parse_zone(raw)

    if clock and zone:
        return datetime.combine(day, clock, tzinfo=zone), None
    if clock:
        return (datetime.combine(day, clock, tzinfo=DEFAULT_TZ),
                f"start_time {raw!r} names no timezone — assumed "
                "America/New_York")
    return (datetime.combine(day, time(0, 0), tzinfo=DEFAULT_TZ),
            f"no usable time in start_time ({raw!r}) — placed at midnight "
            "America/New_York")


def as_offset(moment: datetime) -> str:
    """ISO 8601 with an explicit offset, never a bare Z on a local time.

    HubSpot accepts either; an explicit offset is what makes the stored
    value checkable against the source by eye.
    """
    return moment.isoformat()


def content_hash(row: dict) -> str:
    """sha256 over the mapped fields only.

    Not the whole record: available_seats and goal_amount change without
    HubSpot ever seeing them, and hashing them would push an update to
    HubSpot every time a ticket sold.
    """
    subset = {k: (row or {}).get(k) for k in HASHED_FIELDS}
    canonical = json.dumps(subset, sort_keys=True, default=str,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class Mapped:
    """One CSuite event date, ready for HubSpot or explicitly not."""

    csuite_eventdate_id: str
    external_event_id: str
    content_hash: str
    payload: dict | None = None
    syncable: bool = True
    review_reason: str | None = None
    archived: bool = False
    # Kept so a not-syncable record can be listed by name in a report.
    # An id on its own is not a work list.
    source_name: str = ""

    @property
    def status(self) -> str:
        if not self.syncable:
            return "review"
        return "review" if self.review_reason else "pending"


def map_event_date(row: dict, organizer: str) -> Mapped:
    """One mirrored/fetched event date -> a HubSpot marketing event payload."""
    event_date_id = str((row or {}).get("event_date_id") or "").strip()
    moment, reason = start_moment(row)
    archived = (row or {}).get("archived") in (1, "1", True)

    mapped = Mapped(
        csuite_eventdate_id=event_date_id,
        external_event_id=external_id(event_date_id),
        content_hash=content_hash(row),
        review_reason=reason,
        archived=archived,
        source_name=event_title(row),
    )

    if moment is None:
        mapped.syncable = False
        return mapped

    payload = {
        "eventName": event_title(row) or f"CSuite event date {event_date_id}",
        "externalEventId": mapped.external_event_id,
        "externalAccountId": EXTERNAL_ACCOUNT_ID,
        "eventOrganizer": organizer,
        "startDateTime": as_offset(moment),
    }
    description = " ".join(str((row or {}).get("location") or "").split())
    if description:
        payload["eventDescription"] = f"Location: {description}"
    mapped.payload = payload

    if archived:
        # Recorded, never acted on. Cancelling or deleting the HubSpot
        # event is exactly what this job must not do.
        mapped.review_reason = (
            (reason + "; " if reason else "")
            + "archived in CSuite — left untouched in HubSpot for review")
    return mapped


# ---------------------------------------------------------------------------
# Reading CSuite
# ---------------------------------------------------------------------------

@dataclass
class Fetched:
    rows: list = field(default_factory=list)
    calls: int = 0
    complete: bool = False
    error: str | None = None
    total_429s: int = 0


def fetch_event_dates(client, pace_ms=None, budget=CALL_BUDGET) -> Fetched:
    """Every CSuite event date. One call; read-only, enforced.

    Reuses clients/csuite_fetch so pacing, the 30/60/120s backoff and the
    Retry-After rule all apply unchanged.
    """
    endpoint = read_only_endpoint(EVENT_DATES_ENDPOINT)
    # A shared CallBudget, not a bare int: that is what fetch_all threads
    # through so a limit means calls-per-run rather than per-fetch.
    if not isinstance(budget, CallBudget):
        budget = CallBudget(budget)
    result = fetch_all(client, endpoint, pace_ms=pace_ms, budget=budget)
    return Fetched(
        rows=[r for r in (result.records or []) if isinstance(r, dict)],
        calls=result.calls,
        complete=result.complete,
        error=result.error,
        total_429s=result.total_429s,
    )


# ---------------------------------------------------------------------------
# Reading HubSpot's side
# ---------------------------------------------------------------------------

def hubspot_index(hubspot) -> tuple:
    """({externalEventId: record}, calls, error).

    One list call, indexed locally, because
    GET marketing/v3/marketing-events/external/{id} returns 404 for every
    id on this portal — including ids that exist. Verified 2026-09-30.
    """
    index, calls, after = {}, 0, None
    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after
        result = hubspot._get("marketing/v3/marketing-events", params)
        calls += 1
        if not isinstance(result, dict) or result.get("error"):
            return index, calls, (result or {}).get("error", "unknown error")
        for record in result.get("results") or []:
            key = record.get("externalEventId")
            if key:
                index[str(key)] = record
        after = ((result.get("paging") or {}).get("next") or {}).get("after")
        if not after or calls > 50:
            return index, calls, None
