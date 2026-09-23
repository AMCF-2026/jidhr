"""
HubSpot Event Enrichment
========================
Turns mirrored CSuite event registrations into HubSpot contact properties,
so that staff-owned HubSpot workflows — not Jidhr — can handle event email.

    python scripts/hubspot_event_enrichment.py            # dry run
    python scripts/hubspot_event_enrichment.py --apply    # writes

DRY RUN IS THE DEFAULT AND MAKES NO HUBSPOT WRITE. Every write in this
script — creating the property group, creating the five properties,
upserting contacts — sits behind `--apply`. Without it the script reads
HubSpot, reads the mirror, prints the plan, writes a markdown diff to
reports/, and stops.

What it reads
    csuite_mirror `event_registration` rows (see sync/mirror.py and the
    2026-09-23 entries in docs/DECISIONS.md) and `event` rows for names
    and dates. No CSuite call: the mirror is the source.

What it would write, all in the "AMCF Events" group
    amcf_event_registrations   multi-checkbox, one option per event date
    amcf_event_attended        multi-checkbox, same option set
    amcf_last_event_registered date
    amcf_last_event_attended   date
    amcf_event_count           number

Rules that are not obvious
    * Contacts are matched on normalised email (trimmed, lowercased).
      A registration with no email is counted and skipped — never
      matched by name, because two people share a name more often than
      a mailing list can afford.
    * An existing contact has ONLY those five properties set. firstname
      and lastname are never touched; a registrant list's spelling of a
      name is not better than what HubSpot already holds.
    * The two date properties are never lowered: the value written is
      max(existing, computed). The same idea applies to the checkbox
      sets, which are unioned with whatever is already ticked rather
      than replaced — see the 2026-09-23 DECISIONS entry.
    * A NEW contact gets firstname/lastname from event_profile_name,
      which CSuite writes "Last, First". A comma decides the split; with
      no comma the last space does, and a single token goes to firstname
      alone.
    * --exclude-emails names addresses this run must not touch. They are
      counted in the report and never planned for a write.

Exit codes: 0 · 1 bad arguments or a HubSpot read failed · 2 mirror not
loaded · 3 the option set breaks one of HubSpot's documented enumeration
limits.
"""

import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

# Make the repo root importable when run as
# `python scripts/hubspot_event_enrichment.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients import mirror_read  # noqa: E402
from clients.hubspot import HubSpotClient  # noqa: E402
from scripts.probe_apis import mask_email  # noqa: E402

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_NOT_LOADED = 2
EXIT_TOO_MANY_OPTIONS = 3

# HubSpot's documented limits on an enumeration property, from
# knowledge.hubspot.com/properties/property-field-types-in-hubspot
# (read 2026-09-23):
#
#   "Each option has a maximum of 3,000 characters, including its
#    label, value, and description."
#   "The maximum size of the property is 512,000 bytes, or 5,000
#    options, whichever is reached first."
#
# Three ceilings, not one, and the first is a budget SHARED between the
# three fields of an option rather than a cap on the label. An earlier
# version of this script assumed 1,000 options and a 255-character
# label; both were guesses and both were wrong, which is why the plan
# now reports every count against its documented limit instead of
# quietly fitting inside one.
HUBSPOT_ENUM_OPTION_LIMIT = 5000
HUBSPOT_ENUM_OPTION_CHARS = 3000
HUBSPOT_ENUM_PROPERTY_BYTES = 512_000

# The group and the five properties. Names are lowercase with
# underscores because HubSpot lowercases property names anyway, and a
# name that differs from what you asked for is a name you cannot find.
PROPERTY_GROUP = {"name": "amcf_events", "label": "AMCF Events"}

REGISTRATIONS_PROPERTY = "amcf_event_registrations"
ATTENDED_PROPERTY = "amcf_event_attended"
LAST_REGISTERED_PROPERTY = "amcf_last_event_registered"
LAST_ATTENDED_PROPERTY = "amcf_last_event_attended"
COUNT_PROPERTY = "amcf_event_count"

PLANNED_PROPERTIES = (REGISTRATIONS_PROPERTY, ATTENDED_PROPERTY,
                      LAST_REGISTERED_PROPERTY, LAST_ATTENDED_PROPERTY,
                      COUNT_PROPERTY)

# The word list step 1 searches the portal's existing schema for. A
# property Lisa already built is a property this script must not
# duplicate, so the search is deliberately wide and the answer is
# reported in full rather than filtered down to "no exact collision".
COLLISION_WORDS = ("event", "webinar", "symposium", "attend")

# CSuite marks a registrant as checked in with 1. See DECISIONS.md,
# 2026-09-23: this is stored as given and never inferred from rsvp.
ATTENDED = 1

# How many planned updates the report shows in full, masked.
PREVIEW_ROWS = 10

# HubSpot's cap on inputs per batch call.
BATCH_SIZE = HubSpotClient.CONTACT_BATCH_SIZE


# ---------------------------------------------------------------------------
# Small conversions
# ---------------------------------------------------------------------------

def normalize_email(value) -> str | None:
    """A comparable email address, or None.

    Trimmed and lowercased — nothing else. No alias stripping, no dot
    folding: "j.smith@gmail.com" and "jsmith@gmail.com" are the same
    inbox at Google and different addresses everywhere else, and a
    mailing list is not the place to guess.
    """
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if not text or "@" not in text:
        return None
    return text


def split_name(full) -> tuple:
    """("first", "last") from a combined name, for NEW contacts only.

    A comma decides the split, because CSuite writes registrant names
    "Last, First": 317 of the 363 registrant rows read on 2026-09-23
    carry a comma and every one of them is that way round. Splitting
    those on the last space, as originally specified, would have created
    fifty contacts with their names reversed and a comma stuck on the
    front name ("Aucoin," / "Alix").

    The last-space rule is kept for the 46 names with no comma, so
    "Mary Anne Fitzgerald" becomes ("Mary Anne", "Fitzgerald") and a
    single token is a first name with no last name. Most of those 46 are
    organisations — "Islamic Food Bank of Toledo" — which no split
    serves; see looks_like_an_organisation, which counts them for the
    report rather than guessing.
    """
    text = " ".join(str(full or "").split())
    if not text:
        return "", ""

    if "," in text:
        last, _, first = text.partition(",")
        last, first = last.strip(), first.strip()
        if last and first:
            return first, last
        return first or last, ""

    first, _, last = text.rpartition(" ")
    if not first:
        return last, ""
    return first, last


def looks_like_an_organisation(full) -> bool:
    """A multi-word name with no comma — probably not a person.

    Not a judgement about the registrant, just the shape CSuite's
    organisation registrants come in. Used only to put a number in the
    report, never to change what is written.
    """
    text = " ".join(str(full or "").split())
    return "," not in text and len(text.split()) > 1


def as_date(value) -> str | None:
    """Any date shape this app meets, as "YYYY-MM-DD", or None.

    Accepts a date/datetime, ISO text with or without a time, and epoch
    milliseconds — which is what HubSpot hands back for a date property.
    """
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        number = int(text)
        # Under 10^11 is seconds, not milliseconds — same rule as
        # clients/mirror_read._to_datetime.
        seconds = number / 1000.0 if abs(number) >= 1e11 else float(number)
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc).date(
            ).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def label_budget(value: str, description: str = "") -> int:
    """How many characters are left for an option's label.

    HubSpot's 3,000 is a budget for the whole option — label, value and
    description together — so the label's own ceiling depends on what
    the other two cost. Computed rather than assumed; these options
    carry no description today, and a description added later shrinks
    the label automatically instead of pushing the option over.
    """
    return max(0, HUBSPOT_ENUM_OPTION_CHARS - len(str(value))
               - len(str(description)))


def fit_label(text: str, suffix: str, budget: int) -> str:
    """`text` + `suffix`, trimmed to fit `budget` characters.

    The suffix is what makes a label unique, so it survives and the
    title is what gives way.
    """
    suffix = suffix or ""
    room = budget - len(suffix)
    if room <= 0:
        return suffix[:budget]
    if len(text) <= room:
        return text + suffix
    return text[:room - 1] + "…" + suffix


def option_size(option: dict) -> int:
    """An option's cost against the 3,000-character per-option budget."""
    return (len(option.get("label") or "") + len(str(option.get("value") or ""))
            + len(option.get("description") or ""))


def property_bytes(options: list) -> int:
    """Roughly what the option set costs against the 512,000-byte cap.

    Counted as the UTF-8 length of every label, value and description,
    which is the part that grows with the catalogue. HubSpot's own
    accounting includes the property's other fields, so this is a floor,
    and the plan says so rather than implying a precise margin.
    """
    return sum(
        len((option.get("label") or "").encode("utf-8"))
        + len(str(option.get("value") or "").encode("utf-8"))
        + len((option.get("description") or "").encode("utf-8"))
        for option in options)


# ---------------------------------------------------------------------------
# The option set
# ---------------------------------------------------------------------------

def event_title(event_row: dict) -> str:
    """The human title of an event date.

    NOT `event_name`. `event_name` belongs to the parent event_id, which
    is a series, and across all 179 event dates it takes exactly three
    values — "Unassigned" (98), "Event - Other" (80) and "Newsletters"
    (1). A checkbox list of nineteen options all labelled
    "Event - Other" is a list nobody can build a workflow from, which is
    the same failure as a search result with no usable id.

    The title CSuite actually holds is `event_description`: "AMCF Open
    House", "AMCF x ISPU Webinar: Political Participation…". Free text,
    sometimes multi-line, so it is collapsed to one line here.
    """
    event_row = event_row or {}
    for field in ("event_description", "event_name"):
        text = " ".join(str(event_row.get(field) or "").split())
        if text:
            return text
    return ""


def event_option(event_date_id: str, event_row: dict,
                 disambiguator: str = "") -> dict:
    """One multi-checkbox option for one event date.

    The internal value is the event_date_id — stable, and the same key
    the mirror uses. The label is what a staff member reads when
    building a workflow, so it carries the title and the date, and says
    so plainly when there is no date: 98 of 179 event dates carry none
    (DECISIONS.md, 2026-09-23).
    """
    event_row = event_row or {}
    value = str(event_date_id)
    name = event_title(event_row) or f"Event date {event_date_id}"
    day = as_date(event_row.get("event_date"))
    text = f"{name} — {day}" if day else f"{name} — undated (id {value})"
    return {"label": fit_label(text, disambiguator,
                               label_budget(value)),
            "value": value}


def build_options(registrations: list, events: dict,
                  excluded_emails=None) -> list:
    """An option per event date that has at least one registrant this run
    could actually write — a date nobody reachable attended would be an
    option no workflow could ever use, and neither would a date whose
    only reachable registrant the operator excluded.

    Sorted newest date first, undated last, so the list a staff member
    scrolls opens on this season's events.

    Labels are made UNIQUE. Two event dates can carry the same
    description on the same day — a session run twice, or a series
    copied — and two identical ticks in a checkbox list is a workflow
    built against the wrong one. When that happens both labels get
    " (id <event_date_id>)", not just the second: a bare label beside a
    suffixed one reads as the "real" one, and neither of them is.
    """
    excluded_emails = excluded_emails or set()
    plannable = set()
    for row in registrations:
        email = normalize_email(row.get("event_profile_email"))
        if not email or email in excluded_emails:
            continue
        if row.get("event_date_id") in (None, ""):
            continue
        plannable.add(str(row.get("event_date_id")))

    # Pass one: the label each event date wants, before uniqueness.
    wanted = {}
    for event_date_id in plannable:
        event_row = events.get(event_date_id) or {}
        wanted[event_date_id] = (event_option(event_date_id, event_row),
                                 as_date(event_row.get("event_date")))

    repeated = {label for label, count in Counter(
        option["label"] for option, _ in wanted.values()).items() if count > 1}

    dated, undated = [], []
    for event_date_id, (option, day) in wanted.items():
        if option["label"] in repeated:
            option = event_option(event_date_id, events.get(event_date_id),
                                  disambiguator=f" (id {event_date_id})")
        (dated if day else undated).append((day, option))

    dated.sort(key=lambda pair: (pair[0], pair[1]["label"]), reverse=True)
    undated.sort(key=lambda pair: pair[1]["label"])
    return [option for _, option in dated] + \
           [option for _, option in undated]


def over_limit(options: list) -> str | None:
    """The documented ceiling this option set breaks, or None.

    All three are checked, because the one that bites is never the one
    you were watching: an option count well inside 5,000 can still blow
    the 512,000-byte property size if the labels are long.
    """
    if len(options) > HUBSPOT_ENUM_OPTION_LIMIT:
        return (f"{len(options):,} options needed but HubSpot allows "
                f"{HUBSPOT_ENUM_OPTION_LIMIT:,} per property")

    oversized = [option for option in options
                 if option_size(option) > HUBSPOT_ENUM_OPTION_CHARS]
    if oversized:
        return (f"{len(oversized)} option(s) exceed HubSpot's "
                f"{HUBSPOT_ENUM_OPTION_CHARS:,}-character per-option budget "
                f"(label + value + description), the largest at "
                f"{max(option_size(o) for o in oversized):,}")

    size = property_bytes(options)
    if size > HUBSPOT_ENUM_PROPERTY_BYTES:
        return (f"the option set is {size:,} bytes and HubSpot allows "
                f"{HUBSPOT_ENUM_PROPERTY_BYTES:,} per property")
    return None


def property_definitions(options: list) -> list:
    """The five property definitions, in HubSpot's own shape."""
    return [
        {"name": REGISTRATIONS_PROPERTY,
         "label": "AMCF events registered",
         "description": "Every AMCF event date this contact registered "
                        "for, from CSuite. Filled by Jidhr; do not edit "
                        "by hand.",
         "groupName": PROPERTY_GROUP["name"],
         "type": "enumeration", "fieldType": "checkbox",
         "options": [dict(option, displayOrder=index)
                     for index, option in enumerate(options)]},
        {"name": ATTENDED_PROPERTY,
         "label": "AMCF events attended",
         "description": "Every AMCF event date this contact was checked "
                        "in at, from CSuite. Attendance is recorded at "
                        "some events and not others — an empty value "
                        "means no check-in was recorded, not that the "
                        "contact stayed away.",
         "groupName": PROPERTY_GROUP["name"],
         "type": "enumeration", "fieldType": "checkbox",
         "options": [dict(option, displayOrder=index)
                     for index, option in enumerate(options)]},
        {"name": LAST_REGISTERED_PROPERTY,
         "label": "Last AMCF event registered",
         "description": "Date of the most recent AMCF event this contact "
                        "registered for. Blank if every one of them is "
                        "undated in CSuite.",
         "groupName": PROPERTY_GROUP["name"],
         "type": "date", "fieldType": "date"},
        {"name": LAST_ATTENDED_PROPERTY,
         "label": "Last AMCF event attended",
         "description": "Date of the most recent AMCF event this contact "
                        "was checked in at.",
         "groupName": PROPERTY_GROUP["name"],
         "type": "date", "fieldType": "date"},
        {"name": COUNT_PROPERTY,
         "label": "AMCF event registrations",
         "description": "How many AMCF event dates this contact has "
                        "registered for.",
         "groupName": PROPERTY_GROUP["name"],
         "type": "number", "fieldType": "number"},
    ]


# ---------------------------------------------------------------------------
# Grouping registrations into contacts
# ---------------------------------------------------------------------------

def read_exclusions(path) -> tuple:
    """(addresses to leave alone, stats) from an --exclude-emails file.

    One address per line; blank lines and lines beginning with # are
    ignored, and every address is put through normalize_email so the
    file matches the same way the mirror rows do. A line that is not an
    address is counted and reported rather than silently dropped — an
    operator who mistypes an address in an exclusion list has excluded
    nobody, and that is exactly the failure this file exists to prevent.
    """
    if not path:
        return set(), {"lines": 0, "addresses": 0, "unreadable_lines": []}

    excluded, unreadable, lines = set(), [], 0
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            text = raw.strip()
            if not text or text.startswith("#"):
                continue
            lines += 1
            email = normalize_email(text)
            if email is None:
                unreadable.append(text)
            else:
                excluded.add(email)
    return excluded, {"lines": lines, "addresses": len(excluded),
                      "unreadable_lines": unreadable}


def group_by_email(registrations: list, excluded_emails=None) -> tuple:
    """({normalised email: [rows]}, stats).

    stats carries the numbers that are otherwise invisible: rows with no
    email at all, rows the operator excluded, and CSuite profile_ids
    that appear under more than one address. None of them is resolved
    here — a profile with two addresses is two contacts until a person
    says otherwise, and an excluded address is one this run must not
    touch whatever the mirror says about it.
    """
    excluded_emails = excluded_emails or set()
    groups = defaultdict(list)
    emails_by_profile = defaultdict(set)
    no_email = 0
    excluded_rows = 0
    excluded_addresses = set()

    for row in registrations:
        email = normalize_email(row.get("event_profile_email"))
        if email is None:
            no_email += 1
            continue
        if email in excluded_emails:
            excluded_rows += 1
            excluded_addresses.add(email)
            continue
        groups[email].append(row)
        profile_id = row.get("profile_id")
        if profile_id not in (None, ""):
            emails_by_profile[str(profile_id)].add(email)

    ambiguous = {profile_id: sorted(addresses)
                 for profile_id, addresses in emails_by_profile.items()
                 if len(addresses) > 1}

    return dict(groups), {
        "rows_no_email": no_email,
        "excluded_rows": excluded_rows,
        "excluded_addresses": sorted(excluded_addresses),
        "ambiguous_profiles": ambiguous,
        "profiles_seen": len(emails_by_profile),
    }


def contact_plan(email: str, rows: list, events: dict) -> dict:
    """What the mirror says about one email address."""
    registered, attended = set(), set()
    name = ""
    latest_name_date = None

    for row in rows:
        event_date_id = row.get("event_date_id")
        if event_date_id in (None, ""):
            continue
        event_date_id = str(event_date_id)
        registered.add(event_date_id)
        if row.get("attended") == ATTENDED:
            attended.add(event_date_id)

        # The name from the most recent event wins; an undated event is
        # only used when nothing dated has offered one.
        candidate = str(row.get("event_profile_name") or "").strip()
        if candidate:
            day = as_date((events.get(event_date_id) or {}).get("event_date"))
            if not name or (day and (latest_name_date is None
                                     or day > latest_name_date)):
                name, latest_name_date = candidate, day or latest_name_date

    def newest(event_date_ids):
        days = [as_date((events.get(i) or {}).get("event_date"))
                for i in event_date_ids]
        days = [day for day in days if day]
        return max(days) if days else None

    return {
        "email": email,
        "name": name,
        "registered": sorted(registered, key=lambda i: (len(i), i)),
        "attended": sorted(attended, key=lambda i: (len(i), i)),
        "last_registered": newest(registered),
        "last_attended": newest(attended),
        "count": len(registered),
    }


# ---------------------------------------------------------------------------
# Merging against what HubSpot already holds
# ---------------------------------------------------------------------------

def parse_checkbox(value) -> list:
    """HubSpot's ";"-joined multi-checkbox value as a list."""
    if value in (None, ""):
        return []
    return [part for part in str(value).split(";") if part]


def merge_checkbox(existing, computed) -> str:
    """Existing ticks UNIONED with the computed ones, never replaced.

    A value already ticked was ticked by someone, and this script does
    not know who or why. The cost is that a registration cancelled in
    CSuite leaves its tick behind; the alternative cost is silently
    clearing a staff member's work, which is worse and harder to notice.
    """
    merged = list(parse_checkbox(existing))
    for value in computed:
        if value not in merged:
            merged.append(value)
    return ";".join(merged)


def merge_date(existing, computed) -> str | None:
    """max(existing, computed) — the date properties never move backwards."""
    existing_day, computed_day = as_date(existing), as_date(computed)
    if existing_day and computed_day:
        return max(existing_day, computed_day)
    return existing_day or computed_day


def properties_for_existing(plan: dict, contact_properties: dict) -> dict:
    """The five properties for a contact HubSpot already holds.

    Only these five. firstname, lastname and everything else are absent
    from the payload, which is what keeps them untouched: HubSpot only
    changes a property the request names.
    """
    current = contact_properties or {}
    properties = {
        REGISTRATIONS_PROPERTY: merge_checkbox(
            current.get(REGISTRATIONS_PROPERTY), plan["registered"]),
        ATTENDED_PROPERTY: merge_checkbox(
            current.get(ATTENDED_PROPERTY), plan["attended"]),
        COUNT_PROPERTY: str(plan["count"]),
    }
    for name, computed in ((LAST_REGISTERED_PROPERTY, plan["last_registered"]),
                           (LAST_ATTENDED_PROPERTY, plan["last_attended"])):
        merged = merge_date(current.get(name), computed)
        if merged is not None:
            properties[name] = merged
    return properties


def properties_for_new(plan: dict) -> dict:
    """The five properties plus a name split, for a contact to be created."""
    first, last = split_name(plan["name"])
    properties = {
        REGISTRATIONS_PROPERTY: ";".join(plan["registered"]),
        ATTENDED_PROPERTY: ";".join(plan["attended"]),
        COUNT_PROPERTY: str(plan["count"]),
    }
    if plan["last_registered"]:
        properties[LAST_REGISTERED_PROPERTY] = plan["last_registered"]
    if plan["last_attended"]:
        properties[LAST_ATTENDED_PROPERTY] = plan["last_attended"]
    if first:
        properties["firstname"] = first
    if last:
        properties["lastname"] = last
    return properties


def changed(before: dict, after: dict) -> dict:
    """{property: (before, after)} for the properties that would move."""
    moved = {}
    for name, new_value in after.items():
        old_value = (before or {}).get(name)
        if str(old_value or "") != str(new_value or ""):
            moved[name] = (old_value, new_value)
    return moved


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def read_mirror() -> tuple:
    """(registration rows, {event_date_id: event row}) from the mirror."""
    registrations = mirror_read.rows("event_registration")
    events = {str(row.get("csuite_id")): row
              for row in mirror_read.rows("event")}
    return registrations, events


def existing_properties(client) -> tuple:
    """(everything the portal defines, the ones worth a second look).

    Returns (by_name, collisions, error). A collision here is not an
    error — it is the list a person reads before agreeing to five more
    properties.
    """
    result = client.get_contact_properties()
    if not isinstance(result, dict) or result.get("error"):
        return {}, [], (result or {}).get("error", "unknown error")

    by_name = {}
    collisions = []
    for prop in result.get("results") or []:
        name = prop.get("name")
        if not name:
            continue
        by_name[name] = prop
        haystack = f"{name} {prop.get('label') or ''}".lower()
        if any(word in haystack for word in COLLISION_WORDS):
            collisions.append(prop)
    collisions.sort(key=lambda p: p.get("name") or "")
    return by_name, collisions, None


def fetch_contacts(client, emails: list) -> tuple:
    """({normalised email: contact}, missing, error).

    One batch read per 100 addresses. An address HubSpot does not hold
    comes back under "errors" and simply does not appear in the result,
    so "missing" is computed by difference rather than parsed out of an
    error shape that HubSpot is free to change.
    """
    found = {}
    wanted = list(emails)
    properties = list(PLANNED_PROPERTIES) + ["email", "firstname", "lastname"]

    for start in range(0, len(wanted), BATCH_SIZE):
        batch = wanted[start:start + BATCH_SIZE]
        result = client.batch_read_contacts_by_email(batch, properties)
        if not isinstance(result, dict) or result.get("error"):
            return found, [], (result or {}).get("error", "unknown error")
        for contact in result.get("results") or []:
            props = contact.get("properties") or {}
            email = normalize_email(props.get("email"))
            if email:
                found[email] = contact

    missing = [email for email in wanted if email not in found]
    return found, missing, None


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------

def build_plan(registrations: list, events: dict, contacts: dict,
               excluded_emails=None) -> dict:
    """Everything the report and the apply step both need.

    An excluded address never reaches `plans`, so it cannot reach
    `updates` or `creates` either, and therefore cannot reach a write.
    The exclusion happens once, at the grouping step, rather than being
    re-checked at each stage — one gate is testable, four are a place
    for one of them to be forgotten.
    """
    groups, stats = group_by_email(registrations, excluded_emails)
    plans = [contact_plan(email, rows, events)
             for email, rows in sorted(groups.items())]

    updates, creates, unchanged = [], [], []
    for plan in plans:
        contact = contacts.get(plan["email"])
        if contact is None:
            creates.append({"plan": plan,
                            "properties": properties_for_new(plan)})
            continue
        before = contact.get("properties") or {}
        after = properties_for_existing(plan, before)
        moved = changed(before, after)
        record = {"plan": plan, "contact_id": contact.get("id"),
                  "properties": after, "before": before, "changed": moved}
        (updates if moved else unchanged).append(record)

    return {"plans": plans, "updates": updates, "creates": creates,
            "unchanged": unchanged, "stats": stats}


def batches(items, size: int = BATCH_SIZE):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def upsert_inputs(records: list) -> list:
    """Batch-upsert inputs, keyed by email (idProperty=email)."""
    return [{"id": record["plan"]["email"], "properties": record["properties"]}
            for record in records]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render(plan: dict, options: list, collisions: list, existing_names: set,
           marketing: dict, when: str, rows_read: int) -> str:
    stats = plan["stats"]
    lines = [
        f"# HubSpot event enrichment — dry run {when}",
        "",
        "Source: `csuite_mirror` event_registration rows. "
        "Destination: HubSpot contact properties in the **AMCF Events** "
        "group. No HubSpot write was made to produce this file.",
        "",
        "## Totals",
        "",
        "| | |",
        "|---|---|",
        f"| registration rows read | {rows_read:,} |",
        f"| contacts matched (already in HubSpot) | "
        f"{len(plan['updates']) + len(plan['unchanged'])} |",
        f"| …of those, would change | {len(plan['updates'])} |",
        f"| …of those, already correct | {len(plan['unchanged'])} |",
        f"| contacts to create | **{len(plan['creates'])}** |",
        f"| rows skipped, no email | {stats['rows_no_email']} |",
        f"| rows excluded by operator | {stats['excluded_rows']} |",
        f"| …distinct addresses excluded | "
        f"{len(stats['excluded_addresses'])} |",
        f"| CSuite profiles under more than one email | "
        f"{len(stats['ambiguous_profiles'])} |",
        "",
        "## Option set",
        "",
        "Every documented ceiling, from "
        "knowledge.hubspot.com/properties/property-field-types-in-hubspot:",
        "",
        "| | needed | HubSpot allows |",
        "|---|---|---|",
        f"| options | **{len(options):,}** | {HUBSPOT_ENUM_OPTION_LIMIT:,} |",
        f"| largest option (label + value + description) | "
        f"{max((option_size(o) for o in options), default=0):,} chars | "
        f"{HUBSPOT_ENUM_OPTION_CHARS:,} chars |",
        f"| option set size | {property_bytes(options):,} bytes | "
        f"{HUBSPOT_ENUM_PROPERTY_BYTES:,} bytes |",
        "",
        "One option per event date with at least one registrant who has "
        "an email and was not excluded; the same set is used by both "
        "checkbox properties. Labels are unique — where two event dates "
        "would read identically, both carry their id.",
        "",
    ]

    duplicated = sorted(
        option["label"] for option in options if " (id " in option["label"]
        and "undated (id " not in option["label"])
    if duplicated:
        lines += [f"{len(duplicated)} label(s) needed an id to stay "
                  "distinct:", ""]
        for label in duplicated[:10]:
            lines.append(f"* {label}")
        lines.append("")

    if options:
        lines += ["First five options:", ""]
        for option in options[:5]:
            lines.append(f"* `{option['value']}` — {option['label']}")
        lines.append("")

    if stats["excluded_addresses"]:
        lines += ["## Excluded by operator", "",
                  "Named in `--exclude-emails`. Counted, never created and "
                  "never updated.", ""]
        for email in stats["excluded_addresses"]:
            lines.append(f"* {mask_email(email)}")
        lines.append("")

    lines += ["## Properties this plan would create", ""]
    for name in PLANNED_PROPERTIES:
        state = "ALREADY EXISTS" if name in existing_names else "new"
        lines.append(f"* `{name}` — {state}")
    lines += ["", "## Existing properties that mention "
              + ", ".join(COLLISION_WORDS), ""]
    if not collisions:
        lines.append("None.")
    else:
        lines.append("| property | label | type | group | HubSpot-defined |")
        lines.append("|---|---|---|---|---|")
        for prop in collisions:
            lines.append(
                f"| `{prop.get('name')}` | {prop.get('label')} | "
                f"{prop.get('type')}/{prop.get('fieldType')} | "
                f"{prop.get('groupName')} | "
                f"{'yes' if prop.get('hubspotDefined') else 'no'} |")
    lines.append("")

    organisations = sum(1 for record in plan["creates"]
                        if looks_like_an_organisation(record["plan"]["name"]))
    lines += ["## New contacts and the marketing-contact setting", "",
              f"This plan would create **{len(plan['creates'])}** contacts.",
              ""]
    if organisations:
        lines += [
            f"* {organisations} of them have a name with no comma in it — "
            "CSuite writes people as \"Last, First\", so these are most "
            "likely organisations (\"Islamic Food Bank of Toledo\"). They "
            "are still split on the last space, which serves an "
            "organisation badly. Worth a decision before `--apply`.",
        ]
    for line in marketing.get("notes", []):
        lines.append(f"* {line}")
    lines.append("")

    if stats["ambiguous_profiles"]:
        lines += ["## CSuite profiles under more than one email", "",
                  "Not merged. Each address is planned as its own contact.",
                  ""]
        for profile_id, addresses in sorted(
                stats["ambiguous_profiles"].items())[:20]:
            masked = ", ".join(mask_email(a) for a in addresses)
            lines.append(f"* profile {profile_id}: {masked}")
        if len(stats["ambiguous_profiles"]) > 20:
            lines.append(f"* … {len(stats['ambiguous_profiles']) - 20} more")
        lines.append("")

    lines += [f"## First {PREVIEW_ROWS} planned updates (existing contacts)",
              ""]
    if not plan["updates"]:
        lines.append("None — every matched contact is already correct.")
    else:
        lines.append("| contact | property | before | after |")
        lines.append("|---|---|---|---|")
        for record in plan["updates"][:PREVIEW_ROWS]:
            masked = mask_email(record["plan"]["email"])
            for name, (before, after) in sorted(record["changed"].items()):
                lines.append(
                    f"| {masked} | `{name}` | {before or '(empty)'} | "
                    f"{after or '(empty)'} |")
    lines.append("")

    lines += [f"## All {len(plan['creates'])} planned creates", ""]
    if not plan["creates"]:
        lines.append("None.")
    else:
        lines.append("| email | firstname | lastname | as CSuite has it | "
                     "events | attended |")
        lines.append("|---|---|---|---|---|---|")
        for record in plan["creates"]:
            props = record["properties"]
            lines.append(
                f"| {mask_email(record['plan']['email'])} | "
                f"{props.get('firstname', '')} | "
                f"{props.get('lastname', '')} | "
                f"{record['plan']['name']} | "
                f"{record['plan']['count']} | "
                f"{len(record['plan']['attended'])} |")
    lines += ["", f"_{mirror_read.as_of_line('event_registration', 'event')}_",
              ""]
    return "\n".join(lines)


def summarise(plan: dict, options: list) -> str:
    stats = plan["stats"]
    return "\n".join([
        "HubSpot event enrichment — DRY RUN, no writes",
        f"  contacts matched      {len(plan['updates']) + len(plan['unchanged']):>6,}"
        f"   ({len(plan['updates']):,} would change, "
        f"{len(plan['unchanged']):,} already correct)",
        f"  contacts to create    {len(plan['creates']):>6,}",
        f"  skipped, no email     {stats['rows_no_email']:>6,} registration row(s)",
        f"  excluded by operator  {stats['excluded_rows']:>6,} registration "
        f"row(s) across {len(stats['excluded_addresses']):,} address(es)",
        f"  ambiguous profiles    {len(stats['ambiguous_profiles']):>6,}"
        "   (one CSuite profile, several emails — not merged)",
        f"  options needed        {len(options):>6,}   of "
        f"{HUBSPOT_ENUM_OPTION_LIMIT:,} allowed",
    ])


# ---------------------------------------------------------------------------
# The write half, which only --apply reaches
# ---------------------------------------------------------------------------

def apply_schema(client, options: list, existing_names: set) -> list:
    """Create the group and any missing property. WRITES."""
    done = []
    result = client.create_contact_property_group(
        PROPERTY_GROUP["name"], PROPERTY_GROUP["label"])
    done.append(("group", PROPERTY_GROUP["name"], result))
    for definition in property_definitions(options):
        if definition["name"] in existing_names:
            continue
        done.append(("property", definition["name"],
                     client.create_contact_property(definition)))
    return done


def apply_contacts(client, plan: dict) -> list:
    """Upsert every planned update and create, 100 at a time. WRITES."""
    results = []
    for records in batches(plan["updates"] + plan["creates"]):
        results.append(client.batch_upsert_contacts(upsert_inputs(records)))
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def default_report_path() -> str:
    return os.path.join(
        "reports", f"hubspot_event_enrichment_{date.today().isoformat()}.md")


def marketing_notes(client) -> dict:
    """What can and cannot be established about marketing contacts.

    The portal toggle "automatically set contacts created by
    integrations as marketing contacts" has no public API, so this
    reports the surrounding facts and names the setting rather than
    guessing at it. A number that decides a bill is not a number to
    infer.
    """
    notes = [
        "HubSpot exposes no API for the portal setting that decides this. "
        "Check **Settings → Data Management → Objects → Contacts → "
        "Marketing contacts** before running `--apply`: if "
        "\"automatically set contacts created by integrations as marketing "
        "contacts\" is on, every contact created here becomes a marketing "
        "contact and counts against the tier.",
    ]

    def total(filters):
        result = client._post("crm/v3/objects/contacts/search",
                              {"filterGroups": [{"filters": filters}],
                               "limit": 1})
        if not isinstance(result, dict) or result.get("error"):
            return None
        return result.get("total")

    marketing_now = total([{"propertyName": "hs_marketable_status",
                            "operator": "EQ", "value": "true"}])
    integration = total([{"propertyName": "hs_object_source",
                          "operator": "EQ", "value": "INTEGRATION"}])
    integration_marketing = total([
        {"propertyName": "hs_object_source", "operator": "EQ",
         "value": "INTEGRATION"},
        {"propertyName": "hs_marketable_status", "operator": "EQ",
         "value": "true"}])

    if marketing_now is not None:
        notes.append(f"The portal currently holds {marketing_now:,} "
                     "marketing contacts.")
    if integration and integration_marketing is not None:
        notes.append(
            f"Of the {integration:,} contacts already created by an "
            f"integration in this portal, {integration_marketing:,} are "
            "marketing contacts — evidence about the setting, not proof: "
            "those contacts came from several integrations and may have "
            "been reclassified since.")
    return {"notes": notes}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan HubSpot contact enrichment from mirrored CSuite "
                    "event registrations. Dry run unless --apply.")
    parser.add_argument("--apply", action="store_true",
                        help="Actually write to HubSpot: create the property "
                             "group and properties, then upsert contacts. "
                             "Without this flag nothing is written.")
    parser.add_argument("--out", default=None,
                        help="Where the markdown diff goes "
                             "(default reports/hubspot_event_enrichment_"
                             "<date>.md).")
    parser.add_argument("--exclude-emails", default=None, metavar="PATH",
                        help="A file of addresses this run must not touch, "
                             "one per line (# comments and blank lines are "
                             "ignored). Matching registrations are counted "
                             "in the report and never created or updated.")
    args = parser.parse_args(argv)

    try:
        excluded, exclusion_stats = read_exclusions(args.exclude_emails)
    except OSError as e:
        print(f"could not read --exclude-emails {args.exclude_emails}: {e}",
              file=sys.stderr)
        return EXIT_FAILED
    for bad in exclusion_stats["unreadable_lines"]:
        print(f"--exclude-emails: {bad!r} is not an email address and "
              "excludes nobody", file=sys.stderr)

    missing = mirror_read.require("event_registration", "event")
    if missing:
        print(missing, file=sys.stderr)
        return EXIT_NOT_LOADED

    registrations, events = read_mirror()
    options = build_options(registrations, events, excluded)

    client = HubSpotClient()
    by_name, collisions, error = existing_properties(client)
    if error:
        print(f"HubSpot property read failed: {error}", file=sys.stderr)
        return EXIT_FAILED

    broken = over_limit(options)
    if broken:
        print(f"{broken}. Stopping rather than truncating: a silently "
              "shortened option list is a workflow that misses people.",
              file=sys.stderr)
        return EXIT_TOO_MANY_OPTIONS

    groups, _ = group_by_email(registrations, excluded)
    contacts, _missing, error = fetch_contacts(client, sorted(groups))
    if error:
        print(f"HubSpot contact read failed: {error}", file=sys.stderr)
        return EXIT_FAILED

    plan = build_plan(registrations, events, contacts, excluded)

    report = render(plan, options, collisions, set(by_name),
                    marketing_notes(client),
                    datetime.now(timezone.utc).date().isoformat(),
                    len(registrations))
    out_path = args.out or default_report_path()
    directory = os.path.dirname(out_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write(report)

    print(summarise(plan, options))
    print(f"\n  report: {out_path}")

    if not args.apply:
        print("\nDRY RUN — nothing was written to HubSpot. "
              "Re-run with --apply to write.")
        return EXIT_OK

    print("\n--apply: writing to HubSpot.")
    for kind, name, result in apply_schema(client, options, set(by_name)):
        status = "error" if (result or {}).get("error") else "ok"
        print(f"  {kind} {name}: {status}")
    for result in apply_contacts(client, plan):
        status = "error" if (result or {}).get("error") else "ok"
        print(f"  batch upsert: {status}")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
