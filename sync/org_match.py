"""Is this organisation already in CSuite? Asked of the mirror, by name.

Why the mirror and not CSuite
-----------------------------
`profile/list` has no name filter: an unknown filter returns the unfiltered
18,797-row table (the V5.80 B6 lesson). Searching by name live would mean
reading every profile on every endowment inquiry. The mirror already holds
every profile's `organization` and `ptype`, so the question costs one local
query. The price is freshness, and the reply always says how old the data is.

Only `ptype = 'org'` rows are compared. 230 individual profiles carry an
`organization` too (an employer), measured 2026-10-14: 2,477 non-empty
`organization` values against 2,247 org profiles. A donor whose employer
shares a name with the inquiring organisation is not that organisation.

Exact and close
---------------
Names are normalised first: lowercased, punctuation removed, "&" read as
"and", and the words in IGNORED_WORDS dropped ("The Peace Foundation, Inc."
and "peace" compare equal).

* EXACT: the normalised names are equal. That is the organisation.
* CLOSE: not equal, but near — a similarity ratio of at least CLOSE_RATIO,
  or every word of the shorter name (two words or more) inside the longer.
  A close match is AMBIGUOUS. It is shown to the reviewer and never linked.
"""

import difflib
import logging
import re

logger = logging.getLogger(__name__)

IGNORED_WORDS = frozenset({"the", "inc", "incorporated", "foundation"})
CLOSE_RATIO = 0.85
MAX_CLOSE_SHOWN = 5


class MirrorUnavailable(RuntimeError):
    """The mirror could not be read, so the name check did not happen."""


def normalise_org_name(name) -> str:
    text = str(name or "").lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return " ".join(w for w in text.split() if w not in IGNORED_WORDS)


def _close(a: str, b: str) -> bool:
    if difflib.SequenceMatcher(None, a, b).ratio() >= CLOSE_RATIO:
        return True
    short, long_ = sorted((a.split(), b.split()), key=len)
    return len(short) >= 2 and set(short) <= set(long_)


def match_org_name(name, org_rows) -> dict:
    """{"exact": [rows], "close": [rows]} for `name` among `org_rows`.

    Each row is a mirror profile dict; the returned rows carry `profile_id`
    and `organization` only.
    """
    wanted = normalise_org_name(name)
    out = {"exact": [], "close": []}
    if not wanted:
        return out
    for row in org_rows or []:
        theirs = normalise_org_name(row.get("organization"))
        if not theirs:
            continue
        slim = {"profile_id": str(row.get("profile_id")
                                  or row.get("csuite_id") or ""),
                "organization": row.get("organization")}
        if theirs == wanted:
            out["exact"].append(slim)
        elif _close(wanted, theirs):
            out["close"].append(slim)
    return out


def mirror_org_rows() -> tuple:
    """(org profile rows, synced_at of the newest profile row). One query
    each. Raises MirrorUnavailable rather than answering "no match"."""
    from clients import mirror_read

    try:
        found = mirror_read.rows("profile", "AND data->>'ptype' = %s",
                                 ("org",), exclude_test=False,
                                 exclude_system=False)
        synced = mirror_read.freshness("profile")
    except Exception as e:
        raise MirrorUnavailable(f"the CSuite mirror could not be read: {e}") \
            from e
    if not found:
        raise MirrorUnavailable(
            "the CSuite mirror holds no organisation profiles — run "
            "mirror_refresh")
    return found, synced


def mirror_age_line(synced, now=None) -> str:
    """"Mirror data from Sep 23, 2026, 3:52 PM ET (21 days old)"."""
    from datetime import datetime, timezone

    from clients import mirror_read

    stamp = mirror_read._to_datetime(synced)
    if stamp is None:
        return "Mirror data age unknown"
    now = now or datetime.now(timezone.utc)
    days = max((now - stamp).days, 0)
    age = "today" if days == 0 else f"{days} day{'' if days == 1 else 's'} old"
    return f"Mirror data from {mirror_read.fmt_ts(stamp)} ({age})"
