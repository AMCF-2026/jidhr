"""
Jidhr Content Report
====================
On-demand chat command that summarizes recent posting history from
content_history: top topics with per-channel breakdown, quiet topics
worth revisiting, channel cadence, and slot collisions.

Pure read + format — no LLM calls, no writes. Backed by
content.content_analysis (V5.2a query layer).

Phrasing follows the house marketing philosophy: post more, not less.
"Quiet lately" topics are framed as opportunities, not warnings.
"""

import logging
import re
from datetime import date, datetime

from content.content_analysis import (
    get_posting_activity,
    get_topic_frequency,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Trigger phrases (exact substring matches)
# ---------------------------------------------------------------------------

CONTENT_REPORT_PHRASES = [
    'content report',
    'what have we been posting',
    'what have we posted',
    'posting report',
    'content gaps',
    'topic report',
]


# ---------------------------------------------------------------------------
# Channel display map
# ---------------------------------------------------------------------------

_CHANNEL_ABBR = {
    "facebook":        "FB",
    "instagram":       "IG",
    "linkedin":        "LI",
    "youtube":         "YT",
    "twitter":         "TW",
    "giving_circle":   "GC",
    "amcf_newsletter": "NL",
    "email":           "EM",
    "unknown":         "?",
}

# Recency thresholds
_QUIET_TOPIC_DAYS = 21
_QUIET_CHANNEL_DAYS = 14
_DEFAULT_WINDOW_DAYS = 42
_TOP_N = 10


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, **kwargs) -> bool:
    """Check if query is a content-report command."""
    q = query.lower().strip()
    return any(p in q for p in CONTENT_REPORT_PHRASES)


def handle(query: str, assistant) -> str:
    """
    Build a content report for a recent window.

    Args:
        query: The user's message (parsed for an optional N day(s)/week(s))
        assistant: JidhrAssistant instance (not used directly, but keeps
                   the interface consistent across all intent modules)

    Returns:
        Formatted markdown summary string
    """
    days = _parse_window(query, default=_DEFAULT_WINDOW_DAYS)
    logger.info(f"Running content report — last {days} days")

    try:
        topics = get_topic_frequency(days=days)
        activity = get_posting_activity(days=days)
    except Exception as e:
        logger.error(f"Content report error: {e}")
        return f"❌ Content report failed: {e}"

    if not topics and not activity:
        return (
            f"📭 No content found in the last {days} days.\n\n"
            "Try expanding the window (e.g. *\"content report 90 days\"*) "
            "or run *\"sync social\"* / *\"sync emails\"* if the capture "
            "is behind."
        )

    return _format_report(days, topics, activity)


# ---------------------------------------------------------------------------
# Window parsing
# ---------------------------------------------------------------------------

_WINDOW_RE = re.compile(r"(\d+)\s*(day|week)s?", re.IGNORECASE)


def _parse_window(query: str, default: int) -> int:
    """Extract a window like '90 days' or '8 weeks' from the query."""
    m = _WINDOW_RE.search(query)
    if not m:
        return default
    n = int(m.group(1))
    unit = m.group(2).lower()
    return n * 7 if unit == "week" else n


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------

def _abbr_channel(c) -> str:
    if not isinstance(c, str) or not c:
        return "?"
    return _CHANNEL_ABBR.get(c.lower(), c[:2].upper())


def _days_since_dt(dt) -> int | None:
    if not isinstance(dt, datetime):
        return None
    return (datetime.utcnow() - dt).days


def _days_since_date(d) -> int | None:
    if not isinstance(d, date) or isinstance(d, datetime):
        # `date` is the parent of `datetime`; reject datetime here.
        return None
    return (date.today() - d).days


def _format_topic_line(entry: dict) -> str:
    per_channel = entry.get("per_channel") or {}
    channel_pairs = sorted(per_channel.items(), key=lambda kv: (-kv[1], kv[0]))
    channel_str = ", ".join(f"{_abbr_channel(c)} {n}" for c, n in channel_pairs) \
        or "no channels"

    days_ago = _days_since_dt(entry.get("last_sent_at"))
    last_str = f"{days_ago}d ago" if days_ago is not None else "?"

    return (
        f"• **{entry.get('topic', '?')}** — "
        f"{entry.get('total', 0)}× — "
        f"{channel_str} — last {last_str}"
    )


def _format_report(days: int, topics: list[dict], activity: list[dict]) -> str:
    lines = [f"📊 **Content Report** — last {days} days\n"]

    # --- Top topics ---
    top = topics[:_TOP_N]
    if top:
        lines.append(f"**Top {len(top)} topics**\n")
        for entry in top:
            lines.append(_format_topic_line(entry))
        lines.append("")
    else:
        lines.append("_No topics found in this window._\n")

    # --- Quiet topics (opportunities) ---
    quiet = [
        e for e in top
        if (d := _days_since_dt(e.get("last_sent_at"))) is not None
        and d > _QUIET_TOPIC_DAYS
    ]
    if quiet:
        lines.append(
            f"💡 **Quiet lately ({_QUIET_TOPIC_DAYS}+ days)** — "
            "good candidates to revisit:\n"
        )
        for entry in quiet:
            d = _days_since_dt(entry["last_sent_at"])
            lines.append(f"• *{entry.get('topic', '?')}* — last posted {d}d ago")
        lines.append("")

    # --- Channel activity (social posts) ---
    daily_rows = [r for r in activity if r.get("kind") == "daily"]
    if daily_rows:
        channel_totals: dict[str, int] = {}
        channel_last_date: dict[str, date] = {}
        for r in daily_rows:
            ch = r.get("channel") or "unknown"
            channel_totals[ch] = channel_totals.get(ch, 0) + (r.get("count") or 0)
            d = r.get("date")
            if isinstance(d, date) and (
                ch not in channel_last_date or d > channel_last_date[ch]
            ):
                channel_last_date[ch] = d

        if channel_totals:
            lines.append(f"**Channel activity (social posts, last {days}d)**\n")
            sorted_channels = sorted(
                channel_totals.items(), key=lambda kv: (-kv[1], kv[0])
            )
            quiet_channels = []
            for ch, total in sorted_channels:
                last_d = channel_last_date.get(ch)
                ds = _days_since_date(last_d) if last_d else None
                last_str = f"{ds}d ago" if ds is not None else "?"
                line = f"• **{_abbr_channel(ch)}** ({ch}): {total} post(s) — last {last_str}"
                if ds is not None and ds > _QUIET_CHANNEL_DAYS:
                    quiet_channels.append((ch, ds))
                lines.append(line)
            lines.append("")

            if quiet_channels:
                qc_str = ", ".join(
                    f"{_abbr_channel(c)} ({d}d)" for c, d in quiet_channels
                )
                lines.append(
                    f"💡 No posts in the last {_QUIET_CHANNEL_DAYS}+ days on: "
                    f"{qc_str} — room to refresh.\n"
                )

    # --- Slot collisions (only if present) ---
    collisions = [r for r in activity if r.get("kind") == "collision"]
    if collisions:
        lines.append("⚠️ **Slot collisions** (same channel, same exact timestamp):\n")
        for r in collisions:
            ch = _abbr_channel(r.get("channel"))
            ts = r.get("sent_at")
            ts_str = ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else "?"
            lines.append(f"• {ch}: {r.get('count')} posts at {ts_str}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
