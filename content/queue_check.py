"""
Queue Cadence Checks
====================
Cadence rules over the upcoming HubSpot social-broadcast queue.

Carl's two rules:
  1. Never the same topic on multiple networks on the same calendar day
     — rotation must stagger across days.
  2. Never two posts stacked at the identical slot on the same network.

All "same day" reasoning is in America/New_York (zoneinfo) — the portal
default. Pure reads + Python; no LLM calls; no writes.

Matches content_analysis.py's contract: every public function catches
all exceptions, logs ERROR with exc_info, and returns a safe empty
value ([] for list returns, None for single-value returns) — never
raises to callers.
"""

import logging
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# _normalize_for_match is module-level and importable in content_analysis.
# Per spec, reuse it rather than replicate. Aliased to the spec-named
# _normalize_text so callers in this module use the requested name.
from content.content_analysis import _normalize_for_match as _normalize_text

logger = logging.getLogger(__name__)


_ET_TZ = ZoneInfo("America/New_York")

# channelKey prefix → short name. Mirrors the V4.5 client's channel_map
# (in get_published_social_broadcasts_with_content). Unknown prefix
# falls back to lowercase as-is.
_CHANNEL_MAP = {
    "FacebookPage":        "facebook",
    "Instagram":           "instagram",
    "LinkedInCompanyPage": "linkedin",
    "YouTube":             "youtube",
}

# Rule-1 text-similarity threshold. _text_similarity returns Jaccard
# overlap of word sets; >= this value means the two posts likely cover
# the same topic.
_TEXT_SIMILARITY_THRESHOLD = 0.5


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_channel(channel_key) -> str:
    if not isinstance(channel_key, str) or not channel_key:
        return "unknown"
    prefix = channel_key.split(":", 1)[0] if ":" in channel_key else channel_key
    return _CHANNEL_MAP.get(prefix, prefix.lower())


def _strip_trailing_slash(url) -> str:
    if not isinstance(url, str):
        return ""
    return url.rstrip("/")


def _excerpt(s, n: int = 80) -> str:
    if not isinstance(s, str):
        return ""
    cleaned = s.replace("\n", " ").replace("\r", " ").strip()
    return cleaned if len(cleaned) <= n else cleaned[: n - 1] + "…"


def _ms_to_utc(ms):
    """Convert epoch-ms to a tz-aware UTC datetime, or None on failure."""
    if not isinstance(ms, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def _text_similarity(a, b) -> float:
    """Overlap coefficient: |A ∩ B| / min(|A|, |B|) over word sets,
    post-_normalize_text. Returns a float in [0, 1].

    Why overlap, not Jaccard: rule 1 compares a SHORT proposed draft
    against potentially LONGER stored queue bodies. Jaccard
    (|A ∩ B| / |A ∪ B|) is dominated by the larger set's size — a
    13-word campaign phrase fully contained in a 50-word stored post
    scored 0.19–0.39 in production, below the 0.5 threshold, so rule 1
    never fired. Overlap-over-smaller-set correctly registers near 1.0
    in that case because the smaller set's words are mostly present in
    the larger one.

    Safeguards:
      - either normalized set has fewer than 4 words → 0.0, since
        otherwise any two 2-word snippets sharing one word would hit
        0.5+ and over-fire rule 1;
      - min(|A|, |B|) == 0 → 0.0 (defensive; unreachable given the
        4-word check above, but documents intent).

    Rule 1's threshold (_TEXT_SIMILARITY_THRESHOLD = 0.5) is unchanged.
    """
    na = _normalize_text(a)
    nb = _normalize_text(b)
    if not na or not nb:
        return 0.0
    sa = set(na.split())
    sb = set(nb.split())
    if len(sa) < 4 or len(sb) < 4:
        return 0.0
    smaller = min(len(sa), len(sb))
    if smaller == 0:
        return 0.0
    return len(sa & sb) / smaller


# Stopwords for the keyword-intersection second pass in rule 1. Common
# English glue words plus "amcf"/"amuslimcf" (always present in AMCF
# copy and don't indicate same-topic) and the http(s) protocol tokens
# (link URLs would otherwise contribute trivial overlaps).
_STOPWORDS = frozenset({
    "this", "that", "with", "from", "have", "your", "their",
    "what", "when", "will", "been", "also",
    "amcf", "http", "https", "amuslimcf",
    # Domain-ubiquitous in AMCF copy — present in nearly every post,
    # so they collide on rule 1 without indicating same-topic.
    # Live probe (V5.12): a Retain Quran draft vs an AGL Fellows LI post
    # scored kw=2 exactly on {muslim, support} — unrelated topics.
    "muslim", "support",
})


def _significant_tokens(text: str) -> set:
    """Extract keywords for the rule-1 second-pass intersection check.

    Tokenization: re.findall(r'[a-z0-9]+', text.lower()) — alphanumeric
    runs, case-folded. Filters to tokens length >= 4 then removes the
    stopword set above.

    Used by rule 1 as an OR-fallback to _text_similarity for cases where
    one post is prose and the other is hashtag/bullet formatted — the
    overlap coefficient can dip below 0.5 when the formatted side
    inflates the word count with hashtags, while the underlying
    keywords still overlap. No < 4-word guard applies here.
    """
    if not isinstance(text, str):
        return set()
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return {t for t in tokens if len(t) >= 4 and t not in _STOPWORDS}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_queue(hubspot=None) -> list[dict]:
    """Fetch and normalize the live HubSpot social-broadcast queue.

    Args:
        hubspot: Optional pre-instantiated HubSpotClient. If None, one is
                 created via the same lazy-import pattern used by
                 content/social_capture.py and intents/content_memory.py.

    Returns:
        list[dict], each with:
          broadcast_guid, channel (normalized short name), channel_guid,
          trigger_at (UTC tz-aware datetime), trigger_at_et
          (America/New_York tz-aware), body, link (may be None).

        Items with no parseable triggerAt are dropped. Field paths read
        from content.body (with messageText as documented fallback) and
        content.link — NOT top-level message/messageUrl.

        Empty list on any error (logs ERROR with exc_info).
    """
    try:
        if hubspot is None:
            # Lazy import — matches content.social_capture.backfill_social_content
            # and intents.content_memory.run_email_backfill.
            from clients.hubspot import HubSpotClient
            hubspot = HubSpotClient()

        raw = hubspot.get_waiting_broadcasts()
        if isinstance(raw, dict) and "error" in raw:
            logger.error(f"get_queue: client returned error dict: {raw}")
            return []
        if not isinstance(raw, list):
            logger.error(
                f"get_queue: unexpected return type from client: "
                f"{type(raw).__name__}"
            )
            return []

        out = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            trigger_utc = _ms_to_utc(item.get("triggerAt"))
            if trigger_utc is None:
                continue

            content = item.get("content")
            if not isinstance(content, dict):
                content = {}
            body = content.get("body") or item.get("messageText")
            link = content.get("link")

            out.append({
                "broadcast_guid": item.get("broadcastGuid"),
                "channel":        _normalize_channel(item.get("channelKey") or ""),
                "channel_guid":   item.get("channelGuid"),
                "trigger_at":     trigger_utc,
                "trigger_at_et":  trigger_utc.astimezone(_ET_TZ),
                "body":           body,
                "link":           link,
            })
        return out
    except Exception as e:
        logger.error(f"get_queue: failed: {e}", exc_info=True)
        return []


def check_schedule(body, link, channel, trigger_at, queue=None) -> list[dict]:
    """Flag rule-1 / rule-2 violations against a queue.

    Args:
        body:       Proposed post body (string; may be empty).
        link:       Proposed CTA URL (string or None).
        channel:    Normalized channel name (e.g. "facebook").
        trigger_at: tz-aware datetime in any timezone. (Defensive
                    fallback: a naive datetime is interpreted as ET so
                    this function never raises — though the contract is
                    tz-aware.)
        queue:      Pre-fetched get_queue() result; refetched if None.

    Returns:
        list of violation dicts. Empty list = clean schedule.
        Each violation:
            {rule, broadcast_guid, channel, trigger_at_et (isoformat),
             excerpt, matched_on}
        rule:       "same_day_cross_network" or "slot_collision".
        matched_on: "slot" (rule 2), "link" or "text" (rule 1).

        Empty list on error (logs ERROR with exc_info).
    """
    try:
        if queue is None:
            queue = get_queue()
        if not queue:
            return []

        if trigger_at.tzinfo is None:
            target_aware = trigger_at.replace(tzinfo=_ET_TZ)
        else:
            target_aware = trigger_at
        target_utc = target_aware.astimezone(timezone.utc)
        target_ms = int(target_utc.timestamp() * 1000)
        target_et_date = target_utc.astimezone(_ET_TZ).date()
        target_link_norm = _strip_trailing_slash(link) if link else ""

        violations = []
        for item in queue:
            item_utc = item["trigger_at"]
            item_ms = int(item_utc.timestamp() * 1000)
            item_channel = item["channel"]
            item_et_date = item["trigger_at_et"].date()
            item_body = item.get("body") or ""
            item_link = item.get("link") or ""
            item_link_norm = _strip_trailing_slash(item_link)

            # Rule 2: slot collision — same channel, exact ms equality
            if item_channel == channel and item_ms == target_ms:
                violations.append({
                    "rule":           "slot_collision",
                    "broadcast_guid": item.get("broadcast_guid"),
                    "channel":        item_channel,
                    "trigger_at_et":  item["trigger_at_et"].isoformat(),
                    "excerpt":        _excerpt(item_body),
                    "matched_on":     "slot",
                })
                continue  # Don't also flag the same item under rule 1

            # Rule 1: cross-network same calendar day in ET
            if item_channel != channel and item_et_date == target_et_date:
                matched_on = None
                if (target_link_norm and item_link_norm
                        and target_link_norm == item_link_norm):
                    matched_on = "link"
                elif body and item_body and (
                    _text_similarity(body, item_body) >= _TEXT_SIMILARITY_THRESHOLD
                    or len(_significant_tokens(body) & _significant_tokens(item_body)) >= 2
                ):
                    matched_on = "text"

                if matched_on:
                    violations.append({
                        "rule":           "same_day_cross_network",
                        "broadcast_guid": item.get("broadcast_guid"),
                        "channel":        item_channel,
                        "trigger_at_et":  item["trigger_at_et"].isoformat(),
                        "excerpt":        _excerpt(item_body),
                        "matched_on":     matched_on,
                    })

        return violations
    except Exception as e:
        logger.error(f"check_schedule: failed: {e}", exc_info=True)
        return []


def suggest_slot(channel, near_date, queue=None):
    """First clean 10:00 America/New_York slot for a channel.

    Args:
        channel:   Normalized channel name.
        near_date: A date or datetime (interpreted in ET). Naive
                   datetimes treated as ET wall time; aware datetimes
                   converted to ET first.
        queue:     Pre-fetched get_queue() result; refetched if None.

    Returns:
        tz-aware datetime at 10:00 America/New_York on the first day
        in [near_date, near_date + 13 days] where the target channel
        has NO queue item at that exact timestamp.

        None if nothing clean in 14 days, or on error.

    Note: this only guarantees a slot-collision-free time (rule 2).
    Cross-network content checks (rule 1) stay in check_schedule.
    """
    try:
        if queue is None:
            queue = get_queue()

        # Coerce near_date to an ET calendar date. Order matters: check
        # datetime first since it's a subclass of date.
        if isinstance(near_date, datetime):
            if near_date.tzinfo is None:
                aware = near_date.replace(tzinfo=_ET_TZ)
            else:
                aware = near_date
            start_date = aware.astimezone(_ET_TZ).date()
        elif isinstance(near_date, date):
            start_date = near_date
        else:
            logger.error(
                f"suggest_slot: near_date must be date or datetime, "
                f"got {type(near_date).__name__}"
            )
            return None

        taken_ms = {
            int(item["trigger_at"].timestamp() * 1000)
            for item in queue
            if item.get("channel") == channel
        }

        for offset in range(14):
            candidate_date = start_date + timedelta(days=offset)
            candidate_et = datetime(
                candidate_date.year,
                candidate_date.month,
                candidate_date.day,
                10, 0,
                tzinfo=_ET_TZ,
            )
            candidate_ms = int(
                candidate_et.astimezone(timezone.utc).timestamp() * 1000
            )
            if candidate_ms not in taken_ms:
                return candidate_et

        return None
    except Exception as e:
        logger.error(f"suggest_slot: failed: {e}", exc_info=True)
        return None
