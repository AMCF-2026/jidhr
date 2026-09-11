"""
Jidhr Reports & Analytics
==========================
Reporting sub-handlers for grant totals, lapsed donor detection,
inactive fund identification, fee calculations, uncashed checks,
and quarterly DAF summaries.

NEW in v1.3 — Survey priority: Everyone rated 4.5+

Sub-handlers:
  A. Grant reporting
  B. Ramadan lapsed donors
  C. Inactive funds
  D. Donors not contacted
  E. Fee calculations
  F. Grants issued but not cleared
  G. Quarterly DAF summary

Where the numbers come from
---------------------------
Everything CSuite-sourced here reads `csuite_mirror` and makes NO live
CSuite call. That is the whole point of Step 3c: each of these reports
used to paginate a live endpoint behind a page cap — 10 pages of grants,
200 funds, 500 checks, 50 fund-detail calls — and print the result as if
it were the whole picture. "6 dormant funds" meant "6 of the 200 funds I
happened to read".

The caps are gone rather than raised, because a report that makes 2,000
CSuite calls is a report that gets rate limited. Each of these now answers
from a complete local copy and ends with `as_of_line()` naming when that
copy was taken. If the mirror is empty for a type a report needs, the
report says so and stops — see clients/mirror_read.py for why there is no
fallback to the old capped fetch.

The HubSpot-sourced reports (DAF inquiries, tasks, investment requests,
donors-not-contacted) are unchanged and still carry their partial_note
banners: those really are capped fetches.
"""

import logging
from datetime import datetime, timedelta

from clients import mirror_read
from config import Config
from intents.queries import (
    _fund_row_code,
    _fund_row_names,
    _norm_for_match,
    extract_fund_name_phrase,
    extract_fund_ref,
    split_fund_name,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger keywords (grouped by sub-handler)
# ---------------------------------------------------------------------------

_GRANT_TRIGGERS = [
    'how many grants', 'grants last quarter', 'grants this year',
    'grant report', 'grants processed', 'quarterly grants',
    'grants this quarter', 'grant summary', 'open grants',
    'show grants', 'list grants', 'recent grants', 'show me grants',
]

_LAPSED_TRIGGERS = [
    'ramadan lapsed', 'lapsed donors', 'ramadan comparison',
    'who gave last ramadan', 'ramadan giving', 'lapsed giving',
]

_INACTIVE_FUND_TRIGGERS = [
    'inactive funds', 'dormant funds', 'no grants',
    'funds with no activity', 'stale funds',
]

_NOT_CONTACTED_TRIGGERS = [
    'not contacted', "haven't reached out", 'no contact in',
    'dormant donors', 'need to contact', 'not been contacted',
    "haven't contacted",
]

_FEE_TRIGGERS = [
    'calculate fees', 'admin fees', 'fund fees',
    'fee on balance', 'quarterly fees', 'fee calculation',
    'estimate fees',
]

_CHECK_TRIGGERS = [
    'uncashed checks', 'checks not cashed', 'outstanding checks',
    "charities haven't cashed", 'check status', 'uncashed',
]

_QUARTERLY_TRIGGERS = [
    'quarterly summary', 'daf summary', 'quarterly daf',
    'quarter review', 'fund activity summary', 'quarterly report',
]

_DAF_INQUIRY_TRIGGERS = [
    'daf inquiries', 'new daf inquiries', 'daf submissions',
    "this month's daf", 'monthly daf', 'daf inquiry summary',
    'endowment inquiries', 'new endowment inquiries',
    'recent inquiries', 'inquiry summary', 'how many inquiries',
    'inquiries this month', 'inquiries last month',
]

_TASK_TRIGGERS = [
    'my tasks', 'pending tasks', 'task list', 'prioritize tasks',
    'task priority', 'open tasks', 'list tasks', 'what tasks',
    'tasks by priority', 'show tasks', 'my to do', 'my todo',
]

_INVESTMENT_TRIGGERS = [
    'investment request', 'investment requests', 'andalus',
    'investment form', 'compile investment', 'incoming investment',
    'new investment requests', 'investment submissions',
]

_ENDOWMENT_DIST_TRIGGERS = [
    'endowment distribution', 'distribution dates', 'upcoming distribution',
    'endowment payout', 'distribution schedule',
    'which endowments have upcoming', 'endowment dates',
]


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# Nothing is donor-facing yet; every handler is staff-and-above.
ALLOWED_ROLES = frozenset({"admin", "staff"})

def can_handle(query: str, **kwargs) -> bool:
    q = query.lower().strip()
    all_triggers = (
        _GRANT_TRIGGERS + _LAPSED_TRIGGERS + _INACTIVE_FUND_TRIGGERS +
        _NOT_CONTACTED_TRIGGERS + _FEE_TRIGGERS + _CHECK_TRIGGERS +
        _QUARTERLY_TRIGGERS + _DAF_INQUIRY_TRIGGERS + _TASK_TRIGGERS +
        _INVESTMENT_TRIGGERS + _ENDOWMENT_DIST_TRIGGERS
    )
    return any(t in q for t in all_triggers)


def handle(query: str, ctx) -> str:
    """Route to the appropriate report sub-handler.

    The whole dispatch runs inside one mirror_read.exclusion_log_scope, so
    the test funds a report drops are logged once for that report — not
    once for every rows() call it makes.
    """
    with mirror_read.exclusion_log_scope():
        return _dispatch(query, ctx)


def _dispatch(query: str, ctx) -> str:
    q = query.lower().strip()
    hubspot = ctx.services.hubspot
    csuite = ctx.services.csuite

    # The CSuite-sourced reports take no client: they read the mirror.
    if any(t in q for t in _GRANT_TRIGGERS):
        return _report_grants(q)

    if any(t in q for t in _LAPSED_TRIGGERS):
        return _report_lapsed_donors(q)

    if any(t in q for t in _INACTIVE_FUND_TRIGGERS):
        return _report_inactive_funds()

    if any(t in q for t in _NOT_CONTACTED_TRIGGERS):
        return _report_not_contacted(hubspot, csuite)

    if any(t in q for t in _FEE_TRIGGERS):
        # The ORIGINAL query, not the lowercased one: the fund-name
        # extractor keys on capitalisation, so "Fees for Alpha Family Fund"
        # resolved to nothing when it was handed `q`. Only codes and ids
        # ever worked. Every other report here matches on lowercase
        # keywords and still gets `q`.
        return _report_fees(query)

    if any(t in q for t in _CHECK_TRIGGERS):
        return _report_uncleared_grants()

    if any(t in q for t in _QUARTERLY_TRIGGERS):
        return _report_quarterly_summary(q)

    if any(t in q for t in _DAF_INQUIRY_TRIGGERS):
        return _report_daf_inquiry_summary(q, hubspot)

    if any(t in q for t in _TASK_TRIGGERS):
        return _report_tasks(hubspot)

    if any(t in q for t in _INVESTMENT_TRIGGERS):
        return _report_investment_requests(hubspot)

    if any(t in q for t in _ENDOWMENT_DIST_TRIGGERS):
        return _report_endowment_distributions()

    return "❌ Report type not recognised."


# ---------------------------------------------------------------------------
# Mirror helpers
# ---------------------------------------------------------------------------

# Fund group ids, from Config (which took them from funit/list/fgroup).
FUND_GROUP_LABELS = {
    Config.FUND_GROUP_SYSTEM: "System",
    Config.FUND_GROUP_FISCAL_SPONSORSHIP: "Fiscal Sponsorship",
    Config.FUND_GROUP_DAF: "DAF",
    Config.FUND_GROUP_MICROPHILANTHROPY: "Microphilanthropy",
    Config.FUND_GROUP_MIGRATION: "Migration",
    Config.FUND_GROUP_FISCAL_DAF: "Fiscal DAF",
    Config.FUND_GROUP_GIVING_CIRCLE: "Giving Circle",
    Config.FUND_GROUP_ENDOWMENT: "Endowment",
    Config.FUND_GROUP_GRANT: "Grant",
}

DAF_GROUP_ID = Config.FUND_GROUP_DAF
ENDOWMENT_GROUP_ID = Config.FUND_GROUP_ENDOWMENT

# grant_status values observed across the sample (probe #2, C2):
#   paid (60), voucher (35), new (4), complete (1)
# 'paid' is issued-but-not-yet-cleared; 'complete' has cleared. grant/list
# carries no check_id or check_num, so grants cannot be joined to checks at
# all — which is why the old uncashed-check report is gone rather than
# fixed.
GRANT_STATUS_ISSUED = "paid"


def _fund_group_id(fund: dict):
    """A mirrored fund's group id.

    The mirror stores it in its own column (fund_group_id); CSuite's field
    inside the payload is fgroup_id. Both are read so a fund written before
    the column existed still reports its group.
    """
    for key in ("fund_group_id", "fgroup_id"):
        value = fund.get(key)
        if value not in (None, ""):
            try:
                return int(value)
            except (TypeError, ValueError):
                return value
    return None


def _fund_group_label(fund: dict) -> str:
    group_id = _fund_group_id(fund)
    if group_id is None:
        return "Ungrouped"
    return FUND_GROUP_LABELS.get(group_id, f"Group {group_id}")


# What the quarterly summary calls a fund that is neither DAF nor Endowment.
OTHER_GROUP_LABEL = "Operating / Other"

# Field names a fund payload might carry its group's NAME under. CSuite's
# funit/display returns only fgroup_id (probe #3, 70-field inventory), so
# in practice these are absent and the id map below decides — but a name
# from the source wins if one ever appears.
_FUND_GROUP_NAME_FIELDS = ("fgroup_name", "fund_group_name")


def _fund_group_name(fund: dict) -> str:
    """The quarterly summary's label for a fund's group.

    The group's own name if the fund row carries one; else DAF / Endowment
    by id, since those two are what the summary splits on; else
    OTHER_GROUP_LABEL.
    """
    for key in _FUND_GROUP_NAME_FIELDS:
        value = fund.get(key)
        if value not in (None, ""):
            return str(value).strip()
    group_id = _fund_group_id(fund)
    if group_id in (DAF_GROUP_ID, ENDOWMENT_GROUP_ID):
        return FUND_GROUP_LABELS[group_id]
    return OTHER_GROUP_LABEL


def _amount(value) -> float:
    """A CSuite money string as a float, for totalling in a report."""
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _fund_index() -> dict:
    """{funit_id (str): mirrored fund row}."""
    return {str(f.get("csuite_id")): f for f in mirror_read.rows("fund")}


def _find_mirrored_fund(query: str) -> tuple:
    """(fund, ambiguous_rows) for the fund named in a query, from the mirror.

    Returns (None, []) when the query names no fund, (fund, []) on an exact
    resolution, and (None, rows) when the name matched more than one — the
    same never-guess rule the live lookup follows, applied to local rows.
    """
    funds = mirror_read.rows("fund")
    if not funds:
        return None, []

    ref = extract_fund_ref(query) or {}

    fund_id = ref.get("fund_id")
    if fund_id is not None:
        for fund in funds:
            if str(fund.get("csuite_id")) == str(fund_id):
                return fund, []
        return None, []

    code = ref.get("code")
    if code:
        wanted = str(code).strip().upper()
        matches = [f for f in funds
                   if (_fund_row_code(f) or "").upper() == wanted]
        if len(matches) == 1:
            return matches[0], []
        return None, matches

    phrase = extract_fund_name_phrase(query)
    if not phrase:
        return None, []

    needle = _norm_for_match(phrase)
    if not needle:
        return None, []

    matches = [
        f for f in funds
        if any(needle in _norm_for_match(name)
               for name in _fund_row_names(f))
    ]
    if len(matches) == 1:
        return matches[0], []
    return None, matches


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_date_range(query: str) -> tuple:
    """
    Infer a date range from the query. Returns (start, end) as date strings.
    Defaults to current quarter if nothing specific detected.
    """
    now = datetime.now()

    if 'last quarter' in query:
        q_month = ((now.month - 1) // 3) * 3  # start of current quarter
        if q_month == 0:
            start = datetime(now.year - 1, 10, 1)
            end = datetime(now.year - 1, 12, 31)
        else:
            start = datetime(now.year, q_month - 2, 1)
            end_month = q_month
            if end_month == 12:
                end = datetime(now.year, 12, 31)
            else:
                end = datetime(now.year, end_month + 1, 1) - timedelta(days=1)
    elif 'this year' in query:
        start = datetime(now.year, 1, 1)
        end = now
    elif 'last year' in query:
        start = datetime(now.year - 1, 1, 1)
        end = datetime(now.year - 1, 12, 31)
    elif 'this quarter' in query or 'quarterly' in query:
        q_start_month = ((now.month - 1) // 3) * 3 + 1
        start = datetime(now.year, q_start_month, 1)
        end = now
    else:
        # Default: current quarter
        q_start_month = ((now.month - 1) // 3) * 3 + 1
        start = datetime(now.year, q_start_month, 1)
        end = now

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _year_quarter(date_str: str) -> tuple:
    """(year, quarter) for a YYYY-MM-DD string — the donation_fund_quarter key."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except (TypeError, ValueError):
        now = datetime.now()
        return now.year, (now.month - 1) // 3 + 1
    return dt.year, (dt.month - 1) // 3 + 1


def _get_quarter_label(date_str: str) -> str:
    """Return 'Q1 2026' style label from a date string."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        q = (dt.month - 1) // 3 + 1
        return f"Q{q} {dt.year}"
    except Exception:
        return "Current Quarter"


# =========================================================================
# A. GRANT REPORTING
# =========================================================================

def _report_grants(query: str) -> str:
    """Grant counts and totals for a date range, from the mirror."""
    missing = mirror_read.require("grant")
    if missing:
        return missing

    start, end = _parse_date_range(query)
    label = f"{start} to {end}"
    logger.info(f"Grant report for {label} (mirror)...")

    # Filtered in SQL rather than in Python: the alternative is pulling all
    # 6,700 grant documents across the wire to keep a few dozen.
    filtered = mirror_read.rows(
        "grant",
        "AND data->>'grant_date' >= %s AND data->>'grant_date' <= %s",
        (start, end),
    )

    if not filtered:
        return (f"📊 No grants found for **{label}**.\n\n"
                f"{mirror_read.as_of_line('grant')}")

    total_amount = sum(_amount(g.get("grant_amount")) for g in filtered)

    by_fund = {}
    for grant in filtered:
        fund = grant.get("fund_name") or "Unknown"
        stats = by_fund.setdefault(fund, {"count": 0, "total": 0.0})
        stats["count"] += 1
        stats["total"] += _amount(grant.get("grant_amount"))

    sorted_funds = sorted(by_fund.items(), key=lambda x: x[1]["total"],
                          reverse=True)

    lines = [
        f"📊 **Grant Report: {label}**",
        "",
        f"**Total:** {len(filtered)} grants totalling **${total_amount:,.2f}**",
        "",
        "**By Fund (top 10):**",
    ]
    for fund_name, stats in sorted_funds[:10]:
        lines.append(
            f"• {fund_name}: {stats['count']} grants, ${stats['total']:,.2f}")
    if len(sorted_funds) > 10:
        lines.append(f"• ... and {len(sorted_funds) - 10} more funds")

    lines += ["", mirror_read.as_of_line("grant")]
    return "\n".join(lines)


def partial_note(count: int, complete: bool, label: str = "records") -> str | None:
    """The banner a capped fetch must carry, or None when the data is whole.

    Still used by the HubSpot-sourced reports below, which really are
    capped. The CSuite reports no longer need it: they read a complete
    mirror and state its age instead.
    """
    if complete:
        return None
    return (f"⚠️ Partial data: first {count} {label} only — "
            f"totals below are NOT complete.")


# =========================================================================
# B. RAMADAN LAPSED DONORS
# =========================================================================

# Only living individuals are worth an outreach list: `dead` marks a
# deceased profile, and ptype is 'indiv' or 'org' (probe #2, C8 — those
# are the only two values observed across the sample).
PROFILE_TYPE_INDIVIDUAL = "indiv"

_LAPSED_NAMES_SHOWN = 25


def _is_outreachable(profile: dict) -> bool:
    """True if this profile belongs on a donor outreach list."""
    if not profile:
        return False
    dead = profile.get("dead")
    if dead in (1, "1", True):
        return False
    return str(profile.get("ptype") or "").strip().lower() == \
        PROFILE_TYPE_INDIVIDUAL


def _report_lapsed_donors(query: str) -> str:
    """Donors who gave during last Ramadan but not this one.

    Reads donation_agg.ramadan_years, which sync/mirror.py computed once
    over the whole donation history. The old version paged the live
    donation endpoint 10 pages deep — 1,000 of 26,500 donations — and
    called the result a lapsed-donor list.
    """
    missing = mirror_read.require("donation_agg", "profile")
    if missing:
        return missing

    logger.info("Running Ramadan lapsed donor analysis (mirror)...")

    now = datetime.now()
    this_year = now.year
    last_year = this_year - 1

    lapsed = []
    for agg in mirror_read.rows("donation_agg"):
        years = agg.get("ramadan_years") or []
        try:
            years = {int(y) for y in years}
        except (TypeError, ValueError):
            continue
        if last_year not in years or this_year in years:
            continue

        profile = mirror_read.get("profile", agg.get("csuite_id"))
        if not _is_outreachable(profile):
            continue

        lapsed.append({
            "profile_id": agg.get("csuite_id"),
            "name": profile.get("name") or profile.get("primary_email")
                    or str(agg.get("csuite_id")),
            "email": profile.get("primary_email") or "",
            "lifetime": agg.get("lifetime_total"),
        })

    footer = mirror_read.as_of_line("donation_agg", "profile")

    if not lapsed:
        return (
            f"✅ **No lapsed Ramadan donors.** Every individual who gave "
            f"during Ramadan {last_year} has also given in Ramadan "
            f"{this_year}.\n\n{footer}"
        )

    lapsed.sort(key=lambda d: d["name"].lower())

    lines = [
        "📊 **Ramadan Lapsed Donors**",
        "",
        f"**{len(lapsed)}** individual donors gave during Ramadan "
        f"{last_year} but have **not** given in Ramadan {this_year}.",
        "",
        f"**Donors to re-engage (showing "
        f"{min(len(lapsed), _LAPSED_NAMES_SHOWN)} of {len(lapsed)}):**",
    ]
    for donor in lapsed[:_LAPSED_NAMES_SHOWN]:
        email = f" — {donor['email']}" if donor["email"] else ""
        lines.append(f"• **{donor['name']}** (Profile {donor['profile_id']})"
                     f"{email}")

    if len(lapsed) > _LAPSED_NAMES_SHOWN:
        lines.append(
            f"• ... and {len(lapsed) - _LAPSED_NAMES_SHOWN} more "
            f"({len(lapsed)} in total)")

    lines += [
        "",
        "💡 *Deceased profiles and organisations are excluded.*",
        "",
        footer,
    ]
    return "\n".join(lines)


# =========================================================================
# C. INACTIVE FUNDS
# =========================================================================

# The date a fund was opened, as funit/display returns it. Confirmed in
# scripts/probe_output/csuite_fields.md: `fund_open_date`, str(date),
# populated on every sampled fund. (`fund_open` beside it is the boolean.)
FUND_OPEN_DATE_FIELD = "fund_open_date"

_DORMANT_GROUP_SHOWN = 25


def _report_inactive_funds() -> str:
    """Every fund with no grant in the last 12 months.

    ALL funds, not the first page of them. The old version read
    the fund list capped at 200 against 397 real funds, and paged grants
    10 deep,
    so "dormant" meant "dormant among the half of the funds I read".

    A fund opened inside the window is not dormant — it has not had twelve
    months in which to grant. Those are counted separately rather than
    listed, so a burst of new DAFs does not read as a burst of neglect.
    """
    missing = mirror_read.require("fund", "grant")
    if missing:
        return missing

    logger.info("Running dormant funds analysis (mirror)...")

    funds = mirror_read.rows("fund")
    cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    last_grant = {}
    for grant in mirror_read.rows("grant"):
        fund_id = grant.get("funit_id")
        grant_date = grant.get("grant_date")
        if fund_id in (None, "") or not grant_date:
            continue
        key = str(fund_id)
        if key not in last_grant or grant_date > last_grant[key]:
            last_grant[key] = grant_date

    dormant = []
    new_funds = 0
    for fund in funds:
        key = str(fund.get("csuite_id"))
        latest = last_grant.get(key)
        if latest is not None and latest >= cutoff:
            continue

        opened = fund.get(FUND_OPEN_DATE_FIELD)
        if opened and str(opened)[:10] >= cutoff:
            new_funds += 1
            continue

        name, code = split_fund_name(fund.get("fund_name"))
        dormant.append({
            "name": name or fund.get("fund_name") or "Unknown",
            "code": code,
            "fund_id": key,
            "group": _fund_group_label(fund),
            "last": latest or "Never",
        })

    footer = mirror_read.as_of_line("fund", "grant")

    if not dormant:
        lines = [f"✅ **All {len(funds) - new_funds} established funds have "
                 "had grant activity in the last 12 months.**"]
        if new_funds:
            lines += ["", f"New funds (<12 months, not yet granting): "
                          f"{new_funds}"]
        lines += ["", footer]
        return "\n".join(lines)

    # Real last-grant dates oldest first, then the never-granted funds.
    # "Never" sorts last on purpose: a fund that granted once and stopped
    # is a relationship that lapsed; one that never granted may simply not
    # have started, and the former is the more actionable of the two.
    dormant.sort(key=lambda f: (f["last"] == "Never", f["last"],
                                f["name"].lower()))

    by_group = {}
    for fund in dormant:
        by_group.setdefault(fund["group"], []).append(fund)

    lines = [
        "📊 **Dormant Funds** (no grants in 12+ months)",
        "",
        f"**{len(dormant)}** of {len(funds)} funds, grouped by fund type:",
    ]
    if new_funds:
        lines.append(
            f"New funds (<12 months, not yet granting): {new_funds}")
    lines.append("")

    for group in sorted(by_group, key=lambda g: (-len(by_group[g]), g)):
        group_funds = by_group[group]
        lines.append(f"**{group}** ({len(group_funds)})")
        for fund in group_funds[:_DORMANT_GROUP_SHOWN]:
            suffix = f" ({fund['code']})" if fund["code"] else ""
            lines.append(
                f"• **{fund['name']}**{suffix} — id {fund['fund_id']}, "
                f"last grant: {fund['last']}")
        if len(group_funds) > _DORMANT_GROUP_SHOWN:
            lines.append(
                f"• ... and {len(group_funds) - _DORMANT_GROUP_SHOWN} more")
        lines.append("")

    lines += [
        "💡 *Consider reaching out to fund advisors to discuss grant "
        "recommendations.*",
        "",
        footer,
    ]
    return "\n".join(lines)


# =========================================================================
# D. DONORS NOT CONTACTED
# =========================================================================

def _report_not_contacted(hubspot, csuite) -> str:
    """Donors not contacted in 6+ months."""
    logger.info("Running donors-not-contacted analysis...")

    cutoff = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%dT00:00:00Z")

    try:
        contacts_data = hubspot.get_contacts(limit=100)
        if 'results' not in contacts_data:
            return "❌ Failed to fetch contacts."
        contacts = contacts_data['results']
    except Exception as e:
        return f"❌ Failed to fetch contacts: {e}"

    stale = []
    for c in contacts:
        props = c.get('properties', {})
        last_activity = (
            props.get('hs_last_activity_date') or
            props.get('notes_last_updated') or
            props.get('lastmodifieddate', '')
        )
        if last_activity and last_activity < cutoff:
            name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
            email = props.get('email', 'N/A')
            stale.append({
                "name": name or email,
                "email": email,
                "last_activity": last_activity[:10],
                "contact_id": c.get('id'),
            })

    if not stale:
        return "✅ **All contacts have been reached in the last 6 months!**"

    # Sort by oldest first
    stale.sort(key=lambda x: x['last_activity'])

    lines = [
        f"📊 **Donors Not Contacted** (6+ months)",
        "",
        f"**{len(stale)}** contacts identified:",
        "",
    ]
    for s in stale[:20]:
        lines.append(f"• **{s['name']}** ({s['email']}) — Last activity: {s['last_activity']}")

    if len(stale) > 20:
        lines.append(f"• ... and {len(stale) - 20} more")

    lines.append("")
    lines.append("💡 *Consider scheduling outreach calls or sending a check-in email.*")

    return "\n".join(lines)


# =========================================================================
# E. FEE CALCULATIONS
# =========================================================================

# Field names taken from scripts/probe_output/csuite_fields.md (funit/feetype).
# The previous code read fee_name / fee_percent / min_fee, none of which
# CSuite returns, so every row rendered as "Unknown: ?% (minimum: $0)".
_FEE_NAME_FIELD = "admin_fee_type_name"
_FEE_TYPE_FIELD = "admin_fee_type_type"
_FEE_PERCENT_FIELD = "admin_fee_percent"
_FEE_AMOUNT_FIELD = "admin_fee_amount"
_FEE_MIN_FIELD = "admin_fee_min_fee"
_FEE_MAX_FIELD = "admin_fee_max_fee"

# CSuite exposes no field linking a fund to a fee type: admin_fee_fundgroup_id
# is null on all 397 funds (probe #3, C9) and funit/feetype's ids match
# nothing on the fund side. The old code silently applied fee_types[0] to
# whatever fund was asked about and printed the result as an estimate; that
# was a guess wearing a dollar sign, so it is gone rather than corrected.
_FEE_JOIN_NOTE = (
    "ℹ️ Fee assignment per fund is not exposed by CSuite — confirming with "
    "Shazeen."
)


def _money(value) -> str | None:
    """Format a CSuite money string as currency, or None if absent."""
    if value in (None, ""):
        return None
    try:
        return f"${float(str(value).replace(',', '').strip()):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def _format_fee_type(ft: dict) -> str:
    """One fee-type line, using whichever of its fields are populated."""
    name = ft.get(_FEE_NAME_FIELD) or "Unnamed fee type"
    bits = []

    percent = ft.get(_FEE_PERCENT_FIELD)
    if percent not in (None, ""):
        try:
            bits.append(f"{float(percent):g}%")
        except (TypeError, ValueError):
            bits.append(f"{percent}%")

    flat = _money(ft.get(_FEE_AMOUNT_FIELD))
    if flat:
        bits.append(f"flat {flat}")

    minimum = _money(ft.get(_FEE_MIN_FIELD))
    if minimum:
        bits.append(f"min {minimum}")

    maximum = _money(ft.get(_FEE_MAX_FIELD))
    if maximum:
        bits.append(f"max {maximum}")

    kind = ft.get(_FEE_TYPE_FIELD)
    if kind:
        bits.append(str(kind).replace("_", " "))

    detail = " · ".join(bits) if bits else "no rate published"
    return f"• **{name}:** {detail}"


def _fund_balance_line(query: str) -> str | None:
    """A one-line balance for the fund named in the query, from the mirror."""
    try:
        fund, ambiguous = _find_mirrored_fund(query)
    except Exception as e:
        logger.error(f"Fee report fund lookup failed: {e}")
        return None

    if fund is None:
        if ambiguous:
            names = ", ".join(
                (_fund_row_names(f) or ["Unknown"])[0] for f in ambiguous[:5])
            return (
                f"⚠️ That fund name matched {len(ambiguous)} funds ({names}). "
                "Name one exactly, or use its code."
            )
        return None

    name, code = split_fund_name(fund.get("fund_name"))
    balance = _money(fund.get("current_fundbalance")) or "unknown"
    suffix = f" ({code})" if code else ""
    return f"💰 **{name or 'Fund'}**{suffix} — balance {balance}"


def _report_fees(query: str) -> str:
    """Show the fee structure, and the balance of a named fund if given."""
    missing = mirror_read.require("fee_type")
    if missing:
        return missing

    logger.info("Running fee report (mirror)...")

    fee_types = mirror_read.rows("fee_type")

    # If the query names a fund, show its balance beside the table. No fee is
    # computed for it: which fee type applies is exactly what CSuite will not
    # tell us, so a number here would be a guess presented as an answer.
    fund_line = _fund_balance_line(query)
    read_types = ["fee_type"] + (["fund"] if fund_line else [])

    lines = []
    if fund_line:
        lines += [fund_line, ""]
    lines += ["📊 **AMCF Fee Structure**", ""]

    if fee_types:
        for fee_type in fee_types:
            lines.append(_format_fee_type(fee_type))
    else:
        lines.append("_The mirror holds no fee types._")

    lines.append("")
    lines.append(_FEE_JOIN_NOTE)

    if not fund_line:
        lines.append("")
        lines.append(
            '💡 *To see a specific fund\'s balance, try: '
            '"Fees for fund 1234" or "Fees for END0026".*'
        )

    lines += ["", mirror_read.as_of_line(*read_types)]
    return "\n".join(lines)


# =========================================================================
# F. GRANTS ISSUED BUT NOT CLEARED
# =========================================================================

_UNCLEARED_GRANTEES_SHOWN = 20

GRANT_STATUS_CLEARED = "complete"


def _status_maintenance_note() -> str:
    """How many grants have EVER reached 'complete', out of all of them.

    Across the probe sample only 1 grant in 100 was 'complete' against 60
    'paid'. If that ratio holds over 6,700 grants, 'paid' is not "awaiting
    clearance" — it is simply where grants stop being updated, and this
    whole report is counting the wrong thing. The number is printed so the
    reader can judge that rather than trust the headline.
    """
    all_grants = mirror_read.rows("grant")
    total = len(all_grants)
    cleared = sum(
        1 for g in all_grants
        if str(g.get("grant_status") or "").strip().lower()
        == GRANT_STATUS_CLEARED)
    return (f"Note: only {cleared} of {total} grants in the mirror have ever "
            f"been marked '{GRANT_STATUS_CLEARED}' — confirm with Shazeen "
            "whether that status is maintained.")


def _report_uncleared_grants() -> str:
    """Grants at status 'paid' — issued, not yet cleared.

    This replaces an uncashed-CHECK report. grant/list carries no check_id
    and no check_num (probe #2, C2), so a grant cannot be joined to the
    check that paid it; the old report grouped check rows by
    account_name, which is the AMCF bank account, not the grantee — so it
    answered "which of our accounts has uncleared checks" while being read
    as "which charities have not cashed". That question cannot be answered
    from this API, and this one can.
    """
    missing = mirror_read.require("grant")
    if missing:
        return missing

    logger.info("Running uncleared grants report (mirror)...")

    grants = mirror_read.rows(
        "grant",
        "AND data->>'grant_status' = %s",
        (GRANT_STATUS_ISSUED,),
    )

    footer = mirror_read.as_of_line("grant")
    status_note = _status_maintenance_note()

    if not grants:
        return ("✅ **No grants are sitting at status "
                f"'{GRANT_STATUS_ISSUED}'.**\n\n{status_note}\n\n{footer}")

    by_grantee = {}
    total = 0.0
    for grant in grants:
        grantee = grant.get("name") or "Unknown grantee"
        amount = _amount(grant.get("grant_amount"))
        total += amount
        by_grantee.setdefault(grantee, []).append({
            "grant_id": grant.get("csuite_id"),
            "date": grant.get("grant_date") or "no date",
            "amount": amount,
            "fund": grant.get("fund_name") or "Unknown fund",
        })

    for entries in by_grantee.values():
        entries.sort(key=lambda g: g["date"])

    # Oldest first: the point of the report is what has been outstanding
    # longest, not who is owed the most.
    ordered = sorted(by_grantee.items(), key=lambda kv: kv[1][0]["date"])

    lines = [
        f"📊 **Grants issued but not yet cleared (status "
        f"'{GRANT_STATUS_ISSUED}')**",
        status_note,
        "",
        f"**{len(grants)}** grants across **{len(by_grantee)}** grantees, "
        f"totalling **${total:,.2f}**",
        "",
        "**By grantee, oldest first:**",
    ]
    for grantee, entries in ordered[:_UNCLEARED_GRANTEES_SHOWN]:
        grantee_total = sum(e["amount"] for e in entries)
        lines.append(
            f"• **{grantee}** — {len(entries)} grant(s), "
            f"${grantee_total:,.2f}")
        for entry in entries[:3]:
            lines.append(
                f"  {entry['date']}: ${entry['amount']:,.2f} "
                f"(grant {entry['grant_id']}, {entry['fund']})")
        if len(entries) > 3:
            lines.append(f"  ... and {len(entries) - 3} more")

    if len(ordered) > _UNCLEARED_GRANTEES_SHOWN:
        lines.append(
            f"• ... and {len(ordered) - _UNCLEARED_GRANTEES_SHOWN} more "
            "grantees")

    lines += [
        "",
        "ℹ️ *CSuite exposes no link from a grant to the check that paid it, "
        "so this is grant status, not bank clearance. Finance can confirm "
        "against the account.*",
        "",
        footer,
    ]
    return "\n".join(lines)


# =========================================================================
# G. QUARTERLY DAF SUMMARY
# =========================================================================

def _report_quarterly_summary(query: str) -> str:
    """Donations in and grants out for one quarter, split DAF vs Endowment.

    Donations come from donation_fund_quarter, which sync/mirror.py rolled
    up per fund per quarter over the whole 26,500-row history. Grants are
    filtered from the mirrored grant rows. Neither is capped.
    """
    missing = mirror_read.require("donation_fund_quarter", "grant", "fund")
    if missing:
        return missing

    start, end = _parse_date_range(query)
    q_label = _get_quarter_label(start)
    year, quarter = _year_quarter(start)

    logger.info(f"Quarterly summary for {q_label} ({start} to {end}, mirror)")

    funds = _fund_index()

    donations = mirror_read.rows(
        "donation_fund_quarter",
        "AND data->>'year' = %s AND data->>'quarter' = %s",
        (str(year), str(quarter)),
    )
    grants = mirror_read.rows(
        "grant",
        "AND data->>'grant_date' >= %s AND data->>'grant_date' <= %s",
        (start, end),
    )

    # Per fund, then folded into group totals. Keeping the per-fund rows is
    # what lets the top-15 list and the DAF/Endowment split come from the
    # same pass.
    per_fund = {}

    def bucket(fund_id, fund_name):
        key = str(fund_id) if fund_id not in (None, "") else "unknown"
        entry = per_fund.get(key)
        if entry is None:
            fund = funds.get(key, {})
            name, _code = split_fund_name(
                fund.get("fund_name") or fund_name or "Unknown")
            entry = per_fund[key] = {
                "name": name or fund_name or "Unknown",
                "group_id": _fund_group_id(fund),
                "group_label": _fund_group_name(fund),
                "donations": 0.0,
                "grants": 0.0,
            }
        return entry

    for row in donations:
        entry = bucket(row.get("funit_id"), row.get("fund_name"))
        entry["donations"] += _amount(row.get("total"))

    for grant in grants:
        entry = bucket(grant.get("funit_id"), grant.get("fund_name"))
        entry["grants"] += _amount(grant.get("grant_amount"))

    total_in = sum(f["donations"] for f in per_fund.values())
    total_out = sum(f["grants"] for f in per_fund.values())

    groups = {DAF_GROUP_ID: {"in": 0.0, "out": 0.0, "funds": 0},
              ENDOWMENT_GROUP_ID: {"in": 0.0, "out": 0.0, "funds": 0},
              None: {"in": 0.0, "out": 0.0, "funds": 0}}
    for entry in per_fund.values():
        key = entry["group_id"] if entry["group_id"] in groups else None
        groups[key]["in"] += entry["donations"]
        groups[key]["out"] += entry["grants"]
        groups[key]["funds"] += 1

    lines = [
        f"📊 **Quarterly Summary: {q_label}**",
        f"🗓 {start} to {end}",
        "",
        f"💰 **Total Donations In:** ${total_in:,.2f}",
        f"🎁 **Total Grants Out:** ${total_out:,.2f}",
        f"📈 **Net:** ${total_in - total_out:,.2f}",
        "",
        "**By fund group:**",
    ]
    for group_id, label in ((DAF_GROUP_ID, "DAF"),
                            (ENDOWMENT_GROUP_ID, "Endowment")):
        stats = groups[group_id]
        net = stats["in"] - stats["out"]
        lines.append(
            f"• **{label}** ({stats['funds']} funds): "
            f"In ${stats['in']:,.2f} / Out ${stats['out']:,.2f} / "
            f"Net ${net:,.2f}")
    other = groups[None]
    if other["funds"]:
        net = other["in"] - other["out"]
        lines.append(
            f"• **{OTHER_GROUP_LABEL}** ({other['funds']} funds): "
            f"In ${other['in']:,.2f} / Out ${other['out']:,.2f} / "
            f"Net ${net:,.2f}")

    ordered = sorted(per_fund.values(),
                     key=lambda f: f["donations"], reverse=True)
    lines += ["", f"**Active Funds:** {len(per_fund)}", "",
              "**By Fund (top 15):**"]
    for entry in ordered[:15]:
        net = entry["donations"] - entry["grants"]
        net_str = f"+${net:,.2f}" if net >= 0 else f"-${abs(net):,.2f}"
        tag = f" [{entry['group_label']}]"
        lines.append(
            f"• **{entry['name']}**{tag}: In ${entry['donations']:,.2f} / "
            f"Out ${entry['grants']:,.2f} / Net {net_str}")

    if len(ordered) > 15:
        lines.append(f"• ... and {len(ordered) - 15} more funds")

    lines += ["",
              mirror_read.as_of_line("donation_fund_quarter", "grant", "fund")]
    return "\n".join(lines)


# =========================================================================
# H. DAF / ENDOWMENT INQUIRY SUMMARY
# =========================================================================

def _report_daf_inquiry_summary(query: str, hubspot) -> str:
    """Summarise recent DAF (and/or endowment) form submissions by month."""
    include_endowment = 'endowment' in query

    # Determine the month window
    now = datetime.now()
    if 'last month' in query:
        # First day of last month
        if now.month == 1:
            start = datetime(now.year - 1, 12, 1)
            end = datetime(now.year, 1, 1)
        else:
            start = datetime(now.year, now.month - 1, 1)
            end = datetime(now.year, now.month, 1)
        period_label = start.strftime("%B %Y")
    else:
        # Default: current month
        start = datetime(now.year, now.month, 1)
        end = now
        period_label = now.strftime("%B %Y")

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    # HubSpot is asked for the most recent N submissions per form, then they
    # are filtered to the month. If a form returns a full page, older
    # submissions inside the window may never have been fetched.
    SUBMISSION_LIMIT = 50
    capped_forms = []

    def _fetch_and_filter(fetch_fn, label):
        """Fetch submissions, filter to the date window, parse key fields."""
        try:
            resp = fetch_fn(limit=SUBMISSION_LIMIT)
            all_subs = resp.get('results', []) if isinstance(resp, dict) else []
        except Exception as e:
            logger.error(f"Error fetching {label} submissions: {e}")
            capped_forms.append(f"{label} (fetch failed: {e})")
            return []

        if len(all_subs) >= SUBMISSION_LIMIT:
            capped_forms.append(label)

        filtered = []
        for s in all_subs:
            submitted_at = s.get('submittedAt', 0)
            if start_ms <= submitted_at <= end_ms:
                values = {v['name'].lower(): v.get('value', '') for v in s.get('values', [])}
                filtered.append({
                    'first_name': values.get('firstname') or values.get('first_name', ''),
                    'last_name': values.get('lastname') or values.get('last_name', ''),
                    'email': values.get('email', ''),
                    'fund_name': values.get('fund_name') or values.get('requested_fund_name', ''),
                    'contribution': values.get('initial_contribution') or values.get('amount', ''),
                    'date': datetime.fromtimestamp(submitted_at / 1000).strftime('%b %d'),
                })
        return filtered

    daf_subs = _fetch_and_filter(hubspot.get_daf_inquiry_submissions, 'DAF')
    endowment_subs = (
        _fetch_and_filter(hubspot.get_endowment_inquiry_submissions, 'Endowment')
        if include_endowment else []
    )

    total = len(daf_subs) + len(endowment_subs)
    if total == 0:
        types = 'DAF or Endowment' if include_endowment else 'DAF'
        if capped_forms:
            return (
                f"⚠️ Couldn't read submissions for {', '.join(capped_forms)} — "
                f"I can't tell whether there were any {types} inquiries in "
                f"**{period_label}**. Try again shortly."
            )
        return f"📭 No {types} inquiry submissions found for **{period_label}**."

    lines = []
    if capped_forms:
        lines += [
            f"⚠️ Partial data: only the most recent {SUBMISSION_LIMIT} "
            f"submissions were fetched for {', '.join(capped_forms)} — "
            "counts below are NOT complete.",
            "",
        ]
    lines += [f"📋 **DAF Inquiry Summary — {period_label}**", ""]

    if daf_subs:
        lines.append(f"**DAF Inquiries ({len(daf_subs)}):**")
        for s in daf_subs:
            name = f"{s['first_name']} {s['last_name']}".strip() or s['email'] or 'Unknown'
            fund = s['fund_name'] or 'Fund name not provided'
            contrib = f" — ${s['contribution']}" if s['contribution'] else ''
            lines.append(f"• {s['date']}: **{name}** | {fund}{contrib}")
        lines.append("")

    if endowment_subs:
        lines.append(f"**Endowment Inquiries ({len(endowment_subs)}):**")
        for s in endowment_subs:
            name = f"{s['first_name']} {s['last_name']}".strip() or s['email'] or 'Unknown'
            fund = s['fund_name'] or 'Fund name not provided'
            contrib = f" — ${s['contribution']}" if s['contribution'] else ''
            lines.append(f"• {s['date']}: **{name}** | {fund}{contrib}")
        lines.append("")

    lines.append(f"**Total: {total} inquiry/inquiries in {period_label}**")

    return "\n".join(lines)


# =========================================================================
# I. TASK LIST BY PRIORITY
# =========================================================================

_PRIORITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
_ACTIVE_STATUSES = {"NOT_STARTED", "IN_PROGRESS"}


def _report_tasks(hubspot) -> str:
    """List active HubSpot tasks grouped by priority, sorted by due date."""
    logger.info("Fetching HubSpot tasks for priority report...")

    try:
        TASK_LIMIT = 50
        tasks_data = hubspot.get_tasks(limit=TASK_LIMIT)
        if 'results' not in tasks_data:
            return "❌ Failed to fetch tasks."
        all_tasks = tasks_data['results']
        tasks_complete = len(all_tasks) < TASK_LIMIT
    except Exception as e:
        return f"❌ Failed to fetch tasks: {e}"

    # Filter to active only
    active = []
    now = datetime.now()
    for t in all_tasks:
        props = t.get('properties', {})
        status = props.get('hs_task_status', 'NOT_STARTED')
        if status not in _ACTIVE_STATUSES:
            continue
        due_ms = props.get('hs_timestamp')
        due_dt = datetime.fromtimestamp(int(due_ms) / 1000) if due_ms else None
        active.append({
            "subject": props.get('hs_task_subject', 'Untitled task'),
            "status": status,
            "priority": props.get('hs_task_priority', 'MEDIUM').upper(),
            "due_dt": due_dt,
            "overdue": due_dt < now if due_dt else False,
        })

    if not active:
        return "✅ **No active tasks outstanding!**"

    # Sort: priority group first, then by due date (None = last)
    active.sort(key=lambda t: (
        _PRIORITY_ORDER.get(t['priority'], 1),
        t['due_dt'] or datetime.max,
    ))

    # Group by priority
    groups = {"HIGH": [], "MEDIUM": [], "LOW": []}
    for t in active:
        groups[t['priority']].append(t)

    priority_labels = {"HIGH": "🔴 High", "MEDIUM": "🟡 Medium", "LOW": "🟢 Low"}

    lines = []
    note = partial_note(len(all_tasks), tasks_complete, "tasks")
    if note:
        lines += [note, ""]
    lines += [f"📋 **Task List** ({len(active)} active)", ""]

    for level in ("HIGH", "MEDIUM", "LOW"):
        tasks = groups[level]
        if not tasks:
            continue
        lines.append(f"**{priority_labels[level]}**")
        for t in tasks:
            if t['due_dt']:
                due_str = t['due_dt'].strftime('%b %d')
                due_tag = f" ⚠️ Overdue ({due_str})" if t['overdue'] else f" · Due {due_str}"
            else:
                due_tag = ""
            status_tag = " *(In Progress)*" if t['status'] == 'IN_PROGRESS' else ""
            lines.append(f"• {t['subject']}{status_tag}{due_tag}")
        lines.append("")

    lines.append(f"🔗 [View all tasks in HubSpot]({hubspot.get_task_url()})")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Report: Investment Requests (Andalus) — Nora's ask
# ---------------------------------------------------------------------------

def _report_investment_requests(hubspot) -> str:
    """Compile incoming investment requests from the HubSpot Investment Request form."""
    logger.info("Running investment requests report...")

    try:
        INVESTMENT_LIMIT = 50
        resp = hubspot.get_investment_request_submissions(limit=INVESTMENT_LIMIT)
    except Exception as e:
        return f"❌ Failed to fetch investment requests: {e}"

    results = resp.get('results', []) if isinstance(resp, dict) else []
    if not results:
        return "No investment request submissions found."

    lines = []
    note = partial_note(len(results), len(results) < INVESTMENT_LIMIT,
                        "submissions")
    if note:
        lines += [note, ""]
    lines += [f"**Investment Requests** ({len(results)} found)\n"]

    for sub in results:
        # Parse submission timestamp
        submitted_ms = sub.get('submittedAt')
        if submitted_ms:
            submitted_dt = datetime.fromtimestamp(submitted_ms / 1000)
            date_str = submitted_dt.strftime('%b %d, %Y')
        else:
            date_str = 'Unknown date'

        # Extract field values
        values = sub.get('values', [])
        fields = {v.get('name', ''): v.get('value', '') for v in values}

        # Common field names — adjust if form uses different names
        name = fields.get('firstname', '') + ' ' + fields.get('lastname', '')
        name = name.strip() or fields.get('name', 'Unknown')
        email = fields.get('email', '')
        fund_name = fields.get('fund_name', fields.get('fund', ''))
        amount = fields.get('amount', fields.get('investment_amount', ''))

        line = f"- **{date_str}** — {name}"
        if email:
            line += f" ({email})"
        if fund_name:
            line += f" | Fund: {fund_name}"
        if amount:
            line += f" | Amount: ${amount}"

        # Append any remaining fields not already shown
        extras = {k: v for k, v in fields.items()
                  if k not in ('firstname', 'lastname', 'name', 'email',
                               'fund_name', 'fund', 'amount', 'investment_amount')
                  and v}
        if extras:
            extra_str = ", ".join(f"{k}: {v}" for k, v in list(extras.items())[:4])
            line += f" | {extra_str}"

        lines.append(line)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Report: Endowment Distribution Dates (#7)
# ---------------------------------------------------------------------------

def _report_endowment_distributions() -> str:
    """Open endowment funds and their distribution schedule.

    Every endowment, not the first 50 funds checked. The old version paged
    the fund list, then spent one funit/display call per fund up to a hard
    cap of fifty — against 397 funds, so it could only ever see an eighth
    of them and said "some endowments may not be shown". The
    mirror already holds every display payload.

    Distribution field names are from scripts/probe_output/csuite_fields.md
    (funit/display): dist_start_date, distribution_interval, dist_type_id.
    All three were null on the sampled fund, so "Not configured" here is a
    real answer about CSuite, not a lookup failure.
    """
    missing = mirror_read.require("fund")
    if missing:
        return missing

    logger.info("Running endowment distribution report (mirror)...")

    endowments = [
        fund for fund in mirror_read.rows("fund")
        if _fund_group_id(fund) == ENDOWMENT_GROUP_ID
        and not fund.get("fund_closed")
    ]

    footer = mirror_read.as_of_line("fund")

    if not endowments:
        return f"No open endowment funds found in the mirror.\n\n{footer}"

    endowments.sort(key=lambda f: f.get("dist_start_date") or "9999")

    lines = [f"**Endowment Funds** ({len(endowments)} found)", ""]

    for fund in endowments:
        name, code = split_fund_name(fund.get("fund_name"))
        suffix = f" ({code})" if code else ""
        balance = _money(fund.get("current_fundbalance")) or "N/A"
        interval = fund.get("distribution_interval")
        starts = fund.get("dist_start_date")

        line = f"- **{name or 'Unnamed'}**{suffix} — Balance: {balance}"
        if interval or starts:
            line += (f" | Distribution: {interval or 'interval not set'}, "
                     f"starts {starts or 'not set'}")
        else:
            line += " | Distribution: Not configured"
        lines.append(line)

    configured = [f for f in endowments
                  if f.get("distribution_interval") or f.get("dist_start_date")]
    if not configured:
        lines.append(
            "\n*Note: no endowment fund has a distribution schedule set in "
            "CSuite — the distribution fields are empty on all "
            f"{len(endowments)} of them.*")

    lines += ["", footer]
    return "\n".join(lines)
