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
  F. Uncashed checks
  G. Quarterly DAF summary
"""

import logging
from datetime import datetime, timedelta
from config import Config
from intents.queries import (
    _fund_row_names,
    extract_fund_ref,
    resolve_fund_id,
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
    """Route to the appropriate report sub-handler."""
    q = query.lower().strip()
    hubspot = ctx.services.hubspot
    csuite = ctx.services.csuite

    if any(t in q for t in _GRANT_TRIGGERS):
        return _report_grants(q, csuite)

    if any(t in q for t in _LAPSED_TRIGGERS):
        return _report_lapsed_donors(q, csuite, hubspot)

    if any(t in q for t in _INACTIVE_FUND_TRIGGERS):
        return _report_inactive_funds(csuite)

    if any(t in q for t in _NOT_CONTACTED_TRIGGERS):
        return _report_not_contacted(hubspot, csuite)

    if any(t in q for t in _FEE_TRIGGERS):
        return _report_fees(q, csuite)

    if any(t in q for t in _CHECK_TRIGGERS):
        return _report_uncashed_checks(csuite)

    if any(t in q for t in _QUARTERLY_TRIGGERS):
        return _report_quarterly_summary(q, csuite)

    if any(t in q for t in _DAF_INQUIRY_TRIGGERS):
        return _report_daf_inquiry_summary(q, hubspot)

    if any(t in q for t in _TASK_TRIGGERS):
        return _report_tasks(hubspot)

    if any(t in q for t in _INVESTMENT_TRIGGERS):
        return _report_investment_requests(hubspot)

    if any(t in q for t in _ENDOWMENT_DIST_TRIGGERS):
        return _report_endowment_distributions(csuite)

    return "❌ Report type not recognised."


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

def _report_grants(query: str, csuite) -> str:
    """Grant counts and totals for a date range."""
    start, end = _parse_date_range(query)
    label = f"{start} to {end}"

    logger.info(f"Grant report for {label}...")

    try:
        all_grants, grants_complete = _fetch_all_grants(csuite)
    except Exception as e:
        logger.exception(f"Error fetching grants: {e}")
        return f"❌ Failed to fetch grants: {e}"

    # Filter by date range
    filtered = []
    for g in all_grants:
        g_date = g.get('grant_date', '')
        if g_date and start <= g_date <= end:
            filtered.append(g)

    if not filtered:
        # An empty result after a capped fetch is a false negative: the
        # grants for this window may simply never have been read.
        note = partial_note(len(all_grants), grants_complete, "grants")
        if note:
            return (
                f"{note}\n\n📊 No grants found for **{label}** in the records "
                "I could read — there may be more."
            )
        return f"📊 No grants found for **{label}**."

    # Aggregate
    total_amount = sum(float(g.get('grant_amount', 0) or 0) for g in filtered)
    count = len(filtered)

    # Group by fund
    by_fund = {}
    for g in filtered:
        fund = g.get('fund_name', 'Unknown')
        by_fund.setdefault(fund, {"count": 0, "total": 0})
        by_fund[fund]["count"] += 1
        by_fund[fund]["total"] += float(g.get('grant_amount', 0) or 0)

    # Sort by total descending
    sorted_funds = sorted(by_fund.items(), key=lambda x: x[1]["total"], reverse=True)

    lines = []
    note = partial_note(len(all_grants), grants_complete, "grants")
    if note:
        lines += [note, ""]
    lines += [
        f"📊 **Grant Report: {label}**",
        "",
        f"**Total:** {count} grants totalling **${total_amount:,.2f}**",
        "",
        "**By Fund (top 10):**",
    ]
    for fund_name, stats in sorted_funds[:10]:
        lines.append(f"• {fund_name}: {stats['count']} grants, ${stats['total']:,.2f}")

    if len(sorted_funds) > 10:
        lines.append(f"• ... and {len(sorted_funds) - 10} more funds")

    return "\n".join(lines)


def partial_note(count: int, complete: bool, label: str = "records") -> str | None:
    """The banner a capped fetch must carry, or None when the data is whole.

    Every one of these reports used to print a total as if it covered
    everything, while the fetch behind it stopped at a page cap. A number
    that is quietly a lower bound is worse than no number.
    """
    if complete:
        return None
    return (f"⚠️ Partial data: first {count} {label} only — "
            f"totals below are NOT complete.")


def _fetch_all_grants(csuite, max_pages: int = 10):
    """Paginate through grants. Returns (records, complete)."""
    all_results = []
    offset = 0
    limit = 100
    complete = False

    for _ in range(max_pages):
        data = csuite.get_grants(limit=limit, offset=offset)
        if not data.get('success') or not data.get('data'):
            # A failed page means we do not know what we are missing.
            break
        results = data['data'].get('results', [])
        if not results:
            complete = True
            break
        all_results.extend(results)
        if len(results) < limit:
            complete = True
            break
        offset += limit

    logger.info(f"Fetched {len(all_results)} total grants (complete={complete})")
    return all_results, complete


# =========================================================================
# B. RAMADAN LAPSED DONORS
# =========================================================================

def _report_lapsed_donors(query: str, csuite, hubspot) -> str:
    """Donors who gave during prior Ramadan but not the current one."""
    logger.info("Running Ramadan lapsed donor analysis...")

    now = datetime.now()
    current_range = Config.get_ramadan_range(now.year)
    prior_range = Config.get_ramadan_range(now.year - 1)

    try:
        all_donations, donations_complete = _fetch_all_donations(csuite)
    except Exception as e:
        logger.error(f"Error fetching donations: {e}")
        return f"❌ Failed to fetch donations: {e}"

    # Bucket by profile
    prior_donors = set()
    current_donors = set()

    for d in all_donations:
        d_date = d.get('donation_date', '')
        profile_id = d.get('profile_id') or d.get('name', 'Unknown')
        if d_date and prior_range[0] <= d_date <= prior_range[1]:
            prior_donors.add(profile_id)
        if d_date and current_range[0] <= d_date <= current_range[1]:
            current_donors.add(profile_id)

    lapsed = prior_donors - current_donors

    if not lapsed:
        return (
            f"✅ **No lapsed Ramadan donors!** Everyone who gave during "
            f"Ramadan {prior_range[0][:4]} has also given in {current_range[0][:4]} so far."
        )

    # Try to enrich with names/emails from HubSpot
    lapsed_details = []
    for pid in list(lapsed)[:20]:
        detail = {"profile_id": pid, "name": str(pid), "email": ""}
        try:
            # Find matching donation record for the name
            for d in all_donations:
                if (d.get('profile_id') or d.get('name', '')) == pid:
                    detail["name"] = d.get('name', str(pid))
                    break
        except Exception:
            pass
        lapsed_details.append(detail)

    lines = []
    note = partial_note(len(all_donations), donations_complete, "donations")
    if note:
        lines += [note, ""]
    lines += [
        f"📊 **Ramadan Lapsed Donors**",
        "",
        f"**{len(lapsed)}** donors gave during Ramadan {prior_range[0][:4]} "
        f"but have **not yet** given in Ramadan {current_range[0][:4]}.",
        "",
        "**Donors to re-engage (up to 20):**",
    ]
    for ld in lapsed_details:
        lines.append(f"• {ld['name']} (Profile: {ld['profile_id']})")

    if len(lapsed) > 20:
        lines.append(f"• ... and {len(lapsed) - 20} more")

    lines.append("")
    lines.append("💡 *Consider a targeted outreach campaign for these donors.*")

    return "\n".join(lines)


def _fetch_all_donations(csuite, max_pages: int = 10):
    """Paginate through donations. Returns (records, complete)."""
    all_results = []
    offset = 0
    limit = 100
    complete = False

    for _ in range(max_pages):
        data = csuite.get_donations(limit=limit, offset=offset)
        if not data.get('success') or not data.get('data'):
            break
        results = data['data'].get('results', [])
        if not results:
            complete = True
            break
        all_results.extend(results)
        if len(results) < limit:
            complete = True
            break
        offset += limit

    logger.info(f"Fetched {len(all_results)} total donations (complete={complete})")
    return all_results, complete


# =========================================================================
# C. INACTIVE FUNDS
# =========================================================================

def _report_inactive_funds(csuite) -> str:
    """Funds with no grant activity in 12+ months."""
    logger.info("Running inactive funds analysis...")

    try:
        FUND_PAGE_LIMIT = 200
        all_funds_data = csuite.get_funds(limit=FUND_PAGE_LIMIT)
        if not all_funds_data.get('success') or not all_funds_data.get('data'):
            return "❌ Failed to fetch funds."
        funds = all_funds_data['data'].get('results', [])
        funds_complete = len(funds) < FUND_PAGE_LIMIT
    except Exception as e:
        return f"❌ Failed to fetch funds: {e}"

    try:
        all_grants, grants_complete = _fetch_all_grants(csuite)
    except Exception as e:
        return f"❌ Failed to fetch grants: {e}"

    # Build map: fund_id → last grant date
    last_grant = {}
    for g in all_grants:
        fid = g.get('funit_id') or g.get('fund_id')
        g_date = g.get('grant_date', '')
        if fid and g_date:
            if fid not in last_grant or g_date > last_grant[fid]:
                last_grant[fid] = g_date

    cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    inactive = []

    for f in funds:
        fid = str(f.get('funit_id', ''))
        fund_name = f.get('fund_name', 'Unknown')
        lg = last_grant.get(fid)
        if lg is None:
            inactive.append((fund_name, fid, "Never"))
        elif lg < cutoff:
            inactive.append((fund_name, fid, lg))

    if not inactive:
        return "✅ **All funds have had grant activity in the last 12 months!**"

    # Sort: never first, then oldest
    inactive.sort(key=lambda x: x[2] if x[2] != "Never" else "0000")

    lines = []
    if not funds_complete:
        lines += [partial_note(len(funds), False, "funds"), ""]
    elif not grants_complete:
        lines += [partial_note(len(all_grants), False, "grants"), ""]
    lines += [
        f"📊 **Inactive Funds** (no grants in 12+ months)",
        "",
        f"**{len(inactive)}** funds identified:",
        "",
    ]
    for name, fid, last in inactive[:25]:
        lines.append(f"• **{name}** (ID: {fid}) — Last grant: {last}")

    if len(inactive) > 25:
        lines.append(f"• ... and {len(inactive) - 25} more")

    lines.append("")
    lines.append("💡 *Consider reaching out to fund advisors to discuss grant recommendations.*")

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


def _fund_balance_line(query: str, csuite) -> str | None:
    """A one-line balance for the fund named in the query, if there is one."""
    try:
        fund_id, rows, error = resolve_fund_id(csuite, query)
    except Exception as e:
        logger.error(f"Fee report fund lookup failed: {e}")
        return None

    if error:
        return f"⚠️ {error}"

    if fund_id is None:
        if rows:
            names = ", ".join(
                (_fund_row_names(r) or ["Unknown"])[0] for r in rows[:5])
            return (
                f"⚠️ That fund name matched {len(rows)} funds ({names}). "
                "Name one exactly, or use its code."
            )
        return None

    try:
        data = csuite.get_fund(fund_id)
    except Exception as e:
        logger.error(f"Fee report fund fetch failed: {e}")
        return f"⚠️ Could not fetch fund {fund_id}: {e}"

    if not data.get("success") or not data.get("data"):
        return (f"⚠️ Could not fetch fund {fund_id}: "
                f"{data.get('error', 'unknown error')}")

    fund = data["data"]
    name, code = split_fund_name(fund.get("fund_name"))
    balance = _money(fund.get("current_fundbalance")) or "unknown"
    suffix = f" ({code})" if code else ""
    return f"💰 **{name or 'Fund'}**{suffix} — balance {balance}"


def _report_fees(query: str, csuite) -> str:
    """Show the fee structure, and the balance of a named fund if given."""
    logger.info("Running fee report...")

    try:
        fee_data = csuite.get_fund_fee_types()
        if not fee_data.get('success') or not fee_data.get('data'):
            return "❌ Failed to fetch fee structure."
        fee_types = fee_data['data'].get('results', [])
    except Exception as e:
        return f"❌ Failed to fetch fee types: {e}"

    # If the query names a fund, show its balance beside the table. No fee is
    # computed for it: which fee type applies is exactly what CSuite will not
    # tell us, so a number here would be a guess presented as an answer.
    fund_line = _fund_balance_line(query, csuite)

    lines = []
    if fund_line:
        lines += [fund_line, ""]
    lines += ["📊 **AMCF Fee Structure**", ""]

    if fee_types:
        for ft in fee_types:
            lines.append(_format_fee_type(ft))
    else:
        lines.append("_CSuite returned no fee types._")

    lines.append("")
    lines.append(_FEE_JOIN_NOTE)

    if not fund_line:
        lines.append("")
        lines.append(
            '💡 *To see a specific fund\'s balance, try: '
            '"Fees for fund 1234" or "Fees for END0026".*'
        )

    return "\n".join(lines)


# =========================================================================
# F. UNCASHED CHECKS
# =========================================================================

def _report_uncashed_checks(csuite) -> str:
    """List uncashed grant checks grouped by recipient."""
    logger.info("Running uncashed checks report...")

    try:
        # The client scans at most 5 pages (500 checks) and returns a
        # filtered list, so completeness is not recoverable from the result.
        # The report says what was scanned rather than implying "all".
        CHECK_SCAN_LIMIT = 500
        checks = csuite.get_uncashed_checks()
    except Exception as e:
        return f"❌ Failed to fetch uncashed checks: {e}"

    if not checks:
        return "✅ **No uncashed checks outstanding!**"

    # Group by account
    by_account = {}
    total = 0
    for c in checks:
        account = c.get('account_name', 'Unknown Account')
        amount = float(c.get('amount', 0) or 0)
        total += amount
        by_account.setdefault(account, []).append({
            "number": c.get('check_num') or c.get('check_id', '?'),
            "amount": amount,
            "date": c.get('check_date', 'N/A'),
            "electronic": c.get('is_electronic', 0),
        })

    # Sort accounts by total outstanding
    sorted_accounts = sorted(
        by_account.items(),
        key=lambda x: sum(ch["amount"] for ch in x[1]),
        reverse=True,
    )

    # The old condition (len(checks) >= 500) almost never fired: the client
    # scans 500 checks and then FILTERS to uncashed ones, so a fully capped
    # scan typically returns far fewer than 500 and the report read as
    # complete. The scan limit is stated unconditionally instead.
    capped_note = (
        f"\n⚠️ Partial data: scanned the most recent {CHECK_SCAN_LIMIT} checks "
        f"only — totals below are NOT complete. Contact Finance for a full "
        f"export.\n"
    )

    lines = [
        f"📊 **Uncashed Checks Report**",
        "",
        f"**{len(checks)}** checks outstanding totalling **${total:,.2f}**",
        capped_note,
        "**By Account:**",
    ]

    for account, account_checks in sorted_accounts[:15]:
        account_total = sum(ch["amount"] for ch in account_checks)
        e_count = sum(1 for ch in account_checks if ch["electronic"])
        type_note = f" ({e_count} electronic)" if e_count else ""
        lines.append(f"• **{account}** — {len(account_checks)} check(s), ${account_total:,.2f}{type_note}")
        for ch in account_checks[:3]:
            lines.append(f"  Check {ch['number']}: ${ch['amount']:,.2f} ({ch['date']})")
        if len(account_checks) > 3:
            lines.append(f"  ... and {len(account_checks) - 3} more")

    if len(sorted_accounts) > 15:
        lines.append(f"• ... and {len(sorted_accounts) - 15} more accounts")

    lines.append("")
    lines.append("💡 *Consider following up on older uncashed checks.*")

    return "\n".join(lines)


# =========================================================================
# G. QUARTERLY DAF SUMMARY
# =========================================================================

def _report_quarterly_summary(query: str, csuite) -> str:
    """Quarterly summary: donations in, grants out, net per fund."""
    start, end = _parse_date_range(query)
    q_label = _get_quarter_label(start)

    logger.info(f"Quarterly summary for {q_label} ({start} to {end})...")

    try:
        all_donations, donations_complete = _fetch_all_donations(csuite)
        all_grants, grants_complete = _fetch_all_grants(csuite)
    except Exception as e:
        return f"❌ Failed to fetch data: {e}"

    # Filter to date range
    q_donations = [d for d in all_donations if start <= d.get('donation_date', '') <= end]
    q_grants = [g for g in all_grants if start <= g.get('grant_date', '') <= end]

    # Aggregate by fund
    funds = {}
    for d in q_donations:
        fund = d.get('fund_name', 'Unknown')
        funds.setdefault(fund, {"donations": 0, "grants": 0})
        funds[fund]["donations"] += float(d.get('donation_amount', 0) or 0)

    for g in q_grants:
        fund = g.get('fund_name', 'Unknown')
        funds.setdefault(fund, {"donations": 0, "grants": 0})
        funds[fund]["grants"] += float(g.get('grant_amount', 0) or 0)

    total_in = sum(f["donations"] for f in funds.values())
    total_out = sum(f["grants"] for f in funds.values())

    # Sort by donations descending
    sorted_funds = sorted(funds.items(), key=lambda x: x[1]["donations"], reverse=True)

    lines = []
    if not donations_complete or not grants_complete:
        lines += [
            "⚠️ Partial data: donations capped at "
            f"{len(all_donations)} and grants at {len(all_grants)} — "
            "totals below are NOT complete.",
            "",
        ]
    lines += [
        f"📊 **Quarterly DAF Summary: {q_label}**",
        f"📅 {start} to {end}",
        "",
        f"💰 **Total Donations In:** ${total_in:,.2f}",
        f"🎁 **Total Grants Out:** ${total_out:,.2f}",
        f"📈 **Net:** ${total_in - total_out:,.2f}",
        "",
        f"**Active Funds:** {len(funds)}",
        "",
        "**By Fund (top 15):**",
    ]

    for fund_name, stats in sorted_funds[:15]:
        net = stats["donations"] - stats["grants"]
        net_str = f"+${net:,.2f}" if net >= 0 else f"-${abs(net):,.2f}"
        lines.append(
            f"• **{fund_name}**: In ${stats['donations']:,.2f} / "
            f"Out ${stats['grants']:,.2f} / Net {net_str}"
        )

    if len(sorted_funds) > 15:
        lines.append(f"• ... and {len(sorted_funds) - 15} more funds")

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

def _report_endowment_distributions(csuite) -> str:
    """List endowment funds with their distribution schedule and dates."""
    logger.info("Running endowment distribution report...")

    ENDOWMENT_FGROUP_ID = 1008
    MAX_DETAIL_CALLS = 50  # Budget for detail calls (24 endowments expected)

    # Step 1: Get all fund IDs from the paginated fund list.
    # The list endpoint doesn't include fgroup_id, so we must fetch
    # details to filter. We iterate all funds but stop detail calls
    # at MAX_DETAIL_CALLS to protect workers.
    all_fund_ids = []
    try:
        result = csuite.get_funds(limit=100, offset=0)
        if result.get("success") and result.get("data"):
            fund_list = result["data"].get("results", [])
            all_fund_ids = [f.get("funit_id") for f in fund_list if f.get("funit_id")]
            total_pages = result["data"].get("pages", 1)

            for page in range(1, min(total_pages, 4)):
                more = csuite.get_funds(limit=100, offset=page * 100)
                if more.get("success") and more.get("data"):
                    for f in more["data"].get("results", []):
                        if f.get("funit_id"):
                            all_fund_ids.append(f["funit_id"])
    except Exception as e:
        logger.exception(f"Error fetching fund list: {e}")
        return f"Failed to fetch funds: {e}"

    if not all_fund_ids:
        return "No funds found in CSuite."

    logger.info(f"Found {len(all_fund_ids)} total funds, checking for endowments...")

    # Step 2: Fetch details and filter to endowments (fgroup_id=1008).
    endowments = []
    calls_made = 0
    for fid in all_fund_ids:
        if calls_made >= MAX_DETAIL_CALLS:
            logger.warning(f"Hit detail call cap ({MAX_DETAIL_CALLS}) — found {len(endowments)} endowments so far")
            break
        try:
            detail = csuite.get_fund(fid)
            calls_made += 1
        except Exception:
            calls_made += 1
            continue

        if not detail.get("success") or not detail.get("data"):
            continue

        fund_data = detail["data"]
        if fund_data.get("fgroup_id") != ENDOWMENT_FGROUP_ID:
            continue
        if fund_data.get("fund_closed"):
            continue

        endowments.append(fund_data)

    if not endowments:
        return "No open endowment funds found in CSuite."

    # Sort by distribution start date (if available)
    endowments.sort(key=lambda f: f.get("dist_start_date") or "9999")

    lines = []
    if calls_made >= MAX_DETAIL_CALLS:
        lines += [
            f"⚠️ Partial data: checked {calls_made} of {len(all_fund_ids)} "
            "funds only — the list below is NOT complete.",
            "",
        ]
    lines += [f"**Endowment Funds** ({len(endowments)} found)\n"]

    for f in endowments:
        name = f.get("fund_name", "Unnamed")
        balance = f.get("current_fundbalance", "N/A")
        dist_interval = f.get("distribution_interval") or "Not set"
        dist_start = f.get("dist_start_date") or "Not set"
        dist_type = f.get("dist_type_id")

        line = f"- **{name}** — Balance: ${balance}"
        if dist_interval != "Not set" or dist_start != "Not set":
            line += f" | Distribution: {dist_interval}, starts {dist_start}"
        else:
            line += " | Distribution: Not configured"

        lines.append(line)

    # Check if no endowments have distributions configured
    configured = [f for f in endowments
                  if f.get("distribution_interval") or f.get("dist_start_date")]
    if not configured:
        lines.append(
            "\n*Note: No endowment funds currently have distribution schedules configured in CSuite.*"
        )

    if calls_made >= MAX_DETAIL_CALLS:
        lines.append(
            f"\n*Note: Checked {calls_made} of {len(all_fund_ids)} funds. "
            f"Some endowments may not be shown.*"
        )

    return "\n".join(lines)