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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger keywords (grouped by sub-handler)
# ---------------------------------------------------------------------------

_GRANT_TRIGGERS = [
    'how many grants', 'grants last quarter', 'grants this year',
    'grant report', 'grants processed', 'quarterly grants',
    'grants this quarter', 'grant summary',
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

def can_handle(query: str, **kwargs) -> bool:
    q = query.lower().strip()
    all_triggers = (
        _GRANT_TRIGGERS + _LAPSED_TRIGGERS + _INACTIVE_FUND_TRIGGERS +
        _NOT_CONTACTED_TRIGGERS + _FEE_TRIGGERS + _CHECK_TRIGGERS +
        _QUARTERLY_TRIGGERS + _DAF_INQUIRY_TRIGGERS + _TASK_TRIGGERS +
        _INVESTMENT_TRIGGERS + _ENDOWMENT_DIST_TRIGGERS
    )
    return any(t in q for t in all_triggers)


def handle(query: str, assistant) -> str:
    """Route to the appropriate report sub-handler."""
    q = query.lower().strip()
    hubspot = assistant.hubspot
    csuite = assistant.csuite

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
        all_grants = _fetch_all_grants(csuite)
    except Exception as e:
        logger.error(f"Error fetching grants: {e}")
        return f"❌ Failed to fetch grants: {e}"

    # Filter by date range
    filtered = []
    for g in all_grants:
        g_date = g.get('grant_date', '')
        if g_date and start <= g_date <= end:
            filtered.append(g)

    if not filtered:
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

    lines = [
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


def _fetch_all_grants(csuite, max_pages: int = 10) -> list:
    """Paginate through all grants from CSuite."""
    all_results = []
    offset = 0
    limit = 100

    for _ in range(max_pages):
        data = csuite.get_grants(limit=limit, offset=offset)
        if not data.get('success') or not data.get('data'):
            break
        results = data['data'].get('results', [])
        if not results:
            break
        all_results.extend(results)
        if len(results) < limit:
            break
        offset += limit

    logger.info(f"Fetched {len(all_results)} total grants")
    return all_results


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
        all_donations = _fetch_all_donations(csuite)
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

    lines = [
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


def _fetch_all_donations(csuite, max_pages: int = 10) -> list:
    """Paginate through all donations from CSuite."""
    all_results = []
    offset = 0
    limit = 100

    for _ in range(max_pages):
        data = csuite.get_donations(limit=limit, offset=offset)
        if not data.get('success') or not data.get('data'):
            break
        results = data['data'].get('results', [])
        if not results:
            break
        all_results.extend(results)
        if len(results) < limit:
            break
        offset += limit

    logger.info(f"Fetched {len(all_results)} total donations")
    return all_results


# =========================================================================
# C. INACTIVE FUNDS
# =========================================================================

def _report_inactive_funds(csuite) -> str:
    """Funds with no grant activity in 12+ months."""
    logger.info("Running inactive funds analysis...")

    try:
        all_funds_data = csuite.get_funds(limit=200)
        if not all_funds_data.get('success') or not all_funds_data.get('data'):
            return "❌ Failed to fetch funds."
        funds = all_funds_data['data'].get('results', [])
    except Exception as e:
        return f"❌ Failed to fetch funds: {e}"

    try:
        all_grants = _fetch_all_grants(csuite)
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

    lines = [
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

def _report_fees(query: str, csuite) -> str:
    """Calculate fees for a fund or show fee structure."""
    logger.info("Running fee calculation...")

    # Try to extract a fund ID from the query
    import re
    id_match = re.search(r'\b(\d{2,})\b', query)
    fund_id = id_match.group(1) if id_match else None

    # Fetch fee types
    try:
        fee_data = csuite.get_fund_fee_types()
        if not fee_data.get('success') or not fee_data.get('data'):
            return "❌ Failed to fetch fee structure."
        fee_types = fee_data['data'].get('results', [])
    except Exception as e:
        return f"❌ Failed to fetch fee types: {e}"

    # If a specific fund, calculate its fee
    if fund_id:
        try:
            fund_data = csuite.get_fund(fund_id)
            if fund_data.get('success') and fund_data.get('data'):
                fund = fund_data['data']
                balance = float(fund.get('balance', 0) or 0)
                fund_name = fund.get('fund_name', 'Unknown')

                fee_estimate = _calculate_fee(balance, fee_types)

                return f"""📊 **Fee Estimate: {fund_name}**

💰 **Balance:** ${balance:,.2f}
📋 **Estimated quarterly fee:** ${fee_estimate:,.2f}
📅 **Annualised:** ${fee_estimate * 4:,.2f}

*Based on current fee structure. Actual fees may vary.*"""
        except Exception as e:
            logger.error(f"Error fetching fund for fee calc: {e}")

    # No specific fund — show fee structure
    lines = [
        "📊 **AMCF Fee Structure**",
        "",
    ]
    for ft in fee_types:
        name = ft.get('fee_name', 'Unknown')
        pct = ft.get('fee_percent', '?')
        min_fee = ft.get('min_fee', '0')
        lines.append(f"• **{name}:** {pct}% (minimum: ${min_fee})")

    lines.append("")
    lines.append('💡 *To calculate fees for a specific fund, try: "Calculate fees for fund 1234"*')

    return "\n".join(lines)


def _calculate_fee(balance: float, fee_types: list) -> float:
    """
    Estimate quarterly fee based on balance and fee structure.
    Uses the first fee type as default (DAF admin fee).
    """
    if not fee_types:
        return 0.0

    # Use first fee type as default
    ft = fee_types[0]
    pct = float(ft.get('fee_percent', 0) or 0)
    min_fee = float(ft.get('min_fee', 0) or 0)

    calculated = balance * (pct / 100) / 4  # quarterly
    return max(calculated, min_fee)


# =========================================================================
# F. UNCASHED CHECKS
# =========================================================================

def _report_uncashed_checks(csuite) -> str:
    """List uncashed grant checks grouped by recipient."""
    logger.info("Running uncashed checks report...")

    try:
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

    capped_note = ""
    if len(checks) >= 500:
        capped_note = "\n*Showing oldest 500 uncashed checks — contact Finance for full export.*\n"

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
        all_donations = _fetch_all_donations(csuite)
        all_grants = _fetch_all_grants(csuite)
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

    lines = [
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

    def _fetch_and_filter(fetch_fn, label):
        """Fetch submissions, filter to the date window, parse key fields."""
        try:
            resp = fetch_fn(limit=50)
            all_subs = resp.get('results', []) if isinstance(resp, dict) else []
        except Exception as e:
            logger.error(f"Error fetching {label} submissions: {e}")
            return []

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
        return f"📭 No {types} inquiry submissions found for **{period_label}**."

    lines = [f"📋 **DAF Inquiry Summary — {period_label}**", ""]

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
        tasks_data = hubspot.get_tasks(limit=50)
        if 'results' not in tasks_data:
            return "❌ Failed to fetch tasks."
        all_tasks = tasks_data['results']
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

    lines = [f"📋 **Task List** ({len(active)} active)", ""]

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
        resp = hubspot.get_investment_request_submissions(limit=50)
    except Exception as e:
        return f"❌ Failed to fetch investment requests: {e}"

    results = resp.get('results', []) if isinstance(resp, dict) else []
    if not results:
        return "No investment request submissions found."

    lines = [f"**Investment Requests** ({len(results)} found)\n"]

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

    MAX_DETAIL_CALLS = 20

    # Step 1: Search for endowment funds by name (avoids iterating all 379 funds)
    try:
        search_result = csuite.search_funds("Endowments")
    except Exception as e:
        return f"Failed to search endowment funds: {e}"

    results = search_result.get("data", {}).get("results", []) if search_result.get("success") else []

    # Filter to actual endowment funds (fullname contains ":: Endowments")
    endowment_ids = [
        r.get("id") for r in results
        if "endowment" in (r.get("fullname") or r.get("name") or "").lower()
        and r.get("id")
    ]

    if not endowment_ids:
        return "No endowment funds found in CSuite."

    # Step 2: Fetch details for up to MAX_DETAIL_CALLS funds
    endowments = []
    for fid in endowment_ids[:MAX_DETAIL_CALLS]:
        try:
            detail = csuite.get_fund(fid)
        except Exception:
            continue

        if not detail.get("success") or not detail.get("data"):
            continue

        fund_data = detail["data"]
        if fund_data.get("fund_closed"):
            continue

        endowments.append(fund_data)

    if not endowments:
        return "No open endowment funds found in CSuite."

    # Sort by distribution start date (if available)
    endowments.sort(key=lambda f: f.get("dist_start_date") or "9999")

    lines = [f"**Endowment Funds** ({len(endowments)} found)\n"]

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

    if len(endowment_ids) > MAX_DETAIL_CALLS:
        lines.append(
            f"\n*Showing first {MAX_DETAIL_CALLS} of {len(endowment_ids)} endowment funds.*"
        )

    return "\n".join(lines)