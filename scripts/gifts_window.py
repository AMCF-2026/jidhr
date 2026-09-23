"""
Gifts in a Window
=================
What came in between two dates, from the mirror. No CSuite call.

    python scripts/gifts_window.py --from 2025-11-15 --to 2025-12-31
    python scripts/gifts_window.py --from 2026-02-15 --to 2026-04-15 --lapsed

Reads the `donation` rows the nightly mirror_refresh has stored since the
2026-09-17 reversal (sync/mirror.py) plus `donation_agg` for first-time
and lapsed, and `fund` for names. Test and System funds are excluded the
way every other mirror report excludes them.

Definitions
    first-time   the profile's very first recorded gift (donation_agg
                 .first_date) falls inside the window
    repeat       it does not — they had given before --from
    lapsed       (--lapsed only) profiles with a gift inside the window
                 and no gift after --to, per donation_agg.latest_date.
                 Output is profile_id only: a list of ids is a work list,
                 a list of names in a terminal is a leak.

Money is cast to Decimal, never float: a window's total is the kind of
number someone repeats to a board.

Exit codes: 0 · 1 bad arguments · 2 mirror not loaded.
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from statistics import median

# Make the repo root importable when run as `python scripts/gifts_window.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients import mirror_read  # noqa: E402

EXIT_OK = 0
EXIT_BAD_ARGS = 1
EXIT_NOT_LOADED = 2

TOP_FUNDS = 15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def money(value) -> Decimal:
    """A stored amount as a Decimal; unparsable amounts count as zero."""
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value).replace("$", "").replace(",", "").strip())
    except InvalidOperation:
        return Decimal("0")


def dollars(amount: Decimal) -> str:
    return f"${amount.quantize(Decimal('0.01')):,}"


def parse_day(text: str) -> str:
    """YYYY-MM-DD, validated as a real date, returned as ISO text."""
    try:
        return date.fromisoformat(text).isoformat()
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"{text!r} is not a YYYY-MM-DD date")


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def gifts_between(start: str, end: str) -> list:
    """Donation rows dated within [start, end], test/system funds excluded."""
    return mirror_read.rows(
        "donation",
        "AND data->>'donation_date' >= %s AND data->>'donation_date' <= %s",
        (start, end),
    )


def aggregates_for(profile_ids) -> dict:
    """{profile_id: donation_agg row} for the profiles that gave."""
    wanted = {str(p) for p in profile_ids if p not in (None, "")}
    if not wanted:
        return {}
    # One read of the aggregate table beats one get() per profile once a
    # window has more than a handful of donors.
    return {str(row.get("csuite_id")): row
            for row in mirror_read.rows("donation_agg")
            if str(row.get("csuite_id")) in wanted}


def fund_names() -> dict:
    """{funit_id: display name} from the mirrored fund rows."""
    from intents.queries import split_fund_name

    names = {}
    for fund in mirror_read.rows("fund"):
        clean, code = split_fund_name(fund.get("fund_name"))
        label = clean or fund.get("fund_name") or "Unknown"
        if code:
            label = f"{label} ({code})"
        names[str(fund.get("csuite_id"))] = label
    return names


# ---------------------------------------------------------------------------
# The numbers
# ---------------------------------------------------------------------------

def summarise(gifts: list, aggregates: dict, start: str, end: str) -> dict:
    amounts = [money(g.get("donation_amount")) for g in gifts]
    total = sum(amounts, Decimal("0"))

    by_fund = defaultdict(lambda: {"count": 0, "total": Decimal("0")})
    by_day = defaultdict(lambda: {"count": 0, "total": Decimal("0")})
    first_time = {"count": 0, "total": Decimal("0"), "profiles": set()}
    repeat = {"count": 0, "total": Decimal("0"), "profiles": set()}
    unknown = {"count": 0, "total": Decimal("0")}

    for gift, amount in zip(gifts, amounts):
        fund_id = str(gift.get("funit_id") or "")
        by_fund[fund_id]["count"] += 1
        by_fund[fund_id]["total"] += amount

        day = str(gift.get("donation_date") or "")[:10]
        by_day[day]["count"] += 1
        by_day[day]["total"] += amount

        profile_id = str(gift.get("profile_id") or "")
        agg = aggregates.get(profile_id)
        first_date = (agg or {}).get("first_date")
        if not agg or not first_date:
            unknown["count"] += 1
            unknown["total"] += amount
        elif start <= str(first_date)[:10] <= end:
            first_time["count"] += 1
            first_time["total"] += amount
            first_time["profiles"].add(profile_id)
        else:
            repeat["count"] += 1
            repeat["total"] += amount
            repeat["profiles"].add(profile_id)

    return {
        "count": len(gifts),
        "total": total,
        "median": median(amounts) if amounts else Decimal("0"),
        "donors": len({str(g.get("profile_id")) for g in gifts
                       if g.get("profile_id") not in (None, "")}),
        "by_fund": dict(by_fund),
        "by_day": dict(by_day),
        "first_time": first_time,
        "repeat": repeat,
        "unknown": unknown,
    }


def lapsed_profiles(gifts: list, aggregates: dict, end: str) -> list:
    """profile_ids with a gift in the window and none after `end`."""
    lapsed = set()
    for gift in gifts:
        profile_id = str(gift.get("profile_id") or "")
        if not profile_id:
            continue
        agg = aggregates.get(profile_id)
        latest = (agg or {}).get("latest_date")
        if not agg or not latest:
            # No aggregate means nothing is known about what came later;
            # a missing fact is not a lapse.
            continue
        if str(latest)[:10] <= end:
            lapsed.add(profile_id)
    return sorted(lapsed, key=lambda p: (len(p), p))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render(summary: dict, start: str, end: str, names: dict,
           footer: str) -> str:
    lines = [f"🎁 Gifts {start} to {end}", ""]
    if not summary["count"]:
        lines += ["No gifts in the mirror for this window.", "", footer]
        return "\n".join(lines)

    lines += [
        f"Gifts:    {summary['count']:,}",
        f"Total:    {dollars(summary['total'])}",
        f"Median:   {dollars(summary['median'])}",
        f"Donors:   {summary['donors']:,}",
        "",
    ]

    ft, rp, un = summary["first_time"], summary["repeat"], summary["unknown"]
    lines += ["First-time vs repeat (first-time = the profile's first ever "
              "gift falls in this window):"]
    lines.append(f"  first-time  {ft['count']:>6,} gifts  "
                 f"{dollars(ft['total']):>14}  {len(ft['profiles']):,} donors")
    lines.append(f"  repeat      {rp['count']:>6,} gifts  "
                 f"{dollars(rp['total']):>14}  {len(rp['profiles']):,} donors")
    if un["count"]:
        lines.append(f"  unknown     {un['count']:>6,} gifts  "
                     f"{dollars(un['total']):>14}  (no aggregate row for "
                     "the donor)")
    lines.append("")

    funds = sorted(summary["by_fund"].items(),
                   key=lambda kv: (-kv[1]["total"], kv[0]))
    lines.append(f"By fund (top {min(TOP_FUNDS, len(funds))} of {len(funds)}):")
    for fund_id, stats in funds[:TOP_FUNDS]:
        label = names.get(fund_id) or f"fund {fund_id or '?'}"
        lines.append(f"  {dollars(stats['total']):>14}  {stats['count']:>5,}  "
                     f"{label}")
    if len(funds) > TOP_FUNDS:
        rest = funds[TOP_FUNDS:]
        lines.append(f"  {dollars(sum((s['total'] for _, s in rest), Decimal('0'))):>14}  "
                     f"{sum(s['count'] for _, s in rest):>5,}  "
                     f"... {len(rest)} more funds")
    lines.append("")

    lines.append("By day:")
    for day, stats in sorted(summary["by_day"].items()):
        lines.append(f"  {day}  {stats['count']:>5,}  "
                     f"{dollars(stats['total']):>14}")
    lines += ["", footer]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Gifts in a date window, from the mirror. No CSuite call.")
    parser.add_argument("--from", dest="start", type=parse_day, required=True,
                        help="First day, YYYY-MM-DD (inclusive).")
    parser.add_argument("--to", dest="end", type=parse_day, required=True,
                        help="Last day, YYYY-MM-DD (inclusive).")
    parser.add_argument("--lapsed", action="store_true",
                        help="Instead of the summary, print the profile_id "
                             "of every donor who gave in the window and has "
                             "not given since --to. Ids only.")
    args = parser.parse_args(argv)

    if args.end < args.start:
        print(f"--to {args.end} is before --from {args.start}",
              file=sys.stderr)
        return EXIT_BAD_ARGS

    missing = mirror_read.require("donation", "donation_agg")
    if missing:
        print(missing, file=sys.stderr)
        return EXIT_NOT_LOADED

    gifts = gifts_between(args.start, args.end)
    aggregates = aggregates_for(g.get("profile_id") for g in gifts)

    if args.lapsed:
        ids = lapsed_profiles(gifts, aggregates, args.end)
        for profile_id in ids:
            print(profile_id)
        print(f"# {len(ids)} lapsed of {len({str(g.get('profile_id')) for g in gifts if g.get('profile_id')})} "
              f"donors who gave {args.start}..{args.end}; "
              f"{mirror_read.as_of_line('donation', 'donation_agg')}",
              file=sys.stderr)
        return EXIT_OK

    summary = summarise(gifts, aggregates, args.start, args.end)
    print(render(summary, args.start, args.end, fund_names(),
                 mirror_read.as_of_line("donation", "donation_agg", "fund")))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
