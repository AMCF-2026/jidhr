"""
Every HubSpot form Jidhr knows about (V5.94)
============================================
One row per form in portal 243832852, from the Step 0 discovery of
2026-10-27 (28 forms: 27 HubSpot, 1 collected). Versioned here, not in a
database table, so a change is reviewed, tested and deployed like code.
What changes without a deploy lives in Railway, per form:

    JIDHR_FORM_<KEY>_ON      "true" turns the form's actions on (exact word)
    JIDHR_FORM_<KEY>_MODE    preview (default) | live — anything else refuses
    JIDHR_FORM_<KEY>_SINCE   YYYY-MM-DD; submissions before it are not acted on

V5.94 runs in OBSERVE mode: forms_sync reads every form here and reports
counts. Nothing acts on these switches yet; they are read and validated so
a wrong value is caught before anything does.

Categories
----------
  A  CSuite record + follow-up task (DAF, endowment: their own code)
  B  CSuite note on an existing record
  C  HubSpot only — nothing goes to CSuite
  D  alert / digest only — a person acts, Jidhr writes nothing
  E  ignore — one-off, test, never launched, or out of scope

Owners and recipients are different fields
------------------------------------------
`owners` are the people a form belongs to: the CSuite task assignee and,
from V5.96, the digest recipients. Confirmed by Carl, 2026-10-27, and the
ONLY names allowed here (OWNERS). A form with no owner gets no task: a
create refuses until one is set.

`report_to_env` names the Railway variable holding who gets the nightly
sync REPORT (e.g. Shazeen for DAF). A recipient, not an owner.

`sensitive` lists the fields whose values must never reach a CSuite note,
task, log line, audit row, preview, email or chat reply. "out" means the
value is never shown at all; "last4" means at most its last four
characters. It is enforced from V5.95; here it is the record of the rule.
"""

import re
from datetime import date

CATEGORIES = ("A", "B", "C", "D", "E")

# The only names a form may carry, with their CSuite employee ids — every one
# read back by task/display on a production task, 2026-10-27.
OWNERS = {
    "munira": {"name": "Ahmed, Munira", "employee_id": 1012},
    "ola": {"name": "Mohamed, Ola", "employee_id": 1002},
    "kods": {"name": "Zouita, Kods", "employee_id": 1007},
    "nora": {"name": "Moorefield, Nora", "employee_id": 1004},
}

OUT, LAST4 = "out", "last4"

DEFAULT_REPORT_TO_ENV = "JIDHR_FORMS_REPORT_TO"


def _form(key, form_id, name, category, why, owners=(), sensitive=None,
          report_to_env=DEFAULT_REPORT_TO_ENV, handler=None, note_type=None):
    return {"key": key, "form_id": form_id, "name": name,
            "category": category, "why": why, "owners": tuple(owners),
            "sensitive": dict(sensitive or {}),
            "report_to_env": report_to_env, "handler": handler,
            "note_type": note_type}


FORMS = (
    # --- A: their own code; registered so the report counts them ---------------
    _form("daf", "8dba272f-1986-4bcc-80b4-0b49bc1a4400", "DAF Inquiry Form",
          "A", "own track: sync/daf_sync.py (V5.90)", owners=("munira",),
          report_to_env="JIDHR_DAF_REPORT_TO", handler="daf_sync"),
    _form("endowment", "0d10e268-75e2-448e-ab93-84e7b2479ef5",
          "Endowment Inquiry Form", "A",
          "own track: intents/endowment_inquiry.py (V5.80-V5.89)",
          # No `sensitive` list: the EIN and goal go INTO the CSuite task by
          # decision (V5.86), and its own code keeps them out of logs and
          # write_audit. The rule here would contradict that.
          owners=("ola",), handler="endowment_inquiry"),

    # --- money and identity: D, a person acts -----------------------------------
    _form("asset_donation", "f45f230b-e7ed-4bdb-90fe-ae00a1af1ec2",
          "AMCF Asset Donation Transfer Form", "D",
          "stock-transfer heads-up; B (note 1071 Stock Contribution) after "
          "DAF live", owners=(), note_type=1071,
          sensitive={"what_is_your_daf_fund": LAST4,
                     "number_of_shares": OUT,
                     "what_stocksmutual_funds_are_you_donating": OUT,
                     "would_you_like_us_to_set_a_limit_order_or_strike_price"
                     "_if_yes_at_what_amount": OUT}),
    _form("ach_request", "dc28c638-b179-43e7-a291-a4008632e62e",
          "ACH Request Form", "D",
          "bank details: organisation and date only, values never read",
          owners=("kods",),
          sensitive={"routing_number": OUT, "account_number": OUT,
                     "confirm_account_number": OUT, "ein": OUT,
                     "bank_name": OUT, "vender_id": LAST4}),
    _form("endowment_distribution", "fcb0cd2f-0c98-438d-9ed7-2b3bf2b03f25",
          "Endowment Distribution Request", "D", "moves money",
          owners=("ola",),
          sensitive={"daf_endowment_account_number": LAST4,
                     "endowment_distribution_amount2": OUT,
                     "endowment_distribution_reason": OUT,
                     "address": OUT, "city": OUT, "state": OUT, "zip": OUT}),
    _form("investment", "39cde168-0903-4237-a267-777607128c98",
          "Investment Form", "D", "grouped by investment cycle",
          owners=("kods", "nora"),
          sensitive={"daf_endowment_account_number": LAST4,
                     "how_much_are_you_putting_in_to_this_investment_cycle_":
                         OUT}),
    _form("nonprofit_directory", "d7f350f7-6c6a-4d9f-bfb5-528695685c54",
          "Nonprofit Directory Application", "D",
          "applicants are not grantees; HubSpot makes the ticket and list",
          owners=("munira",),
          sensitive={"organization_ein": OUT, "fiscal_sponsor_ein": OUT}),

    # --- C: HubSpot only ----------------------------------------------------------
    _form("general_signup", "01f58e99-206d-4a73-8fed-eed72cd0defc",
          "General Email Signup Form", "C", "HubSpot welcome flow"),
    _form("giving_circle_email", "171f2e10-32ce-4a7b-a9c1-14f3cc7d63b5",
          "Giving Circle Email", "C", "list 31 and welcome flow"),
    _form("footer_signup", "9f5d51b7-4dbd-4089-a29e-52aeffa788af",
          "Website – Footer Signup", "C",
          "on every page, 0 submissions since Sep 24: check it works"),
    _form("afp_affinity", "effc09d5-a069-468c-8dc8-70cddcf1d3e5",
          "AFP Muslim Affinity Group", "C", "no HubSpot flow; counted here"),
    _form("landscape_survey", "520fd383-e289-4a3a-8bc9-f50dad2f3f11",
          "2026 AMCF Endowment Landscape Survey", "C",
          "answers stay in HubSpot"),
    _form("share_your_story", "afe9abc8-73f1-4f21-aef1-160855c9b2db",
          "Share Your Story - Testimonials", "C",
          "marketing content; five people are notified"),

    # --- E: ignore ------------------------------------------------------------------
    _form("nmen", "85d83a60-cac1-45d7-90d4-a1e45490c1b5",
          "National Muslim Endowment Network", "E", "no page"),
    _form("wgc_calm_at_work", "515c3704-37de-472c-85a0-3fd46956203b",
          "AMCF Women's Giving Circle Event - Calm at Work", "E",
          "past event (Jan 2026)"),
    _form("wgc_winter_nominee", "c0bae593-d40e-4135-91ae-f0b5850ad0aa",
          "AMCF Women's Giving Circle - Winter Nominee Meet-up", "E",
          "past event (Jan 2026)"),
    _form("summit_feedback_2026", "20f1e663-9f34-4c4a-a9f1-ae7d47f6e195",
          "2026 Nonprofit Summit Feedback Form", "E", "past event (Sep 2026)"),
    _form("summit_session_feedback", "153d99cf-5b1a-463e-a32d-08fc7b0479dc",
          "AMCF 3rd Annual Nonprofit Summit — Session Feedback", "E",
          "past event (Apr 2026)"),
    _form("market_update_q3_2025", "e16893c4-4e61-4bf8-8c21-3ab546d5e6d4",
          "Q3 2025 Financial Market Update Meeting", "E",
          "past event; page is a draft"),
    _form("testimonial_campaign", "93539f15-2d20-42e3-b319-23c835dbfdb2",
          "Testimonial Collection Campaign Form", "E",
          "replaced by Share Your Story"),
    _form("gc_testimonial_campaign", "3db4a14d-7d40-4b5a-a9fb-bfef18d05998",
          "GC Testimonial Collection Campaign Form", "E", "never used"),
    _form("book_a_meeting", "0a30f089-1805-4e49-9a6e-2c928dd5638a",
          "New book a meeting form", "E",
          "may back a meeting link: ignore, don't archive"),
    _form("blank_form", "5119f474-7cc7-48fb-b2a5-a2f89b8ee06d",
          "New blank form (Oct 29 2025)", "E", "test form"),
    _form("commentform", "46ae5041-6c2f-42be-8a39-f84476f51d92",
          "#commentform .comment-form (collected)", "E",
          "WordPress blog comments; stop capture in HubSpot"),
    _form("agl_contractor_clone", "11f81a49-ceb7-4f6c-a852-725be3bc7e70",
          "AGL Contractor Form (clone)", "E", "out of scope (Carl)"),
    _form("agl_incomplete", "1e4ee5df-a56f-42d0-9eef-b711c765787b",
          "AGL Incomplete Application Follow-up", "E", "out of scope (Carl)"),
    _form("ethical_investing", "96366aa5-1cc3-4566-be25-da412066dcae",
          "Ethical Investing Form", "E", "out of scope (Carl)"),
    _form("zakat_calculator", "b8991104-4fa8-4ca3-8bc7-d1872c4c55ca",
          "Zakat Calculator", "E", "out of scope (Carl)"),
)

BY_ID = {f["form_id"]: f for f in FORMS}
BY_KEY = {f["key"]: f for f in FORMS}


class BadSetting(ValueError):
    """A per-form Railway variable is set to something it cannot mean."""


class NoOwner(LookupError):
    """A task was asked for on a form with no single confirmed owner."""


def env_name(key, suffix) -> str:
    return f"JIDHR_FORM_{key.upper()}_{suffix}"


def switches(form, environ) -> dict:
    """The form's three Railway switches, validated. Raises BadSetting."""
    key = form["key"]
    on_raw = str(environ.get(env_name(key, "ON"), "") or "").strip()
    mode_raw = str(environ.get(env_name(key, "MODE"), "") or "").strip()
    since_raw = str(environ.get(env_name(key, "SINCE"), "") or "").strip()

    mode = mode_raw.lower() or "preview"
    if mode not in ("preview", "live"):
        raise BadSetting(f"{env_name(key, 'MODE')} is {mode_raw!r}; it must "
                         "be preview or live.")
    since = None
    if since_raw:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", since_raw):
            raise BadSetting(f"{env_name(key, 'SINCE')} is {since_raw!r}; it "
                             "must be a date, YYYY-MM-DD.")
        try:
            since = date.fromisoformat(since_raw)
        except ValueError:
            raise BadSetting(f"{env_name(key, 'SINCE')} is {since_raw!r}, "
                             "which is not a real date.")
    return {"on": on_raw == "true", "mode": mode, "since": since}


def task_assignee(form) -> int:
    """The CSuite employee id a task on this form goes to. Exactly one
    confirmed owner, or NoOwner — never a guess, never a default."""
    owners = form.get("owners") or ()
    if len(owners) != 1:
        raise NoOwner(f"{form['name']} has "
                      f"{'no owner' if not owners else 'more than one owner'}"
                      " set, so no task can be created for it.")
    return OWNERS[owners[0]]["employee_id"]


def owner_names(form) -> list:
    return [OWNERS[o]["name"] for o in form.get("owners") or ()]
