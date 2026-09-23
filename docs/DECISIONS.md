# Decisions

Governance record for Jidhr, as of 2026-09-23. One dated paragraph per
decision that changed what the system stores, what it refuses to store,
or how it is operated. Newest last.

This file is the record; it is not executable. Schema lives in
`schema.sql`, behaviour lives in the module docstrings the decision
touched, and this file says *why* and *when*.

---

## 2026-09-17 — Donation rows enter the mirror

Reverses the rule set on 2026-09-10 that the mirror would never store
individual donation rows. That rule aggregated `donation/list` per profile
and per fund-quarter and discarded the rows, which left ordinary questions
unanswerable: gifts in a date window, median gift, first-time versus
repeat, who gave then and not since. The nightly job was already fetching
all 267 pages and throwing them away, so a third gatherer on the same
shared fetch keeps the rows at zero extra CSuite calls. The whitelist is
the privacy boundary and is exactly nine fields: `donation_id`,
`donation_guid`, `profile_id`, `funit_id`, `donation_date`,
`donation_amount`, `donation_status`, `anonymous_donation`,
`payment_method_id`. No donor name, no `payment_method_name`, no `cf_`
custom fields — the donor is reachable through `profile_id`, and a gift row
that named its donor would be a second copy of the profile table.

## 2026-09-23 — Event registrations enter the mirror

Registrants are stored as jsonb in `csuite_mirror` under record type
`event_registration`, following the donations pattern rather than a typed
table. CSuite gives a registrant row no id of its own, so the key is
synthetic — `{event_date_id}:{profile_id}` — the same approach
`donation_fund_quarter` uses for `{funit_id}:{year}Q{n}`. The whitelist is
exactly nine fields: `event_date_id`, `event_id`, `profile_id`,
`event_profile_email`, `event_profile_name`, `rsvp`, `attended`,
`guest_count`, `pulled_at`. `guests[]` is reduced to a count; guest names
and emails never enter the mirror, because a guest never gave us their
address. The purpose is HubSpot contact enrichment, so that staff-owned
workflows — not Jidhr — handle event email; Jidhr supplies the data and
sends nothing. Note that the key is `event_date_id`, not `event_id`:
`event_id` has three distinct values across 179 event dates and identifies
a *series*, so using it as a key would collapse the catalogue.

## 2026-09-23 — Fields CSuite does not have are absent, not stubbed

`registration_id`, `first_name`, `last_name`, `ticket_type` and
`registered_at` were specified for the registration whitelist and do not
exist in the CSuite API — verified live on 2026-09-23, where the complete
registrant key set is `profile_id`, `event_profile_email`,
`event_profile_name`, `rsvp`, `attended`, `guests`. They are therefore not
stored and not written as null columns. A column that is permanently null
reads as "we have no value for this person" when the truth is "this field
does not exist"; the same reasoning left `schema.sql`'s seven unknown
tables blank rather than guessed. Names arrive combined in
`event_profile_name` and are not split, because splitting a human name is
lossy and this repo has no reason to guess where the boundary falls.

## 2026-09-23 — `attended` is stored and never inferred

Earlier passes reported `attended` as null on every registrant row — 78 of
78 in probe #2, 14 of 14 on a 2026-09-23 spot check. **That was wrong**, and
wrong in a way worth recording: every event sampled in those passes was
either in the future or had no attendance taken. A 20-date backfill on
2026-09-23 read 363 registrants and found `attended = 1` on 44 of them, all
on a single event date (1231), where 44 of 80 registrants were checked in
against 69 RSVPs. So the field is real, is used, and is used rarely — one
event in twenty. Jidhr stores it exactly as CSuite returns it and never
derives it from `rsvp`. On 1231 the two counts differ, which is the
evidence: inferring attendance from an RSVP would overwrite a genuine
check-in record with a guess, and an RSVP is an intention either way.

## 2026-09-23 — The backfill has no date filter

A 2025-01-01 backfill window was specified and is not applied. Of 179 event
dates, only 81 carry an `event_date` at all and only 62 fall on or after
2025-01-01, so a date filter would silently discard 117 event dates — 98 of
them merely undated rather than old. Registrant lists for undated events
are as real as any other. The backfill therefore reads every event date and
is resumable instead: it skips event dates that already have rows, so it
can be run in capped passes across several invocations, and `--force`
re-reads. Nightly runs read only event dates where `archived != 1` — 14 of
179 on 2026-09-23 — because an archived list is final and re-reading it
would spend 165 calls a night to learn nothing.

## 2026-09-23 — `schema.sql` is documentation; typed tables are deferred

`schema.sql` is not executed by anything in this repository; its own header
says so, and applying it means pasting into Railway's Postgres console.
Typed tables for donations and event registrations are therefore deferred
until a query needs them, and the mirror's jsonb rows in `csuite_mirror`
remain the live shape for both. The proposed `csuite_donations` DDL stays
in `schema.sql` commented out as the upgrade path. The trade is real: jsonb
gives up column types and indexes, and buys the mirror's existing
machinery — hash-skip, delete-what-vanished, TTL, the `sync_runs` ledger,
and test/system-fund exclusion — none of which a new typed table would
inherit without being rebuilt.

## 2026-09-23 — HubSpot event enrichment is dry-run by default

`scripts/hubspot_event_enrichment.py` turns mirrored registrations into
five contact properties in a new **AMCF Events** group, so that
staff-owned HubSpot workflows handle event email and Jidhr sends nothing.
Every write in it — the property group, the five properties, the contact
upserts — sits behind `--apply`, and the default run reads HubSpot, reads
the mirror, writes a masked markdown diff to `reports/` and stops. The
reason is that the cheap half of this job is reversible and the expensive
half is not: creating contacts in a portal that bills by marketing
contact is a decision with a price, and the number that decides it
belongs in a report a person reads, not in a script's control flow. An
existing contact has only those five properties set, never `firstname` or
`lastname`, and the two date properties are written as `max(existing,
computed)`, so the script can raise a value and never lower one.

## 2026-09-23 — Registrant names are "Last, First", and the label is the description

Two field-shape corrections found by running the plan against live data
rather than by reading the schema. First, `event_profile_name` is
"Last, First" in 317 of 363 registrant rows; the specified rule — split
on the last space — would have created fifty contacts with reversed names
and a comma welded to the first name ("Aucoin," / "Alix"). A comma now
decides the split and the last-space rule survives as the fallback for
the 46 names without one, most of which are organisations that no split
serves well and which the dry-run report counts instead of guessing at.
Second, the multi-checkbox option label is built from `event_description`
("AMCF Open House"), not `event_name`, because `event_name` belongs to
the parent `event_id` — the series — and takes exactly three values
across all 179 event dates: "Unassigned" (98), "Event - Other" (80),
"Newsletters" (1). Nineteen options all reading "Event - Other" would be
a list no workflow could be built from, which is the same failure as a
search result with no usable id.

## 2026-09-23 — Checkbox ticks are unioned, never replaced

`amcf_event_registrations` and `amcf_event_attended` are written as the
union of what the contact already carries and what the mirror computes.
The script therefore adds ticks and never removes one, and the cost is
real and known: a registration cancelled in CSuite leaves its tick behind
on the contact for good, and no run will ever clear it. That is the
lesser of the two errors. A stale tick is visible — it names an event, a
person can check it against CSuite and untick it — whereas a replace
would silently clear a tick a staff member set by hand, in a property
nobody is watching, with nothing on screen to say it happened. The same
reasoning governs the date properties, which are written as
`max(existing, computed)`. If removals ever need to propagate, the honest
version is a separate reconciliation pass that reports what it would
untick before it unticks it — not a quiet overwrite folded into the
nightly enrichment.

## 2026-09-23 — HubSpot's enumeration limits, as documented rather than assumed

The first version of the enrichment script assumed a 1,000-option ceiling
and a 255-character label cap. Both were invented. HubSpot documents
three ceilings on an enumeration property
(knowledge.hubspot.com/properties/property-field-types-in-hubspot):
each option may total **3,000 characters including its label, value and
description**, and a property may reach **512,000 bytes or 5,000 options,
whichever comes first**. The first is a budget shared across three fields
rather than a cap on the label, so the label's own ceiling is computed
from what the value and description cost. All three are checked before
any write and the plan reports each count beside its limit, because the
ceiling that bites is never the one being watched: an option set well
inside 5,000 options can still break 512,000 bytes if the labels are
long. Option labels are also forced unique — where two event dates would
read identically, both carry their `event_date_id`, not just the second,
since a bare label beside a suffixed one reads as the real one and
neither is.

## 2026-09-23 — The email chrome lives in HubSpot, not in Jidhr

AMCF's email chrome — header, logo, colours, fonts, spacing, footer address
and socials — lives in the HubSpot coded template `/jidhr_shell.html`
("AMCF Standard Email"), where staff can edit it in the UI without a
deploy. Jidhr renders no chrome. `templates/email/amcf_base.html.j2` is
therefore not built, and the request for a sent .eml to derive it from is
withdrawn; the template's source is mirrored into the repository at
`templates/email/hubspot_shell.html` for review only, and the portal
remains the authority. Jidhr supplies exactly five things: `body_html`
(paragraphs, lists, links, bold — no tables, no inline styles),
`date_bar` text, optional `button_label` and `button_url`, and
`preview_text`. The body module stays `@hubspot/email_body`. This
reverses the direction taken earlier the same day, in conversation and
never written down here, that Jidhr would hand HubSpot finished
raw-HTML: that plan existed only because the layout was thought to live
in the module, and once the module carries no layout, raw HTML buys
nothing and costs the staff their ability to edit. The division is worth
stating plainly, because it is the whole point: whoever owns the
appearance owns the template, and that is not Jidhr.

## 2026-09-23 — Writes are audited after the fact, and the spool lives in var/audit_spool/

`clients/audit.record_write` is a post-flight. Its docstring says it never
raises and its callers ignore the return, so when the audit store is
unreachable every HubSpot and CSuite write still proceeds — unrecorded,
with nothing upstream able to tell. That is not hypothetical: the
2026-09-23 email probe wrote to HubSpot twice while `write_audit` took
neither row. `DATABASE_URL` points at `postgres.railway.internal`, which
is Railway's private hostname and resolves only inside Railway, so any
run from a developer machine audits nothing at all, silently. Until
`record_write` is redesigned to fail closed — reserve the row, perform
the write, complete the row, so a crash in between leaves a visible
`attempted` row rather than silence — rows that cannot be stored are
appended as JSON lines to `var/audit_spool/api_writes_pending.jsonl`,
built with the real `payload_hash` and `payload_meta` helpers so they
replay into `write_audit` unchanged. That path is deliberately NOT in
`.gitignore`, unlike `reports/`, where the spool briefly sat: an audit
record that a fresh clone silently loses is not an audit record. The
fail-closed redesign is flagged and not implemented.

## 2026-09-23 — templatePath goes inside `content`; three templates, one slot contract

Two probes settled how a coded template attaches to a marketing email,
both since archived. Probe **399921857254** sent `templatePath` at the top
level, which is where HubSpot's own create-email reference puts it in its
only example; HubSpot answered `201` and stored
`content.templatePath = "@hubspot/email/dnd/plain_text.html"` with
`emailTemplateMode = DRAG_AND_DROP`. The path was not rejected, it was
silently discarded — which is why nothing could retry or fall back, and
why every API-built email for the previous fortnight arrived in default
portal styling. Probe **399908795086** sent the identical payload with
`templatePath` moved inside `content` and HubSpot stored
`/jidhr_shell.html` with `emailTemplateMode = DESIGN_MANAGER`. One nesting
level was the entire bug. Carl's UI check on probe 2: (a) the AMCF chrome
rendered with the marker body — yes; (b) the Email Body is a normal module
in the content editor, so staff edit it as they would any email — yes;
(c) preview-as-contact was not reported. `/jidhr_shell.html`,
`/Giving_Circle.html` and `/SoCal.html` were read from the portal on the
same day and mirrored byte-for-byte into `templates/email/`; all three
expose the same six slots with no drift, which is what lets one payload
builder serve all three. `site_settings.company_country` is present in all
three as published. Because a `201` is not evidence — probe 1 returned one
— `create_draft_email` reads the email back and archives it before raising
if the stored path or mode is not what was asked for.

## 2026-09-23 — The chat flow saves for real, and HubL tokens pass the sanitizer

`_save_email_draft` is called with `apply=True`, so asking Jidhr to save an
email creates the HubSpot draft rather than preparing one. It never
downgrades to a dry run without saying so: if the audit store is
unreachable the reply is "Draft not saved: audit store unreachable" with
the full payload summary and the draft left open, because a person who
asked for a save and got silence will assume it worked. Every save reply
— succeeded, refused or dry — lists the template key, the date bar, the
preview text and the button as label plus URL or the words **NO BUTTON**,
since an email that quietly went out without its call to action is the
failure that list exists to prevent. Separately: HubL personalization
tokens such as `{{ personalization_token('contact.firstname', 'Friend') }}`
pass `sanitize_body_html` untouched, because the allowlist works on tags
and a token is text. That is acceptable while every body is
Jidhr-generated and every template is staff-owned. It stops being
acceptable the moment staff can paste HTML into a draft: a pasted token
is then an expression someone else wrote, rendering inside AMCF's
templates against AMCF's contact data. Revisit before any paste path
ships.
