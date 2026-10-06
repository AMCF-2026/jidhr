"""Does a CSuite profile id actually resolve to a profile?

Three answers, not two, because two of them lead to opposite decisions:

* **PROFILE_EXISTS** — read back, with a profile_id in it.
* **PROFILE_MISSING** — a clean "not found". The id is stale.
* **PROFILE_UNREADABLE** — anything else. **Not** the same as missing: a
  transport fault or a 500 says nothing about whether the profile is there,
  and treating it as missing invites acting on a record that exists.

Why it lives here rather than in intents/daf_workflow.py, where it was written
on 2026-10-02: the donation sync needs the same question answered before it
overwrites a contact's `csuite_profile_id`, and two implementations of "is this
id real?" would drift. 68 production contacts carry ids that do not resolve,
and the two writers of that field have to agree about what to do with them.

One READ per call, `profile/display`. No write budget is touched.
"""

import logging

logger = logging.getLogger(__name__)

# A stored id is not evidence that a profile exists.
PROFILE_EXISTS = "exists"
PROFILE_MISSING = "missing"
PROFILE_UNREADABLE = "unreadable"


def csuite_profile_state(csuite, profile_id):
    """(state, record) for a profile id. One READ."""
    try:
        response = csuite._request("profile/display", {"profile_id": profile_id})
    except Exception as e:
        logger.error("could not read CSuite profile %s: %s", profile_id, e)
        return PROFILE_UNREADABLE, None

    if not isinstance(response, dict):
        return PROFILE_UNREADABLE, None

    record = response.get("data")
    if isinstance(record, list) and record:
        record = record[0]
    if isinstance(record, dict) and record.get("profile_id"):
        return PROFILE_EXISTS, record

    # CSuite answers a missing profile with success=0 and "Profile not found".
    # Anything else — a 5xx, a network fault, an unparseable body — is not a
    # statement that the profile is absent.
    error = str(response.get("error") or "")
    errors = " ".join(str(e) for e in (response.get("errors") or []))
    if "not found" in (error + " " + errors).lower():
        return PROFILE_MISSING, None
    return PROFILE_UNREADABLE, None


class ProfileStateCache:
    """csuite_profile_state memoised for the length of one run.

    A sync over thousands of contacts hits the same ids repeatedly — a
    household's donors, a repeated stale id — and CSuite has no batch display.
    Scoped to a run on purpose: cached across runs it would be answering about
    a profile as it was.
    """

    def __init__(self, csuite):
        self.csuite = csuite
        self._seen = {}
        self.reads = 0

    def state(self, profile_id):
        key = str(profile_id)
        if key not in self._seen:
            self.reads += 1
            self._seen[key] = csuite_profile_state(self.csuite, profile_id)
        return self._seen[key]
