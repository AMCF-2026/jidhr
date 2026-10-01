"""Shared test doubles for the DAF/Endowment inquiry workflow.

From 2026-10-01 the workflow searches CSuite before it creates, through
`sync/filter_trust.py`. That check **raises rather than answering** when the
filter cannot be trusted, so a CSuite double with no `_request` makes the
workflow refuse to create — correctly, but it is not what most of these tests
are about.

`NoDuplicates` gives a double the three reads `search_before_create` makes:

  1. unfiltered `profile/list`      -> a non-zero total, or an empty table
     makes every filter look like it works
  2. the same filter with a probe   -> 0, the one answer an ignored filter
     cannot give
  3. the real search                -> 0, no duplicate

`HasDuplicate` answers the third with a match instead.
"""


class _ProfileListDouble:
    """Answers profile/list and profile/display. Nothing else.

    `profile/display` matters from 2026-10-01: the guard no longer trusts a
    stored csuite_profile_id, it reads the profile back. `live_profile_ids`
    says which ids exist; anything else answers a clean "Profile not found",
    and `unreadable_profile_ids` answers a fault instead — which is NOT the
    same thing and must not be treated as missing.
    """

    unfiltered_total = 18797
    duplicate_ids = ()
    # Everything is assumed to exist unless a subclass narrows it.
    live_profile_ids = None          # None = "any id exists"
    unreadable_profile_ids = ()

    def _request(self, endpoint, data=None):
        data = data or {}
        if endpoint == "profile/display":
            return self._display(data.get("profile_id"))
        if endpoint != "profile/list":
            raise AssertionError(
                f"this double only answers profile/list and profile/display, "
                f"not {endpoint!r}")

        if "primary_email" not in data:
            return self._page(self.unfiltered_total, [])

        value = str(data["primary_email"])
        if value.startswith("filter-probe-"):
            return self._page(0, [])        # the known-absent probe

        rows = [{"profile_id": pid} for pid in self.duplicate_ids]
        return self._page(len(rows), rows)

    def _display(self, profile_id):
        key = str(profile_id)
        if key in {str(i) for i in self.unreadable_profile_ids}:
            return {"success": False, "error": "Internal server error",
                    "http_status": 500}
        if self.live_profile_ids is not None and \
                key not in {str(i) for i in self.live_profile_ids}:
            return {"success": False, "error": "Profile not found",
                    "errors": ["Profile not found"], "http_status": 200}
        return {"success": True, "http_status": 200,
                "data": {"profile_id": int(profile_id), "ptype": "indiv"}}

    @staticmethod
    def _page(count, rows):
        return {"success": True, "http_status": 200, "outcome": "ok",
                "data": {"count": count, "results": rows}}


class NoDuplicates(_ProfileListDouble):
    """No CSuite profile carries the submitted email."""


class HasDuplicate(_ProfileListDouble):
    """One CSuite profile already carries it."""

    duplicate_ids = (19999,)


class EmptyTable(_ProfileListDouble):
    """An empty CSuite — filter_trust refuses to trust any filter here."""

    unfiltered_total = 0


def contact(contact_id="70123", csuite_profile_id=None, **props):
    """A HubSpot contacts/search result row, with properties."""
    properties = {"email": "s@example.invalid"}
    if csuite_profile_id is not None:
        properties["csuite_profile_id"] = csuite_profile_id
    properties.update(props)
    return {"results": [{"id": contact_id, "properties": properties}]}


class StaleLink(_ProfileListDouble):
    """HubSpot's stored id does not exist in CSuite, and no email match either."""

    live_profile_ids = ()


class StaleLinkWithMatch(_ProfileListDouble):
    """The stored id is stale, and the email search finds the real profile."""

    live_profile_ids = (21663,)
    duplicate_ids = (21663,)


class UnreadableProfile(_ProfileListDouble):
    """CSuite will not say whether the stored id exists."""

    unreadable_profile_ids = ("99999",)
