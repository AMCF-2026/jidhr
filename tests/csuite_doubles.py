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
    """Answers profile/list the way filter_trust needs. Nothing else."""

    unfiltered_total = 18797
    duplicate_ids = ()

    def _request(self, endpoint, data=None):
        data = data or {}
        if endpoint != "profile/list":
            raise AssertionError(
                f"this double only answers profile/list, not {endpoint!r}")

        if "primary_email" not in data:
            return self._page(self.unfiltered_total, [])

        value = str(data["primary_email"])
        if value.startswith("filter-probe-"):
            return self._page(0, [])        # the known-absent probe

        rows = [{"profile_id": pid} for pid in self.duplicate_ids]
        return self._page(len(rows), rows)

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
