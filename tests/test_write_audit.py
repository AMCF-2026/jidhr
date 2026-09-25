"""Step 2b: every write is audited, and no field value leaks into the row.

No network, no database — clients.database.execute_query is mocked and the
HTTP layer is stubbed.
"""

import json

import pytest

from clients import audit
from clients.csuite import CSuiteClient, is_csuite_write
from clients.hubspot import HubSpotClient, is_hubspot_write
from intents.context import Actor, current_actor, current_intent

# Column order of the INSERT in clients/audit.py.
COLUMNS = [
    "actor_user_id", "actor_label", "intent", "target_system", "http_method",
    "endpoint", "target_id", "payload_hash", "payload_meta", "status",
    "http_status", "error", "duration_ms", "sync_run_id",
]


class Recorder:
    """Stands in for clients.database.execute_query."""

    def __init__(self, fail=False):
        self.rows = []
        self.fail = fail

    def __call__(self, sql, params=None, fetch=True):
        if self.fail:
            raise RuntimeError("audit database unreachable")
        text = " ".join(str(sql).split())
        if text.startswith("UPDATE write_audit"):
            # complete_write stamping the outcome on a reserved row.
            (status, http_status, error, duration_ms, target_id,
             row_id) = params
            row = self.rows[int(row_id) - 1]
            row.update(status=status, http_status=http_status, error=error,
                       duration_ms=duration_ms)
            # COALESCE(target_id, %s): the reserved value wins if set.
            if row.get("target_id") is None and target_id is not None:
                row["target_id"] = target_id
            return 1
        self.rows.append(dict(zip(COLUMNS, params)))
        if "RETURNING id" in text:
            return [{"id": len(self.rows)}]
        return 1

    @property
    def one(self):
        assert len(self.rows) == 1, f"expected 1 audit row, got {len(self.rows)}"
        return self.rows[0]


@pytest.fixture
def recorder(monkeypatch):
    """Capture audit inserts; pretend DATABASE_URL is set."""
    rec = Recorder()
    monkeypatch.setattr("clients.database.execute_query", rec)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)
    monkeypatch.setattr(audit, "_warned_no_database", False)
    return rec


@pytest.fixture(autouse=True)
def clean_context():
    """No actor or intent leaks between tests."""
    a = current_actor.set(None)
    i = current_intent.set(None)
    yield
    current_actor.reset(a)
    current_intent.reset(i)


class Response:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"id": "701"}
        self.text = text or json.dumps(self._payload)

    def json(self):
        return self._payload


def hubspot_client(monkeypatch, response=None, exc=None):
    calls = []

    def sender(url, **kwargs):
        calls.append((url, kwargs))
        if exc is not None:
            raise exc
        return response if response is not None else Response()

    for verb in ("post", "put", "patch", "delete"):
        monkeypatch.setattr(f"clients.hubspot.requests.{verb}", sender)
    monkeypatch.setattr("clients.hubspot.requests.get", sender)

    client = HubSpotClient()
    client.access_token = "test-token"
    return client, calls


def csuite_client(monkeypatch, payload=None, exc=None, status_code=200):
    client = CSuiteClient()
    client.api_key = "k"
    client.api_secret = "s"
    calls = []

    def post(url, **kwargs):
        calls.append(url)
        if exc is not None:
            raise exc
        return Response(status_code,
                        payload if payload is not None
                        else {"success": 1, "data": {"profile_id": 19879}})

    monkeypatch.setattr(client.session, "post", post)
    return client, calls


# ===========================================================================
# HubSpot
# ===========================================================================

class TestHubSpotAudit:
    def test_a_write_is_recorded_with_system_method_and_endpoint(self,
                                                                 monkeypatch,
                                                                 recorder):
        client, _ = hubspot_client(monkeypatch)
        client._post("crm/v3/objects/contacts", {"properties": {"email": "x@y.org"}})

        row = recorder.one
        assert row["target_system"] == "hubspot"
        assert row["http_method"] == "POST"
        assert row["endpoint"] == "crm/v3/objects/contacts"
        assert row["status"] == "success"
        assert row["http_status"] == 200
        assert row["duration_ms"] is not None

    def test_a_patch_records_the_id_from_the_url(self, monkeypatch, recorder):
        client, _ = hubspot_client(monkeypatch)
        client._patch("crm/v3/objects/contacts/701", {"properties": {"x": "1"}})

        row = recorder.one
        assert row["http_method"] == "PATCH"
        assert row["target_id"] == "701"

    def test_a_delete_is_recorded(self, monkeypatch, recorder):
        client, _ = hubspot_client(monkeypatch)
        client._delete("crm/v3/objects/notes/55")

        assert recorder.one["http_method"] == "DELETE"
        assert recorder.one["target_id"] == "55"

    def test_a_get_is_not_recorded(self, monkeypatch, recorder):
        client, _ = hubspot_client(monkeypatch)
        client._get("crm/v3/objects/contacts", {"limit": 5})

        assert recorder.rows == []

    @pytest.mark.parametrize("endpoint", [
        "crm/v3/objects/contacts/search",
        "crm/v3/lists/search",
        "crm/v3/objects/contacts/batch/read",
        "crm/v3/objects/notes/search",
    ])
    def test_read_shaped_posts_are_not_recorded(self, monkeypatch, recorder,
                                                endpoint):
        client, _ = hubspot_client(monkeypatch)
        client._post(endpoint, {"filterGroups": []})

        assert recorder.rows == [], f"{endpoint} is a read"
        assert is_hubspot_write("POST", endpoint) is False

    def test_a_non_2xx_write_is_recorded_as_failed(self, monkeypatch, recorder):
        client, _ = hubspot_client(
            monkeypatch, response=Response(403, {"message": "no scope"}))
        client._post("crm/v3/objects/contacts", {"properties": {}})

        row = recorder.one
        assert row["status"] == "failed"
        assert row["http_status"] == 403

    def test_a_transport_failure_is_recorded_as_failed(self, monkeypatch,
                                                       recorder):
        import requests

        client, _ = hubspot_client(
            monkeypatch, exc=requests.exceptions.ConnectionError("no route"))
        result = client._post("crm/v3/objects/contacts", {"properties": {}})

        row = recorder.one
        assert row["status"] == "failed"
        assert row["http_status"] is None
        assert "no route" in row["error"]
        assert "error" in result

    def test_a_missing_token_records_skipped_and_attempts_nothing(self,
                                                                  monkeypatch,
                                                                  recorder):
        client, calls = hubspot_client(monkeypatch)
        client.access_token = ""
        result = client._post("crm/v3/objects/contacts", {"properties": {}})

        assert calls == [], "nothing may be sent without a token"
        assert recorder.one["status"] == "skipped"
        assert "error" in result

    def test_the_write_still_returns_normally(self, monkeypatch, recorder):
        client, _ = hubspot_client(
            monkeypatch, response=Response(200, {"id": "701"}))
        assert client._post("crm/v3/objects/contacts", {}) == {"id": "701"}


# ===========================================================================
# CSuite
# ===========================================================================

class TestCSuiteAudit:
    def test_a_write_endpoint_is_recorded(self, monkeypatch, recorder):
        client, _ = csuite_client(monkeypatch)
        client._request("profile/create/individual",
                        {"first_name": "A", "email": "a@b.org"})

        row = recorder.one
        assert row["target_system"] == "csuite"
        assert row["http_method"] == "POST"
        assert row["endpoint"] == "profile/create/individual"
        assert row["status"] == "success"

    @pytest.mark.parametrize("endpoint", [
        "profile/list", "profile/display", "funit/list/search",
        "donation/list", "event/display/eventdate", "check/list",
        "funit/feetype", "task/list",
    ])
    def test_read_endpoints_are_not_recorded(self, monkeypatch, recorder,
                                             endpoint):
        client, _ = csuite_client(monkeypatch)
        client._request(endpoint, {"view_limit": 5})

        assert recorder.rows == [], f"{endpoint} is a read"
        assert is_csuite_write(endpoint) is False

    @pytest.mark.parametrize("endpoint", [
        "profile/create/individual", "profile/edit", "funit/create",
        "task/create", "task/edit/complete", "event/create/eventdate",
        "event/edit/eventdate", "grantee/create", "vendor/create",
        "profile/create/org", "profile/create/household",
    ])
    def test_every_write_endpoint_is_classified(self, endpoint):
        assert is_csuite_write(endpoint) is True

    def test_the_payload_id_becomes_the_target_id(self, monkeypatch, recorder):
        client, _ = csuite_client(monkeypatch)
        client._request("profile/edit", {"profile_id": 19879,
                                         "primary_email": "a@b.org"})

        assert recorder.one["target_id"] == "19879"

    def test_success_zero_is_recorded_as_failed(self, monkeypatch, recorder):
        """HTTP 200 with success != 1 is still a failed write."""
        client, _ = csuite_client(
            monkeypatch, payload={"success": 0, "errors": ["duplicate email"]})
        result = client._request("profile/create/individual", {"email": "a@b.org"})

        row = recorder.one
        assert row["status"] == "failed"
        assert row["http_status"] == 200
        assert "duplicate email" in row["error"]
        assert result["success"] is False

    def test_a_transport_failure_is_recorded(self, monkeypatch, recorder):
        import requests

        client, _ = csuite_client(
            monkeypatch, exc=requests.exceptions.Timeout("slow"))
        client._request("profile/edit", {"profile_id": 1})

        assert recorder.one["status"] == "failed"

    def test_missing_credentials_record_skipped(self, monkeypatch, recorder):
        client, calls = csuite_client(monkeypatch)
        client.api_key = ""
        client._request("profile/edit", {"profile_id": 1})

        assert calls == []
        assert recorder.one["status"] == "skipped"

    def test_the_pattern_tuple_is_visible_and_complete(self):
        from clients.csuite import CSUITE_WRITE_PATTERNS

        assert CSUITE_WRITE_PATTERNS == (
            "create", "edit", "delete", "complete", "update")


# ===========================================================================
# No field values in the row
# ===========================================================================

SECRET_EMAIL = "donor.private@example.org"
SECRET_AMOUNT = "48250.00"


class TestNoValuesLeak:
    @pytest.mark.parametrize("payload", [
        {"properties": {"email": SECRET_EMAIL, "lifetime_giving": SECRET_AMOUNT}},
        {"email": SECRET_EMAIL, "donation_amount": SECRET_AMOUNT,
         "profile_id": 19879},
        {"inputs": [{"email": SECRET_EMAIL}, {"email": "b@c.org"}],
         "amount": SECRET_AMOUNT},
    ])
    def test_no_field_value_appears_anywhere_in_the_row(self, monkeypatch,
                                                        recorder, payload):
        client, _ = hubspot_client(monkeypatch)
        client._post("crm/v3/objects/contacts", payload)

        row_text = json.dumps(recorder.one, default=str)
        assert SECRET_EMAIL not in row_text
        assert SECRET_AMOUNT not in row_text

    def test_the_shape_is_still_recorded(self, monkeypatch, recorder):
        client, _ = hubspot_client(monkeypatch)
        client._post("crm/v3/objects/contacts", {
            "properties": {"email": SECRET_EMAIL, "lifetime_giving": SECRET_AMOUNT},
            "associations": [1, 2, 3],
        })

        meta = json.loads(recorder.one["payload_meta"])
        assert meta["keys"] == ["associations", "properties"]
        assert meta["property_keys"] == ["email", "lifetime_giving"]
        assert meta["count"] == {"associations": 3}

    def test_ids_are_kept_because_they_are_the_point(self, monkeypatch,
                                                     recorder):
        client, _ = hubspot_client(monkeypatch)
        client._post("crm/v3/objects/notes",
                     {"properties": {"hs_note_body": "private note text"},
                      "contact_id": 701})

        row = recorder.one
        meta = json.loads(row["payload_meta"])
        assert meta["ids"] == {"contact_id": 701}
        assert "private note text" not in json.dumps(row, default=str)

    def test_the_hash_identifies_an_identical_payload(self):
        a = {"properties": {"email": SECRET_EMAIL, "amount": SECRET_AMOUNT}}
        b = {"properties": {"amount": SECRET_AMOUNT, "email": SECRET_EMAIL}}

        assert audit.payload_hash(a) == audit.payload_hash(b), "key order"
        assert audit.payload_hash(a) != audit.payload_hash({"properties": {}})
        assert SECRET_EMAIL not in audit.payload_hash(a)

    def test_a_note_body_never_reaches_the_row(self, monkeypatch, recorder):
        client, _ = csuite_client(monkeypatch)
        client._request("profile/edit", {
            "profile_id": 1,
            "notes": "Spoke to the family about their bereavement",
        })

        assert "bereavement" not in json.dumps(recorder.one, default=str)


# ===========================================================================
# Actor attribution
# ===========================================================================

class TestActor:
    def test_the_current_actor_is_recorded(self, monkeypatch, recorder):
        token = current_actor.set(
            Actor(user_id=42, email="carl@amuslimcf.org", role="admin"))
        try:
            client, _ = hubspot_client(monkeypatch)
            client._post("crm/v3/objects/contacts", {})
        finally:
            current_actor.reset(token)

        row = recorder.one
        assert row["actor_user_id"] == 42
        assert row["actor_label"] == "carl@amuslimcf.org"

    def test_the_intent_is_recorded(self, monkeypatch, recorder):
        a = current_actor.set(Actor(1, "s@amuslimcf.org", "staff"))
        i = current_intent.set("notes")
        try:
            client, _ = hubspot_client(monkeypatch)
            client._post("crm/v3/objects/notes", {})
        finally:
            current_actor.reset(a)
            current_intent.reset(i)

        assert recorder.one["intent"] == "notes"

    def test_no_actor_falls_back_to_a_system_label(self, monkeypatch, recorder):
        client, _ = hubspot_client(monkeypatch)
        client._post("crm/v3/objects/contacts", {})

        row = recorder.one
        assert row["actor_user_id"] is None
        assert row["actor_label"].startswith("system:")

    def test_a_background_sync_is_labelled_by_its_intent(self, monkeypatch,
                                                         recorder):
        token = current_intent.set("sync_commands")
        try:
            client, _ = hubspot_client(monkeypatch)
            client._post("crm/v3/objects/contacts", {})
        finally:
            current_intent.reset(token)

        assert recorder.one["actor_label"] == "system:sync_commands"

    def test_the_assistant_publishes_the_actor_and_intent(self, monkeypatch,
                                                          recorder):
        """End to end: a handler's write is attributed to the user."""
        from assistant import JidhrAssistant
        from intents.context import Services, new_draft_state

        seen = {}

        def handler(query, ctx):
            seen["actor"] = current_actor.get()
            seen["intent"] = current_intent.get()
            return "done"

        class Module:
            ALLOWED_ROLES = frozenset({"admin", "staff"})

            def can_handle(self, query, **kwargs):
                return True

            handle = staticmethod(handler)

        monkeypatch.setattr("intents.HANDLER_CHAIN", [("notes", Module())])

        a = JidhrAssistant.__new__(JidhrAssistant)
        a.claude = a.hubspot = a.csuite = None
        a.conversation_history = []
        a.services = Services(None, None, None)
        a.draft_state, a.workflow_state = new_draft_state(), {}

        actor = Actor(user_id=7, email="lisa@amuslimcf.org", role="staff")
        a.process_query("log a note", actor)

        assert seen["actor"] is actor
        assert seen["intent"] == "notes"
        # Reset once the handler returns, so the next write is not mislabelled.
        assert current_intent.get() is None


# ===========================================================================
# Auditing never changes the caller's result
# ===========================================================================

class TestAuditFailureRefusesTheWrite:
    """The 2026-09-23 reversal.

    Auditing used to run after the fact and swallow its own failures, so
    when the store was unreachable every write still went out unrecorded
    and nothing upstream could tell. That is not hypothetical: the email
    probe of 2026-09-23 wrote to HubSpot twice with no row between them.
    Now reserve_write claims the row first and the write is refused if it
    cannot.
    """

    def _failing(self, monkeypatch):
        rec = Recorder(fail=True)
        monkeypatch.setattr("clients.database.execute_query", rec)
        monkeypatch.setattr("clients.database.is_configured", lambda: True)
        return rec

    def test_a_hubspot_write_is_refused_and_never_sent(self, monkeypatch):
        """Raised, not returned.

        A refusal that comes back as an ordinary error dict is
        indistinguishable from HubSpot rejecting the request, and the
        caller then reports the wrong cause.
        """
        self._failing(monkeypatch)
        client, calls = hubspot_client(
            monkeypatch, response=Response(200, {"id": "701"}))

        with pytest.raises(audit.AuditUnavailable):
            client._post("crm/v3/objects/contacts", {})

        assert calls == [], "the request went out despite having no audit row"

    def test_a_csuite_write_is_refused_and_never_sent(self, monkeypatch):
        self._failing(monkeypatch)
        client, calls = csuite_client(monkeypatch)

        with pytest.raises(audit.AuditUnavailable):
            client._request("profile/create/individual", {"first_name": "A"})

        assert calls == [], "the request went out despite having no audit row"

    def test_a_read_is_not_refused(self, monkeypatch):
        """Only writes need a row. A refused read would break every report."""
        self._failing(monkeypatch)
        client, calls = hubspot_client(
            monkeypatch, response=Response(200, {"results": []}))

        assert client._post("crm/v3/objects/contacts/search", {}) == \
            {"results": []}
        assert len(calls) == 1

    def test_reserve_write_raises_rather_than_returning_false(self,
                                                              monkeypatch):
        self._failing(monkeypatch)
        with pytest.raises(audit.AuditUnavailable):
            audit.reserve_write("hubspot", "POST", "x", payload={})

    def test_record_write_raises_too(self, monkeypatch):
        self._failing(monkeypatch)
        with pytest.raises(audit.AuditUnavailable):
            audit.record_write("hubspot", "POST", "x", payload={})

    def test_a_missing_database_url_refuses_with_a_clear_message(self,
                                                                 monkeypatch):
        rec = Recorder()
        monkeypatch.setattr("clients.database.execute_query", rec)
        monkeypatch.setattr("clients.database.is_configured", lambda: False)

        with pytest.raises(audit.AuditUnavailable) as caught:
            audit.reserve_write("hubspot", "POST", "x", payload={})

        assert "DATABASE_URL" in str(caught.value)
        assert rec.rows == []

    def test_a_store_that_returns_no_id_refuses(self, monkeypatch):
        """A reserve that stores nothing must not read as success."""
        monkeypatch.setattr("clients.database.execute_query",
                            lambda *a, **k: [])
        monkeypatch.setattr("clients.database.is_configured", lambda: True)

        with pytest.raises(audit.AuditUnavailable):
            audit.reserve_write("hubspot", "POST", "x", payload={})


class TestReserveThenComplete:
    def test_the_row_exists_before_the_request_goes_out(self, monkeypatch,
                                                        recorder):
        """The ordering that makes a killed process visible."""
        seen = {}

        def sender(url, **kwargs):
            # At this moment the audit row must already be there.
            # Copied, not referenced: complete_write mutates these dicts
            # in place afterwards.
            seen["rows"] = [dict(row) for row in recorder.rows]
            return Response(200, {"id": "701"})

        monkeypatch.setattr("clients.hubspot.requests.post", sender)
        client = HubSpotClient()
        client.access_token = "test-token"
        client._post("crm/v3/objects/contacts", {})

        assert len(seen["rows"]) == 1
        assert seen["rows"][0]["status"] == audit.ATTEMPTED

    def test_the_outcome_is_stamped_on_the_same_row(self, monkeypatch,
                                                    recorder):
        client, _ = hubspot_client(
            monkeypatch, response=Response(201, {"id": "701"}))
        client._post("crm/v3/objects/contacts", {})

        assert recorder.one["status"] == "success"
        assert recorder.one["http_status"] == 201

    def test_a_lost_completion_leaves_the_row_attempted(self, monkeypatch,
                                                        recorder, caplog):
        """The write happened, so complete_write must not raise.

        The 'attempted' row is the visible residue — which is the point.
        """
        import logging
        real = recorder.__call__

        def flaky(sql, params=None, fetch=True):
            if " ".join(str(sql).split()).startswith("UPDATE write_audit"):
                raise RuntimeError("gone")
            return real(sql, params, fetch)

        monkeypatch.setattr("clients.database.execute_query", flaky)
        client, calls = hubspot_client(
            monkeypatch, response=Response(201, {"id": "701"}))

        with caplog.at_level(logging.ERROR, logger="clients.audit"):
            result = client._post("crm/v3/objects/contacts", {})

        assert result == {"id": "701"}, "a bookkeeping failure hid a real result"
        assert len(calls) == 1
        assert recorder.one["status"] == audit.ATTEMPTED
        assert "attempted" in caplog.text

    def test_an_unserialisable_payload_does_not_break_the_write(
            self, monkeypatch, recorder):
        class Odd:
            pass

        client, _ = hubspot_client(
            monkeypatch, response=Response(200, {"id": "1"}))
        result = client._post("crm/v3/objects/contacts", {"weird": Odd()})

        assert result == {"id": "1"}
        assert recorder.one["payload_hash"] is not None


# ===========================================================================
# Provenance: the deployed build, and the id of what a create created
# ===========================================================================

class TestDeployedVersion:
    """/health names the build it is running.

    Without it, "which build is live?" can only be inferred — and on
    2026-09-25 a post-deploy check was carried out against a build that
    turned out to predate the change it was verifying.
    """

    def test_the_short_sha_is_served(self, monkeypatch):
        import app
        monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA",
                           "a964994c0ffee1234567890abcdef1234567890")
        assert app.deployed_version() == "a964994"

    def test_an_unset_sha_reads_unknown(self, monkeypatch):
        import app
        monkeypatch.delenv("RAILWAY_GIT_COMMIT_SHA", raising=False)
        assert app.deployed_version() == "unknown"

    def test_a_blank_sha_reads_unknown(self, monkeypatch):
        import app
        monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "   ")
        assert app.deployed_version() == "unknown"

    def test_health_carries_it(self, monkeypatch):
        import app
        monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "abcdef1234567")
        with app.app.test_client() as client:
            body = client.get("/health").get_json()
        assert body["version"] == "abcdef1"


class TestCreatedIdIsRecorded:
    """reserve_write runs BEFORE the request, so a create has no id yet.

    Every marketing email Jidhr made was audited with target_id NULL, and
    working out which row made which email meant matching timestamps by
    hand (2026-09-25).
    """

    def test_a_create_records_the_id_from_the_response(self, monkeypatch,
                                                       recorder):
        client, _ = hubspot_client(
            monkeypatch, response=Response(201, {"id": "400628858587"}))
        client._post("marketing/v3/emails", {"name": "x"})

        assert recorder.one["target_id"] == "400628858587"
        assert recorder.one["status"] == "success"
        assert recorder.one["http_status"] == 201

    def test_an_id_already_known_at_reserve_time_is_not_overwritten(
            self, monkeypatch, recorder):
        """A PATCH has its id in the URL; the response must not replace it."""
        client, _ = hubspot_client(
            monkeypatch, response=Response(200, {"id": "999999"}))
        client._patch("crm/v3/objects/contacts/42", {"properties": {}})

        assert recorder.one["target_id"] == "42"

    def test_a_failed_create_records_no_id(self, monkeypatch, recorder):
        client, _ = hubspot_client(
            monkeypatch, response=Response(400, {"message": "Bad Request"}))
        client._post("marketing/v3/emails", {"name": "x"})

        assert recorder.one["status"] == "failed"
        assert recorder.one["target_id"] is None

    @pytest.mark.parametrize("body, expected", [
        ({"id": "123"}, "123"),
        ({"id": 123}, "123"),
        ({"objectId": "456"}, "456"),
        ({"emailId": "789"}, "789"),
        ({"results": []}, None),
        ({"id": ""}, None),
        ({}, None),
        (None, None),
        ("not a dict", None),
    ])
    def test_the_id_is_read_from_the_shapes_hubspot_returns(self, body,
                                                            expected):
        assert audit.target_id_from_response(body) == expected
