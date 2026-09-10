"""Step 2c: association writes use PUT, and a failure never leaves an orphan.

From write_audit rows 1-2 (2026-09-10 16:33): a call was created (201), the
association POST returned 405, nobody checked, and the user saw ✅ for an
engagement attached to nobody.

No network — the requests module is stubbed and every call is recorded.
"""

import json

import pytest

from clients.hubspot import HubSpotClient, flatten_error_body
from intents.context import Actor, RequestContext, Services, new_draft_state

ACTOR = Actor(user_id=1, email="staff@amuslimcf.org", role="staff")


class Call:
    def __init__(self, method, url, kwargs):
        self.method = method
        self.url = url
        self.kwargs = kwargs

    @property
    def path(self):
        return self.url.split("api.hubapi.com/", 1)[-1]

    def __repr__(self):
        return f"{self.method} {self.path}"


class Response:
    def __init__(self, status_code=200, payload=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else (
            json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def make_client(monkeypatch, responder):
    """A HubSpotClient whose HTTP layer is `responder(Call) -> Response`."""
    calls = []

    def make_sender(method):
        def sender(url, **kwargs):
            call = Call(method, url, kwargs)
            calls.append(call)
            return responder(call)
        return sender

    for verb in ("post", "put", "patch", "delete", "get"):
        monkeypatch.setattr(f"clients.hubspot.requests.{verb}",
                            make_sender(verb.upper()))

    # Auditing is exercised elsewhere; keep it out of the way here.
    monkeypatch.setattr("clients.database.is_configured", lambda: False)

    client = HubSpotClient()
    client.access_token = "test-token"
    return client, calls


def happy(created_id="701"):
    """Creation succeeds, association succeeds."""
    def responder(call):
        if call.method == "POST" and "/associations/" not in call.path:
            return Response(201, {"id": created_id})
        if call.method == "PUT" and "/associations/" in call.path:
            return Response(204)
        return Response(200, {})
    return responder


def association_405(created_id="701", delete_status=204):
    """The production failure: creation 201, association 405."""
    def responder(call):
        if call.method == "POST" and "/associations/" not in call.path:
            return Response(201, {"id": created_id})
        if "/associations/" in call.path:
            return Response(405, text=(
                "<html>\n<head>\n<title>Error 405</title>\n</head>\n"
                "<body>\n<h2>HTTP ERROR 405</h2>\n<p>Method Not Allowed</p>\n"
                "</body>\n</html>"))
        if call.method == "DELETE":
            return Response(delete_status)
        return Response(200, {})
    return responder


# ===========================================================================
# 1. Associations use PUT on the /default/ path
# ===========================================================================

class TestAssociationMethod:
    CREATORS = [
        ("calls", lambda c: c.create_call_note(body="b", contact_id="42")),
        ("notes", lambda c: c.create_note(body="b", contact_id="42")),
        ("meetings", lambda c: c.create_meeting_note(
            title="t", body="b", contact_id="42")),
    ]

    @pytest.mark.parametrize("object_type,create", CREATORS)
    def test_the_association_is_a_put(self, monkeypatch, object_type, create):
        client, calls = make_client(monkeypatch, happy())
        create(client)

        assoc = [c for c in calls if "/associations/" in c.path]
        assert len(assoc) == 1
        assert assoc[0].method == "PUT", "POST returned 405 in production"

    @pytest.mark.parametrize("object_type,create", CREATORS)
    def test_the_path_uses_the_default_form(self, monkeypatch, object_type,
                                            create):
        client, calls = make_client(monkeypatch, happy())
        create(client)

        assoc = [c for c in calls if "/associations/" in c.path][0]
        assert assoc.path == (
            f"crm/v4/objects/{object_type}/701/associations/default/contacts/42")

    @pytest.mark.parametrize("object_type,create", CREATORS)
    def test_the_default_endpoint_is_sent_without_a_body(self, monkeypatch,
                                                         object_type, create):
        """The association type is implied by the object pair."""
        client, calls = make_client(monkeypatch, happy())
        create(client)

        assoc = [c for c in calls if "/associations/" in c.path][0]
        assert assoc.kwargs.get("json") is None

    def test_no_association_write_still_uses_post(self, monkeypatch):
        client, calls = make_client(monkeypatch, happy())
        client.create_call_note(body="b", contact_id="42")
        client.create_note(body="b", contact_id="42")

        posts_to_associations = [
            c for c in calls
            if c.method == "POST" and "/associations/" in c.path]
        assert posts_to_associations == []

    def test_the_engagement_itself_is_still_created_with_post(self,
                                                              monkeypatch):
        client, calls = make_client(monkeypatch, happy())
        client.create_call_note(body="b", contact_id="42")

        creation = [c for c in calls if c.path == "crm/v3/objects/calls"]
        assert len(creation) == 1 and creation[0].method == "POST"


# ===========================================================================
# 2. A failed association leaves no orphan
# ===========================================================================

class TestOrphanRollback:
    CREATORS = [
        ("calls", lambda c: c.create_call_note(body="b", contact_id="42")),
        ("notes", lambda c: c.create_note(body="b", contact_id="42")),
        ("meetings", lambda c: c.create_meeting_note(
            title="t", body="b", contact_id="42")),
    ]

    @pytest.mark.parametrize("object_type,create", CREATORS)
    def test_the_orphan_is_deleted(self, monkeypatch, object_type, create):
        client, calls = make_client(monkeypatch, association_405())
        create(client)

        deletes = [c for c in calls if c.method == "DELETE"]
        assert len(deletes) == 1
        assert deletes[0].path == f"crm/v3/objects/{object_type}/701"

    @pytest.mark.parametrize("object_type,create", CREATORS)
    def test_an_error_dict_is_returned_not_the_created_object(self,
                                                              monkeypatch,
                                                              object_type,
                                                              create):
        client, _ = make_client(monkeypatch, association_405())
        result = create(client)

        assert result.get("association_failed") is True
        assert result.get("http_status") == 405
        assert result.get("orphan_deleted") is True
        assert "id" not in result, "must not look like a created object"

    def test_a_failed_rollback_is_reported(self, monkeypatch):
        client, _ = make_client(
            monkeypatch, association_405(delete_status=500))
        result = client.create_call_note(body="b", contact_id="42")

        assert result["association_failed"] is True
        assert result["orphan_deleted"] is False
        assert result["orphan_id"] == "701"

    def test_a_json_4xx_association_error_is_still_caught(self, monkeypatch):
        """A 4xx with a JSON body parses into an ordinary-looking dict."""
        def responder(call):
            if call.method == "POST" and "/associations/" not in call.path:
                return Response(201, {"id": "701"})
            if "/associations/" in call.path:
                return Response(400, {"status": "error",
                                      "message": "invalid association"})
            return Response(204)

        client, calls = make_client(monkeypatch, responder)
        result = client.create_call_note(body="b", contact_id="42")

        assert result["association_failed"] is True
        assert any(c.method == "DELETE" for c in calls)

    def test_no_contact_id_means_no_association_and_no_rollback(self,
                                                                monkeypatch):
        client, calls = make_client(monkeypatch, happy())
        result = client.create_note(body="b")

        assert result == {"id": "701"}
        assert [c for c in calls if "/associations/" in c.path] == []
        assert [c for c in calls if c.method == "DELETE"] == []

    def test_a_failed_creation_is_not_followed_by_an_association(self,
                                                                 monkeypatch):
        def responder(call):
            return Response(403, {"status": "error", "message": "no scope"})

        client, calls = make_client(monkeypatch, responder)
        result = client.create_call_note(body="b", contact_id="42")

        assert [c for c in calls if "/associations/" in c.path] == []
        assert [c for c in calls if c.method == "DELETE"] == []
        assert "id" not in result

    def test_the_success_path_is_unchanged(self, monkeypatch):
        client, calls = make_client(monkeypatch, happy())
        result = client.create_call_note(body="b", contact_id="42")

        assert result == {"id": "701"}
        assert [c.method for c in calls] == ["POST", "PUT"]


# ===========================================================================
# 3. The user is told, and never sees a ✅
# ===========================================================================

class NotesHubSpot:
    """A HubSpot double for intents.notes, wrapping a real client."""

    def __init__(self, client, contacts):
        self._client = client
        self._contacts = contacts

    def search_contacts(self, query, limit=10):
        return {"results": self._contacts}

    def create_call_note(self, body, contact_id):
        return self._client.create_call_note(body=body, contact_id=contact_id)

    def create_meeting_note(self, title, body, contact_id):
        return self._client.create_meeting_note(
            title=title, body=body, contact_id=contact_id)

    def create_note(self, body, contact_id):
        return self._client.create_note(body=body, contact_id=contact_id)

    @staticmethod
    def get_contact_url(contact_id):
        return f"https://hubspot.example/contact/{contact_id}"


CONTACT = {"id": "42", "properties": {
    "firstname": "Ahmed", "lastname": "Khan", "email": "ahmed@example.org"}}


def notes_ctx(hubspot):
    return RequestContext(
        actor=ACTOR,
        services=Services(hubspot=hubspot, csuite=None, claude=None),
        draft_state=new_draft_state(), workflow_state={},
        conversation_history=[])


class TestUserMessage:
    @pytest.mark.parametrize("query,noun", [
        ("log call with Ahmed - discussed the timeline", "call"),
        ("log meeting with Ahmed - discussed the timeline", "meeting"),
        ("add a note about Ahmed - discussed the timeline", "note"),
    ])
    def test_the_failure_is_stated_plainly(self, monkeypatch, query, noun):
        from intents.notes import handle

        client, _ = make_client(monkeypatch, association_405())
        out = handle(query, notes_ctx(NotesHubSpot(client, [CONTACT])))

        assert "✅" not in out
        assert f"Couldn't attach the {noun}" in out
        assert "Ahmed Khan" in out
        assert "HubSpot 405" in out
        assert "Nothing was saved." in out

    def test_no_hubspot_link_is_offered(self, monkeypatch):
        from intents.notes import handle

        client, _ = make_client(monkeypatch, association_405())
        out = handle("log call with Ahmed - discussed timeline",
                     notes_ctx(NotesHubSpot(client, [CONTACT])))

        assert "hubspot.example" not in out
        assert "View in HubSpot" not in out

    def test_a_failed_rollback_warns_about_the_stray_record(self, monkeypatch):
        from intents.notes import handle

        client, _ = make_client(
            monkeypatch, association_405(delete_status=500))
        out = handle("log call with Ahmed - discussed timeline",
                     notes_ctx(NotesHubSpot(client, [CONTACT])))

        assert "stray call" in out
        assert "701" in out
        assert "✅" not in out

    def test_the_success_path_still_confirms(self, monkeypatch):
        from intents.notes import handle

        client, _ = make_client(monkeypatch, happy())
        out = handle("log call with Ahmed - discussed timeline",
                     notes_ctx(NotesHubSpot(client, [CONTACT])))

        assert "✅" in out
        assert "Ahmed Khan" in out
        assert "View in HubSpot" in out


# ===========================================================================
# 4. Error bodies are one log line
# ===========================================================================

class TestErrorBodyFlattening:
    def test_an_html_error_page_becomes_one_line(self):
        html = ("<html>\n<head>\n<title>Error 405</title>\n</head>\n"
                "<body>\n<h2>HTTP ERROR 405</h2>\n</body>\n</html>")
        flat = flatten_error_body(html)

        assert "\n" not in flat
        assert " | " in flat
        assert "Error 405" in flat

    def test_it_is_capped_at_three_hundred_characters(self):
        flat = flatten_error_body("x" * 500)

        assert len(flat) <= 301  # 300 plus the ellipsis
        assert flat.endswith("…")

    def test_a_short_body_is_untouched(self):
        assert flatten_error_body("Method Not Allowed") == "Method Not Allowed"

    def test_empty_and_none_are_safe(self):
        assert flatten_error_body("") == "(empty)"
        assert flatten_error_body(None) == "(empty)"
        assert flatten_error_body("\n\n  \n") == "(empty)"

    def test_the_error_returned_to_the_caller_is_also_flat(self, monkeypatch):
        def responder(call):
            return Response(500, text="line one\nline two\nline three")

        client, _ = make_client(monkeypatch, responder)
        result = client._post("crm/v3/objects/notes", {"properties": {}})

        assert "\n" not in result["error"]
        assert "line one | line two" in result["error"]
