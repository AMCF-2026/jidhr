"""V5.97: the redaction layer under write_audit.

Three things could leave a value in an audit row: an unkeyed payload hash,
a non-id under an id-named key in payload_meta.ids, and error text that
echoes what an API rejected. No network, no database.
"""

import hashlib
import json

import pytest
import requests

from clients import audit, redact
from tests.test_write_audit import (Recorder, Response, csuite_client,
                                    hubspot_client)

SECRET_EMAIL = "donor.private@example.org"
KEY = "k" * redact.MIN_HASH_KEY_CHARS


@pytest.fixture
def recorder(monkeypatch):
    rec = Recorder()
    monkeypatch.setattr("clients.database.execute_query", rec)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)
    return rec


def set_key(monkeypatch, value):
    import config
    monkeypatch.setattr(config.Config, redact.HASH_KEY_ENV, value,
                        raising=False)


# ===========================================================================
# Hash
# ===========================================================================

class TestKeyedHash:

    def test_a_guessed_payload_cannot_be_confirmed_without_the_key(
            self, monkeypatch):
        set_key(monkeypatch, KEY)
        payload = {"email": SECRET_EMAIL}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        unkeyed = hashlib.sha256(canonical.encode()).hexdigest()

        assert audit.payload_hash(payload) != unkeyed

    def test_it_still_spots_an_identical_payload(self, monkeypatch):
        set_key(monkeypatch, KEY)
        a = {"properties": {"email": SECRET_EMAIL, "amount": "48250.00"}}
        b = {"properties": {"amount": "48250.00", "email": SECRET_EMAIL}}

        assert audit.payload_hash(a) == audit.payload_hash(b)
        assert audit.payload_hash(a) != audit.payload_hash({"properties": {}})

    def test_another_key_gives_another_hash(self, monkeypatch):
        set_key(monkeypatch, KEY)
        first = audit.payload_hash({"a": 1})
        set_key(monkeypatch, "z" * redact.MIN_HASH_KEY_CHARS)

        assert audit.payload_hash({"a": 1}) != first

    @pytest.mark.parametrize("value", ["", "true", "x" * 31])
    def test_no_usable_key_means_no_hash_never_an_unkeyed_one(
            self, monkeypatch, value):
        set_key(monkeypatch, value)

        assert audit.payload_hash({"email": SECRET_EMAIL}) is None

    def test_a_missing_key_does_not_stop_the_write(self, monkeypatch,
                                                   recorder):
        set_key(monkeypatch, "")
        client, calls = hubspot_client(
            monkeypatch, response=Response(201, {"id": "701"}))

        assert client._post("crm/v3/objects/contacts", {"a": 1}) == {
            "id": "701"}
        assert len(calls) == 1
        assert recorder.one["payload_hash"] is None

    def test_the_missing_key_is_named_once(self, monkeypatch, caplog):
        set_key(monkeypatch, "")
        monkeypatch.setattr(redact, "_warned_no_key", False)
        with caplog.at_level("WARNING", logger="clients.redact"):
            audit.payload_hash({"a": 1})
            audit.payload_hash({"b": 2})

        assert caplog.text.count(redact.HASH_KEY_ENV) == 1


# ===========================================================================
# Ids in payload_meta
# ===========================================================================

class TestIdsInMeta:

    @pytest.mark.parametrize("key, value", [
        ("profile_id", 21696),
        ("contact_id", "701"),
        ("cash_account_id", 1069),
        ("email_id", "399921857254"),
        ("hubspot_form_id", "8dba272f-1986-4bcc-80b4-0b49bc1a4400"),
        # conversionId, and the submittedAt fallback in both forms it has
        # come in: processed tracking reads these back (2026-10-05).
        ("hubspot_submission_id", "6fff2fa0-1c2d-4e5f-8a9b-0c1d2e3f4a5b"),
        ("hubspot_submission_id", "1761234567890"),
        ("hubspot_submission_id", "2026-10-05T14:03:22.123Z"),
        ("hubspot_submission_id", "conv-genc"),
    ])
    def test_a_record_id_is_kept(self, key, value):
        assert audit.payload_meta({key: value})["ids"] == {key: value}

    @pytest.mark.parametrize("key, value", [
        ("id", SECRET_EMAIL),                      # HubSpot idProperty=email
        ("contact_id", "Aisha Rahman"),
        ("ref_id", "+1 (555) 201-3344"),
        ("note_id", "x" * 65),
        ("tax_id", "123456789"),
        ("org_ein_id", "12-3456789"),
        ("vender_id", "4471"),
        ("routing_id", 21000021),
        ("is_id", True),
    ])
    def test_anything_else_is_redacted_and_the_key_kept(self, key, value):
        ids = audit.payload_meta({key: value})["ids"]

        assert ids == {key: redact.ID_REDACTED}
        assert str(value) not in json.dumps(ids)

    def test_nested_ids_get_the_same_rule(self):
        meta = audit.payload_meta(
            {"properties": {"owner_id": "159996166", "guid": SECRET_EMAIL}})

        assert meta["ids"] == {"properties.owner_id": "159996166",
                               "properties.guid": redact.ID_REDACTED}

    def test_processed_tracking_ids_survive_a_real_record_write(
            self, recorder):
        audit.record_write(
            audit.LINK_TARGET_SYSTEM, "HOLD", "submission/held/ambiguous",
            payload={"hubspot_form_id": "8dba272f-1986-4bcc-80b4-0b49bc1a4400",
                     "hubspot_submission_id": "1761234567890"})

        ids = json.loads(recorder.one["payload_meta"])["ids"]
        assert ids == {
            "hubspot_form_id": "8dba272f-1986-4bcc-80b4-0b49bc1a4400",
            "hubspot_submission_id": "1761234567890"}


# ===========================================================================
# Error text
# ===========================================================================

class TestScrubText:

    @pytest.mark.parametrize("raw, gone, kept", [
        (f"Email {SECRET_EMAIL} already exists", SECRET_EMAIL,
         "already exists"),
        ("Property values were not valid: 48250x was not a valid long.",
         "48250x", "was not a valid long"),
        ('"Gold" was not one of the allowed options', "Gold",
         "was not one of the allowed options"),
        ('{"name":"phone","value":"555 201 3344","error":"INVALID"}',
         "201 3344", '"error":"INVALID"'),
        ("call (555) 201-3344 back", "201-3344", "call"),
        ("SSN 123-45-6789 rejected", "123-45-6789", "rejected"),
        ("fedid 12-3456789 invalid", "12-3456789", "invalid"),
        ("card 4111111111111111 declined", "4111111111111111", "declined"),
        ("Authorization: Bearer pat-na1-abc123 refused", "pat-na1-abc123",
         "refused"),
    ])
    def test_the_value_goes_and_the_reason_stays(self, raw, gone, kept):
        out = redact.scrub_text(raw)

        assert gone not in out
        assert kept in out

    @pytest.mark.parametrize("raw", [
        "Contact already exists. Existing ID: 399921857254",
        "profile 21696 not found",
        "HTTP 409",
        "CSuite API credentials not configured",
    ])
    def test_record_ids_and_plain_reasons_are_untouched(self, raw):
        assert redact.scrub_text(raw) == raw

    def test_scrubbed_before_it_is_cut(self):
        error = "x" * (audit.MAX_ERROR_CHARS - 10) + " " + SECRET_EMAIL

        out = redact.scrub_error(error, audit.MAX_ERROR_CHARS)
        assert len(out) <= audit.MAX_ERROR_CHARS
        assert "donor.private" not in out

    def test_no_error_stays_none(self):
        assert redact.scrub_error(None, 500) is None
        assert redact.scrub_error("", 500) is None


class TestErrorTextReachingTheRow:

    def test_a_csuite_rejection_echoing_an_address(self, monkeypatch,
                                                   recorder):
        client, _ = csuite_client(monkeypatch, payload={
            "success": 0,
            "errors": [f"primary_email {SECRET_EMAIL} is already in use"]})
        client._request("profile/edit", {"profile_id": 1})

        row = recorder.one
        assert row["status"] == "failed"
        assert "is already in use" in row["error"]
        assert SECRET_EMAIL not in json.dumps(row, default=str)

    def test_a_hubspot_transport_error_echoing_a_url(self, monkeypatch,
                                                     recorder):
        client, _ = hubspot_client(
            monkeypatch, exc=requests.exceptions.ConnectionError(
                f"Max retries exceeded with url: /contacts?email="
                f"{SECRET_EMAIL}"))
        client._post("crm/v3/objects/contacts", {"a": 1})

        row = recorder.one
        assert row["status"] == "failed"
        assert "Max retries exceeded" in row["error"]
        assert SECRET_EMAIL not in row["error"]

    def test_a_skipped_row_is_scrubbed_too(self, recorder):
        audit.record_write("hubspot", "POST", "crm/v3/objects/contacts",
                           status="skipped",
                           error=f"refused for {SECRET_EMAIL}")

        assert SECRET_EMAIL not in recorder.one["error"]

    def test_a_database_error_quoting_the_row_is_scrubbed(self, monkeypatch):
        def failing(sql, params=None, fetch=True):
            raise RuntimeError("new row violates check constraint\nDETAIL: "
                               f"Failing row contains ({SECRET_EMAIL}).")

        monkeypatch.setattr("clients.database.execute_query", failing)
        monkeypatch.setattr("clients.database.is_configured", lambda: True)
        monkeypatch.setattr(audit, "_HAS_CSUITE_ENV", False)

        with pytest.raises(audit.AuditUnavailable) as raised:
            audit.reserve_write("hubspot", "POST", "crm/v3/objects/contacts")
        assert SECRET_EMAIL not in str(raised.value)
        assert "violates check constraint" in str(raised.value)

    def test_record_write_scrubs_the_database_error(self, monkeypatch):
        # V5.97.1: record_write was pinned to V5.80 and left unscrubbed.
        def failing(sql, params=None, fetch=True):
            raise RuntimeError("new row violates check constraint\nDETAIL: "
                               f"Failing row contains ({SECRET_EMAIL}).")

        monkeypatch.setattr("clients.database.execute_query", failing)
        monkeypatch.setattr("clients.database.is_configured", lambda: True)
        monkeypatch.setattr(audit, "_HAS_CSUITE_ENV", False)

        with pytest.raises(audit.AuditUnavailable) as raised:
            audit.record_write(audit.LINK_TARGET_SYSTEM, "HOLD",
                               "submission/held/ambiguous")
        assert SECRET_EMAIL not in str(raised.value)
        assert "violates check constraint" in str(raised.value)

    def test_a_lost_completion_logs_no_value(self, monkeypatch, caplog):
        def failing(sql, params=None, fetch=True):
            raise RuntimeError(f"Failing row contains ({SECRET_EMAIL}).")

        monkeypatch.setattr("clients.database.execute_query", failing)
        with caplog.at_level("ERROR", logger="clients.audit"):
            assert audit.complete_write(7, status="success") is False

        assert "stays 'attempted'" in caplog.text
        assert SECRET_EMAIL not in caplog.text
