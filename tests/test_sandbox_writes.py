"""CSuite writes: sandbox only, capped, classified.

No network. The guard tests use a client that raises on any request, so
a write that slipped past a refusal fails the test rather than passing
quietly.
"""

import json

import pytest

from clients import csuite
from clients.csuite import (CSuiteClient, classify_status, host_of,
                            OUTCOME_AUTH_REJECTED, OUTCOME_BAD_RESPONSE,
                            OUTCOME_INVALID_REQUEST, OUTCOME_NETWORK,
                            OUTCOME_OK, OUTCOME_RATE_LIMITED,
                            OUTCOME_REJECTED, OUTCOME_SERVER_ERROR)
from sync.sandbox_writes import (NotSandbox, SANDBOX_HOSTNAME, WriteBudget,
                                 WriteBudgetExceeded, assert_sandbox,
                                 sandbox_write)

SANDBOX_URL = "https://amuslimcf-sandbox.fcsuite.com/api/v2"
PROD_URL = "https://amuslimcf.fcsuite.com/api/v2"


# ===========================================================================
# STEP 1 — HTTP status is surfaced and classified
# ===========================================================================

class Response:
    def __init__(self, status_code=200, payload=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload or {})

    def json(self):
        if self._payload is None:
            raise json.JSONDecodeError("no json", self.text or "", 0)
        return self._payload


def client_returning(monkeypatch, response=None, exception=None,
                     env="sandbox"):
    client = CSuiteClient(env=env,
                          base_url=SANDBOX_URL if env == "sandbox" else PROD_URL,
                          api_key="k", api_secret="s")

    def post(url, **kwargs):
        if exception is not None:
            raise exception
        return response

    monkeypatch.setattr(client.session, "post", post)
    return client


class TestStatusClassification:
    """2026-09-30: a deliberate 401 came back as "Unknown error".

    Telling an expired key from a malformed body meant capturing the raw
    response by hand.
    """

    @pytest.mark.parametrize("code, outcome", [
        (200, OUTCOME_OK), (201, OUTCOME_OK),
        (400, OUTCOME_INVALID_REQUEST), (422, OUTCOME_INVALID_REQUEST),
        (401, OUTCOME_AUTH_REJECTED), (403, OUTCOME_AUTH_REJECTED),
        (429, OUTCOME_RATE_LIMITED),
        (500, OUTCOME_SERVER_ERROR), (503, OUTCOME_SERVER_ERROR),
        (None, OUTCOME_NETWORK),
    ])
    def test_a_status_maps_to_an_outcome(self, code, outcome):
        assert classify_status(code) == outcome

    def test_a_transport_failure_is_a_network_error(self):
        assert classify_status(200, exception=RuntimeError("boom")) == \
            OUTCOME_NETWORK

    def test_an_auth_rejection_is_named_not_called_unknown(self, monkeypatch):
        """The exact shape the sandbox key returned against production."""
        client = client_returning(monkeypatch, Response(
            401, {"need_auth": 1, "success": 0},
            text='{"need_auth":1,"success":0}'))
        result = client._request("profile/list", {})

        assert result["http_status"] == 401
        assert result["outcome"] == OUTCOME_AUTH_REJECTED
        assert result["success"] is False
        assert "need_auth" in result["body"]

    def test_an_invalid_request_is_named(self, monkeypatch):
        """The shape the sandbox returned for a mismatched env."""
        client = client_returning(monkeypatch, Response(
            400, {"success": 0,
                  "errors": ["Invalid env (we are [sandbox], you think we "
                             "are [live]"]}))
        result = client._request("profile/list", {})
        assert result["http_status"] == 400
        assert result["outcome"] == OUTCOME_INVALID_REQUEST
        assert "Invalid env" in result["error"]

    def test_rate_limiting_is_named(self, monkeypatch):
        client = client_returning(monkeypatch,
                                  Response(429, {"success": 0, "errors": []}))
        result = client._request("profile/list", {})
        assert result["outcome"] == OUTCOME_RATE_LIMITED

    def test_a_server_error_is_named(self, monkeypatch):
        client = client_returning(monkeypatch,
                                  Response(503, {"success": 0, "errors": []}))
        result = client._request("profile/list", {})
        assert result["outcome"] == OUTCOME_SERVER_ERROR

    def test_a_timeout_is_a_network_error(self, monkeypatch):
        import requests
        client = client_returning(
            monkeypatch, exception=requests.exceptions.Timeout("timed out"))
        result = client._request("profile/list", {})
        assert result["outcome"] == OUTCOME_NETWORK
        assert result["http_status"] is None
        assert result["success"] is False

    def test_a_2xx_that_is_not_json_is_a_bad_response(self, monkeypatch):
        client = client_returning(monkeypatch,
                                  Response(200, None, text="<html>nope</html>"))
        result = client._request("profile/list", {})
        assert result["outcome"] == OUTCOME_BAD_RESPONSE
        assert result["http_status"] == 200

    def test_a_2xx_saying_success_zero_is_a_rejection_not_a_fault(
            self, monkeypatch):
        """HTTP was fine; the application said no. Different problem."""
        client = client_returning(monkeypatch, Response(
            200, {"success": 0, "errors": ["Missing required field"]}))
        result = client._request("profile/list", {})
        assert result["http_status"] == 200
        assert result["outcome"] == OUTCOME_REJECTED

    def test_a_success_carries_its_status_too(self, monkeypatch):
        client = client_returning(monkeypatch, Response(
            200, {"success": 1, "data": {"count": 3}}))
        result = client._request("profile/list", {})
        assert result["success"] is True
        assert result["http_status"] == 200
        assert result["outcome"] == OUTCOME_OK

    def test_existing_callers_keep_working(self, monkeypatch):
        """Everything that read success/data/error still reads them."""
        client = client_returning(monkeypatch, Response(
            200, {"success": 1, "data": {"count": 7}, "messages": ["hi"]}))
        result = client._request("profile/list", {})
        assert result["success"] is True
        assert result["data"] == {"count": 7}
        assert result["messages"] == ["hi"]

    def test_a_response_body_is_length_capped(self, monkeypatch):
        client = client_returning(monkeypatch, Response(
            500, {"success": 0, "errors": ["x"]}, text="y" * 5000))
        result = client._request("profile/list", {})
        assert len(result["body"]) <= csuite.MAX_BODY_CHARS + 1


# ===========================================================================
# Sandbox-only guard
# ===========================================================================

class Exploding:
    """Any request is a call that should never have been made."""

    base_url = PROD_URL
    env = "live"
    api_key = "k"
    api_secret = "s"

    def _request(self, endpoint, data=None):
        raise AssertionError(f"a refused write was sent: {endpoint}")


def sandbox_client():
    return CSuiteClient(env="sandbox", base_url=SANDBOX_URL,
                        api_key="k", api_secret="s")


class TestSandboxOnly:

    def test_the_sandbox_client_passes(self):
        assert assert_sandbox(sandbox_client()) == SANDBOX_HOSTNAME

    def test_a_production_client_is_refused(self):
        client = CSuiteClient(env="live", base_url=PROD_URL,
                              api_key="k", api_secret="s")
        with pytest.raises(NotSandbox) as caught:
            assert_sandbox(client)
        assert "sandbox-only" in str(caught.value)
        assert "Nothing was sent" in str(caught.value)

    def test_a_write_to_production_is_never_sent(self):
        """The client raises on any request, so a leak fails the test."""
        budget = WriteBudget(2)
        with pytest.raises(NotSandbox):
            sandbox_write(Exploding(), "profile/create/individual",
                          {"first_name": "x"}, budget)
        assert budget.used == 0, "a refused write still spent the budget"

    @pytest.mark.parametrize("host", [
        "https://amuslimcf.fcsuite.com/api/v2",
        "https://amuslimcf-sandbox.fcsuite.com.evil.example/api/v2",
        "https://sandbox-notreally.fcsuite.com/api/v2",
        "https://amuslimcf.fcsuite.com/api/v2/sandbox",
        "https://amuslimcf.fcsuite.com/api/v2?env=sandbox",
        "",
    ])
    def test_only_the_exact_sandbox_hostname_passes(self, host):
        """Not a substring test.

        `amuslimcf-sandbox.fcsuite.com.evil.example` contains the sandbox
        name, and `amuslimcf.fcsuite.com/api/v2/sandbox` contains it in
        the path.
        """
        class Client:
            base_url = host
            env = "sandbox"
            api_key = "k"
            api_secret = "s"

        with pytest.raises(NotSandbox):
            assert_sandbox(Client())

    def test_the_sandbox_host_with_a_live_env_is_refused(self):
        """Reassigning .env after construction must not get past this.

        The diagnostic runs on 2026-09-30 did exactly that reassignment,
        which is why the guard re-derives instead of trusting.
        """
        client = sandbox_client()
        client.env = "live"
        with pytest.raises(NotSandbox) as caught:
            assert_sandbox(client)
        assert "mismatched env" in str(caught.value)

    def test_a_missing_credential_is_refused(self):
        client = sandbox_client()
        client.api_key = ""
        with pytest.raises(Exception):
            assert_sandbox(client)

    def test_a_read_endpoint_does_not_go_through_the_write_budget(self):
        budget = WriteBudget(2)
        with pytest.raises(ValueError) as caught:
            sandbox_write(sandbox_client(), "profile/list", {}, budget)
        assert "not a write endpoint" in str(caught.value)
        assert budget.used == 0


# ===========================================================================
# The hard cap
# ===========================================================================

class TestWriteBudget:
    """A cap written in a brief is a cap until someone is tired."""

    def test_it_allows_exactly_its_limit(self):
        budget = WriteBudget(2)
        assert budget.spend("profile/create/individual") == 1
        assert budget.spend("profile/edit") == 2
        assert budget.used == 2
        assert budget.remaining == 0

    def test_the_third_write_raises(self):
        budget = WriteBudget(2)
        budget.spend("profile/create/individual")
        budget.spend("profile/edit")
        with pytest.raises(WriteBudgetExceeded) as caught:
            budget.spend("profile/edit")
        assert "capped at 2" in str(caught.value)
        assert "Nothing was sent" in str(caught.value)
        assert budget.used == 2, "a refused write incremented the counter"

    def test_a_budget_of_zero_allows_nothing(self):
        budget = WriteBudget(0)
        with pytest.raises(WriteBudgetExceeded):
            budget.spend("profile/edit")

    def test_the_budget_stops_the_request_not_just_the_count(self):
        """The client raises on any request, so a leak fails the test."""
        class Recording:
            base_url = SANDBOX_URL
            env = "sandbox"
            api_key = "k"
            api_secret = "s"
            calls = []

            def _request(self, endpoint, data=None):
                Recording.calls.append(endpoint)
                return {"success": True, "data": {}}

        client = Recording()
        budget = WriteBudget(1)
        sandbox_write(client, "profile/create/individual", {}, budget)
        with pytest.raises(WriteBudgetExceeded):
            sandbox_write(client, "profile/edit", {}, budget)
        assert Recording.calls == ["profile/create/individual"]

    def test_a_failed_write_still_costs_its_budget(self):
        """Attempts, not successes.

        A call that was sent and failed still consumed the thing the cap
        exists to limit — a request that may have changed the far side.
        """
        class Failing:
            base_url = SANDBOX_URL
            env = "sandbox"
            api_key = "k"
            api_secret = "s"

            def _request(self, endpoint, data=None):
                return {"success": False, "error": "boom", "http_status": 500}

        budget = WriteBudget(1)
        sandbox_write(Failing(), "profile/create/individual", {}, budget)
        assert budget.used == 1
        with pytest.raises(WriteBudgetExceeded):
            sandbox_write(Failing(), "profile/edit", {}, budget)

    def test_it_records_what_it_spent_on(self):
        budget = WriteBudget(2)
        budget.spend("profile/create/individual")
        budget.spend("profile/edit")
        assert budget.log == ["profile/create/individual", "profile/edit"]

    def test_there_is_no_retry_anywhere_in_the_write_path(self):
        import inspect
        from sync import sandbox_writes
        source = inspect.getsource(sandbox_writes)
        assert "retry" not in source.lower().replace("no retry", "").replace(
            "not retry", "").replace("never retries", "")
        assert "for attempt" not in source
        assert "while" not in source
