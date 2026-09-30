"""CSuite: which database, which credentials, and what counts as a write.

No network. Nothing here reads a real key or secret; the signing test
uses the vendor's published example values, which are not credentials for
anything.
"""

import json

import pytest

from clients import csuite
from clients.csuite import (CSuiteClient, CSuiteEnvMismatch, ENV_LIVE,
                            ENV_SANDBOX, host_of, host_looks_like_sandbox,
                            is_csuite_write, resolve_csuite_env)

LIVE_HOST = "https://amuslimcf.fcsuite.com/api/v2"
SANDBOX_HOST = "https://amuslimcf-sandbox.fcsuite.com/api/v2"


# ===========================================================================
# PART 2 — write classification
# ===========================================================================

class TestWriteClassification:
    """CSuite signs the body, so every call is a POST.

    The verb carries no information about whether a call changes
    anything; the endpoint name is the only signal. Getting this wrong in
    the permissive direction means a "read-only" job writing to a live
    fund ledger.
    """

    @pytest.mark.parametrize("endpoint", [
        "profile/create/individual",
        "profile/create/org",
        "profile/create/household",
        "task/edit/complete",
        "task/complete",
        "custom_field/delete",
        "/api/v1/note/create",
        # Already covered before this change; kept so the guard cannot
        # regress on them either.
        "profile/edit",
        "funit/create",
        "event/create/eventdate",
        "event/edit/eventdate",
    ])
    def test_a_write_path_is_a_write(self, endpoint):
        assert is_csuite_write(endpoint) is True

    @pytest.mark.parametrize("endpoint", [
        "profile/list",
        "profile/display",
        "note/list/type",
        "custom_field/list",
        "custom_field/answers",
        "opportunity/list",
        "task/list",
        # The rest of the read surface, so a widened rule cannot start
        # refusing the reads the app depends on.
        "event/list/dates",
        "event/display/eventdate",
        "donation/list",
        "grant/list",
        "funit/display",
        "funit/list/search",
        "check/list",
    ])
    def test_a_read_path_is_a_read(self, endpoint):
        assert is_csuite_write(endpoint) is False

    @pytest.mark.parametrize("endpoint, expected", [
        ("/api/v1/note/create", True),
        ("api/v2/profile/create/individual", True),
        ("/api/v2/profile/list", False),
        ("v1/custom_field/delete", True),
    ])
    def test_a_version_prefix_changes_nothing(self, endpoint, expected):
        """Stripped, so a future /api/v3/ cannot smuggle a write past a
        rule that only knew about v2."""
        assert is_csuite_write(endpoint) is expected

    @pytest.mark.parametrize("endpoint", [
        "profile/createhousehold",      # word inside a segment
        "PROFILE/CREATE/INDIVIDUAL",    # case
        "profile\\\\create\\\\individual",  # backslashes
        "profile/create/individual?x=1",
        " profile/create/individual ",
    ])
    def test_the_guard_is_not_less_strict_than_a_substring_rule(self, endpoint):
        """The union with the whole-string check exists for exactly this.

        Segment-equality alone would let `createhousehold` through.
        """
        assert is_csuite_write(endpoint) is True

    @pytest.mark.parametrize("endpoint", [None, "", "   ", "/"])
    def test_nothing_is_not_a_write(self, endpoint):
        assert is_csuite_write(endpoint) is False

    def test_every_word_in_the_pattern_list_is_still_enforced(self):
        """Including `update`, which was in the rule before this change.

        The brief named create/edit/delete/complete; dropping `update`
        would have made the guard less strict than it was.
        """
        assert "update" in csuite.CSUITE_WRITE_PATTERNS
        for word in csuite.CSUITE_WRITE_PATTERNS:
            assert is_csuite_write(f"thing/{word}") is True


# ===========================================================================
# PART 1 — environment resolution
# ===========================================================================

class TestEnvResolution:

    def test_unset_means_live_so_production_is_unchanged(self, monkeypatch):
        monkeypatch.delenv("CSUITE_ENV", raising=False)
        monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
        resolved = resolve_csuite_env(base_url=LIVE_HOST, key="k", secret="s")
        assert resolved["env"] == ENV_LIVE
        assert resolved["key_var"] == "CSUITE_API_KEY"

    def test_the_production_key_comes_from_csuite_api_key(self):
        resolved = resolve_csuite_env(env="live", base_url=LIVE_HOST,
                                      key="k", secret="s")
        assert resolved["key_var"] == "CSUITE_API_KEY"
        assert resolved["secret_var"] == "CSUITE_API_SECRET"

    def test_sandbox_selects_the_sandbox_pair(self):
        resolved = resolve_csuite_env(
            env="sandbox", base_url=SANDBOX_HOST,
            sandbox_key="k", sandbox_secret="s")
        assert resolved["env"] == ENV_SANDBOX
        assert resolved["key_var"] == "CSUITE_SANDBOX_KEY"
        assert resolved["secret_var"] == "CSUITE_SANDBOX_SECRET"
        assert host_looks_like_sandbox(resolved["base_url"])

    def test_sandbox_env_against_a_production_host_is_refused(self):
        """The dangerous direction: a live host may well accept it."""
        with pytest.raises(CSuiteEnvMismatch) as caught:
            resolve_csuite_env(env="sandbox", base_url=LIVE_HOST,
                               sandbox_key="k", sandbox_secret="s")
        assert "not a sandbox host" in str(caught.value)

    def test_live_env_against_a_sandbox_host_is_refused(self):
        with pytest.raises(CSuiteEnvMismatch) as caught:
            resolve_csuite_env(env="live", base_url=SANDBOX_HOST,
                               key="k", secret="s")
        assert "is a sandbox host" in str(caught.value)

    @pytest.mark.parametrize("env", ["prod", "production", "test", "LIVE2", "x"])
    def test_an_unknown_env_value_is_refused(self, env):
        with pytest.raises(CSuiteEnvMismatch) as caught:
            resolve_csuite_env(env=env, base_url=LIVE_HOST,
                               key="k", secret="s")
        assert "allowed values" in str(caught.value)

    @pytest.mark.parametrize("key, secret, missing", [
        ("", "s", "CSUITE_SANDBOX_KEY"),
        ("k", "", "CSUITE_SANDBOX_SECRET"),
        ("", "", "CSUITE_SANDBOX_KEY"),
    ])
    def test_a_missing_credential_is_refused_by_name(self, key, secret,
                                                     missing):
        with pytest.raises(CSuiteEnvMismatch) as caught:
            resolve_csuite_env(env="sandbox", base_url=SANDBOX_HOST,
                               sandbox_key=key, sandbox_secret=secret)
        message = str(caught.value)
        assert missing in message
        # Names only. A refusal that echoed the value would put a key in
        # a log line.
        assert "no value is read" in message

    @pytest.mark.parametrize("url, expected", [
        ("https://amuslimcf-sandbox.fcsuite.com/api/v2", True),
        ("https://amuslimcf.fcsuite.com/api/v2", False),
        # A path or query cannot spoof the hostname.
        ("https://amuslimcf.fcsuite.com/api/v2/sandbox", False),
        ("https://amuslimcf.fcsuite.com/api/v2?env=sandbox", False),
    ])
    def test_sandbox_is_decided_by_hostname_not_by_the_url(self, url,
                                                           expected):
        assert host_looks_like_sandbox(url) is expected

    def test_host_of_strips_scheme_and_path(self):
        assert host_of("https://amuslimcf.fcsuite.com/api/v2") == \
            "amuslimcf.fcsuite.com"

    def test_one_place_decides_all_three(self):
        """Host, body `env` and credentials are chosen together.

        A host picked in one place and an `env` picked in another is how a
        sandbox run writes to the live ledger.
        """
        import inspect
        source = inspect.getsource(CSuiteClient.__init__)
        assert "resolve_csuite_env" in source
        for chosen in ("Config.CSUITE_API_KEY", "Config.CSUITE_BASE_URL",
                       'self.env = "live"'):
            assert chosen not in source

    def test_the_client_refuses_to_construct_on_a_mismatch(self):
        with pytest.raises(CSuiteEnvMismatch):
            CSuiteClient(env="sandbox", base_url=LIVE_HOST,
                         api_key="k", api_secret="s")

    def test_the_client_records_where_its_credentials_came_from(self):
        client = CSuiteClient(env="live", base_url=LIVE_HOST,
                              api_key="k", api_secret="s")
        assert client.key_var == "CSUITE_API_KEY"
        assert client.env == "live"


# ===========================================================================
# PART 3 — signing
# ===========================================================================

class TestSigning:
    """The vendor's published example. These are not credentials."""

    VENDOR_KEY = "Testing"
    VENDOR_SECRET = "secrettestingkey12345"
    VENDOR_BODY = '{"epoch":1738194125}'
    VENDOR_SIGNATURE = "88Yvc7LkjSq7fwJUJ6552DgSc15PlVyEX4uTmBhQ+9o="

    def client(self):
        return CSuiteClient(env="live", base_url=LIVE_HOST,
                            api_key=self.VENDOR_KEY,
                            api_secret=self.VENDOR_SECRET)

    def test_the_vendor_example_reproduces(self):
        assert self.client()._generate_signature(self.VENDOR_BODY) == \
            self.VENDOR_SIGNATURE

    def test_the_example_signature_requires_compact_json(self):
        """A finding, pinned.

        The vendor's expected signature is over `{"epoch":1738194125}` —
        no space after the colon. `json.dumps` without `separators`
        produces `{"epoch": 1738194125}`, which signs to something else.
        The client is self-consistent (it signs the string it sends), so
        CSuite accepts it — but only because CSuite verifies against the
        bytes it received rather than re-serialising. If that ever
        changes, every call breaks at once.
        """
        client = self.client()
        spaced = json.dumps({"epoch": 1738194125})
        compact = json.dumps({"epoch": 1738194125}, separators=(",", ":"))

        assert compact == self.VENDOR_BODY
        assert spaced != self.VENDOR_BODY
        assert client._generate_signature(compact) == self.VENDOR_SIGNATURE
        assert client._generate_signature(spaced) != self.VENDOR_SIGNATURE

    def test_the_client_signs_the_exact_bytes_it_sends(self, monkeypatch):
        """One serialization, used for both. Anything else is a 401 a
        month from now that nobody can reproduce."""
        sent = {}

        class Response:
            status_code = 200

            def json(self):
                return {"success": 1, "data": {}}

        client = self.client()

        def capture(url, **kwargs):
            sent["body"] = kwargs.get("data")
            sent["signature"] = (kwargs.get("headers") or {}).get("SIGNATURE")
            sent["signer"] = (kwargs.get("headers") or {}).get("SIGNER")
            return Response()

        monkeypatch.setattr(client.session, "post", capture)
        client._request("profile/list", {"view_limit": 1})

        assert isinstance(sent["body"], str), \
            "the body must be a pre-serialised string, or requests will " \
            "re-encode it and the signature will cover different bytes"
        assert sent["signature"] == \
            client._generate_signature(sent["body"])
        assert sent["signer"] == self.VENDOR_KEY

    def test_the_signed_body_carries_env_and_epoch(self, monkeypatch):
        sent = {}

        class Response:
            status_code = 200

            def json(self):
                return {"success": 1, "data": {}}

        client = self.client()
        monkeypatch.setattr(
            client.session, "post",
            lambda url, **kw: (sent.update(body=kw.get("data")), Response())[1])
        client._request("profile/list")

        body = json.loads(sent["body"])
        assert body["env"] == "live"
        assert isinstance(body["epoch"], int)

    def test_the_sandbox_client_signs_env_sandbox(self, monkeypatch):
        sent = {}

        class Response:
            status_code = 200

            def json(self):
                return {"success": 1, "data": {}}

        client = CSuiteClient(env="sandbox", base_url=SANDBOX_HOST,
                              api_key="k", api_secret="s")
        monkeypatch.setattr(
            client.session, "post",
            lambda url, **kw: (sent.update(body=kw.get("data")), Response())[1])
        client._request("profile/list")

        assert json.loads(sent["body"])["env"] == "sandbox"
        assert host_looks_like_sandbox(client.base_url)
