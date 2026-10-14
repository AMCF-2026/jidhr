"""The suite does not talk to the outside world.

On 2026-10-06 a test called intents.sync_commands.handle("sync all") without
stubbing the individual syncs, and ran the REAL newsletter sync against
production: 70 seconds of paging live CSuite for every newsletter opt-in,
which would then have POSTed a subscription change per contact. Nothing was
written, and not because the test was careful — writes are audited pre-flight,
reserve_write needs a database, and conftest had already removed it. The read
half had nothing stopping it.

These tests exist because a guard that nothing happens to trip is
indistinguishable from no guard at all.
"""

import socket

import pytest

from tests.conftest import PLACEHOLDER_CREDENTIALS, OutboundNetworkBlocked


def holds(value, name) -> bool:
    """Whether a client holds the placeholder for `name`.

    A bare bool on purpose. `assert client.api_key == PLACEHOLDER` makes
    pytest print BOTH sides when it fails, and on 2026-10-14 that printed
    part of a real CSuite key to the terminal. Never compare a credential in
    a way that can show it.
    """
    return value == PLACEHOLDER_CREDENTIALS[name]


def test_a_real_hubspot_client_cannot_reach_hubspot():
    """The seam clients/hubspot.py uses: module-level requests helpers.

    A placeholder token, because a client with NO token returns "not
    configured" before it touches the network — so in a checkout without
    .env this test used to fail with DID NOT RAISE, proving nothing.
    """
    import clients.hubspot as hubspot

    # The token is conftest's placeholder (V5.83), set on the Config the
    # client module holds — see _placeholder_credentials.
    client = hubspot.HubSpotClient()
    assert holds(client.access_token, "HUBSPOT_ACCESS_TOKEN")
    with pytest.raises(OutboundNetworkBlocked) as caught:
        client._get("crm/v3/objects/contacts", {"limit": 1})

    # The HOSTNAME, not the IP it resolves to: urllib3 resolves before it
    # connects, so blocking getaddrinfo is what keeps the message diagnosable.
    assert "api.hubapi.com" in str(caught.value)
    assert "allow_network" in str(caught.value), "say how to opt out"


def test_a_real_csuite_client_cannot_reach_csuite():
    """A different seam: clients/csuite.py posts through a requests.Session,
    which is why the block is on the socket and not on the HTTP library.

    It holds the conftest placeholder credentials, so it gets past its own
    "not configured" refusal and really tries — which is what makes this a
    test of the guard rather than of the refusal."""
    from clients.csuite import CSuiteClient

    client = CSuiteClient()
    assert holds(client.api_key, "CSUITE_API_KEY")
    with pytest.raises(OutboundNetworkBlocked) as caught:
        client._request("profile/display", {"profile_id": 1})

    assert "amuslimcf.fcsuite.com" in str(caught.value)


def test_a_sandbox_csuite_client_cannot_reach_the_sandbox(monkeypatch):
    import clients.csuite as csuite
    import config

    # resolve_csuite_env reads the CURRENT config.Config (it imports Config
    # inside the function), which after another test's reload is not the
    # class clients.csuite imported. Both are set.
    for holder in (config.Config, csuite.Config):
        monkeypatch.setattr(holder, "CSUITE_ENV", "sandbox")
    client = csuite.CSuiteClient()
    assert holds(client.api_key, "CSUITE_SANDBOX_KEY")
    with pytest.raises(OutboundNetworkBlocked) as caught:
        client._request("profile/display", {"profile_id": 1})

    assert "amuslimcf-sandbox.fcsuite.com" in str(caught.value)


def test_a_csuite_write_cannot_leave_either(monkeypatch):
    """A write goes through the same session. Audit and budget are stubbed
    so the request is actually attempted."""
    import clients.csuite as csuite
    from sync.sandbox_writes import WriteBudget

    monkeypatch.setattr(csuite, "reserve_write", lambda *a, **k: {"id": 1})
    monkeypatch.setattr(csuite, "complete_write", lambda *a, **k: None)
    client = csuite.CSuiteClient()
    client.write_budget = WriteBudget(1)
    client.verify_writes = False
    with pytest.raises(OutboundNetworkBlocked):
        client._request("task/create", {"name": "x", "employee_id": 1,
                                        "due_ts": "2030-01-01"})


def test_every_client_holds_placeholders_never_real_credentials():
    """In a checkout WITH .env the suite used to run holding production
    credentials. conftest replaces them for every test."""
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    assert holds(HubSpotClient().access_token, "HUBSPOT_ACCESS_TOKEN")
    client = CSuiteClient()
    assert holds(client.api_key, "CSUITE_API_KEY")
    assert holds(client.api_secret, "CSUITE_API_SECRET")
    assert all("not-real" in value
               for value in PLACEHOLDER_CREDENTIALS.values())


def test_placeholders_survive_a_config_reload(monkeypatch):
    """The trap the HubSpot guard test fell into: after importlib.reload,
    `config.Config` is a new class, but the clients still read the one they
    imported — which is the one carrying the placeholders."""
    import importlib

    import config
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    importlib.reload(config)
    try:
        assert holds(CSuiteClient().api_key, "CSUITE_API_KEY")
        assert holds(CSuiteClient().api_secret, "CSUITE_API_SECRET")
        assert holds(HubSpotClient().access_token, "HUBSPOT_ACCESS_TOKEN")
    finally:
        importlib.reload(config)


def test_dns_resolution_is_blocked_so_nothing_leaves_at_all():
    with pytest.raises(OutboundNetworkBlocked) as caught:
        socket.getaddrinfo("api.hubapi.com", 443)

    assert "api.hubapi.com" in str(caught.value)


def test_a_bare_socket_connect_is_blocked():
    with pytest.raises(OutboundNetworkBlocked):
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
            ("example.com", 80))


def test_a_literal_ip_is_blocked_even_though_it_resolves_nothing():
    """The backstop. Blocking only getaddrinfo would miss this."""
    with pytest.raises(OutboundNetworkBlocked) as caught:
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
            ("104.16.50.78", 443))

    assert "104.16.50.78:443" in str(caught.value)


def test_connect_ex_is_blocked_too():
    """It returns an errno rather than raising, so a caller using it would
    otherwise slip past a guard that only covered connect()."""
    with pytest.raises(OutboundNetworkBlocked):
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
            ("example.com", 80))


def test_create_connection_is_blocked():
    with pytest.raises(OutboundNetworkBlocked):
        socket.create_connection(("example.com", 80), timeout=1)


def test_the_message_names_the_test_and_the_host():
    with pytest.raises(OutboundNetworkBlocked) as caught:
        socket.create_connection(("api.hubapi.com", 443))

    message = str(caught.value)
    assert "api.hubapi.com:443" in message
    assert "test_the_message_names_the_test_and_the_host" in message


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1"])
def test_loopback_is_not_blocked(host):
    """A local fixture server is a legitimate thing to talk to. Nothing is
    listening, so the real connect refuses — the point is that the GUARD
    does not, which a different exception type proves."""
    try:
        socket.create_connection((host, 1), timeout=0.2)
    except OutboundNetworkBlocked:
        pytest.fail(f"{host} must not be treated as outbound")
    except OSError:
        pass            # nothing listening on port 1, as expected


@pytest.mark.allow_network
def test_the_marker_lifts_the_block():
    """Proves the opt-out works without actually going anywhere: the guard is
    not installed, so a connection to a closed local port fails the ordinary
    way rather than raising OutboundNetworkBlocked."""
    try:
        socket.create_connection(("127.0.0.1", 1), timeout=0.2)
    except OutboundNetworkBlocked:
        pytest.fail("the marker did not lift the guard")
    except OSError:
        pass


def test_the_marker_is_registered():
    """An unregistered marker is a silent no-op, so a typo would disable the
    guard without anyone noticing."""
    config = open("pytest.ini").read()

    assert "allow_network" in config
