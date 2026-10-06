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

from tests.conftest import OutboundNetworkBlocked


def test_a_real_hubspot_client_cannot_reach_hubspot():
    """The seam clients/hubspot.py uses: module-level requests helpers."""
    from clients.hubspot import HubSpotClient

    client = HubSpotClient()
    with pytest.raises(OutboundNetworkBlocked) as caught:
        client._get("crm/v3/objects/contacts", {"limit": 1})

    # The HOSTNAME, not the IP it resolves to: urllib3 resolves before it
    # connects, so blocking getaddrinfo is what keeps the message diagnosable.
    assert "api.hubapi.com" in str(caught.value)
    assert "allow_network" in str(caught.value), "say how to opt out"


def test_a_real_csuite_client_cannot_reach_csuite():
    """A different seam: clients/csuite.py posts through a requests.Session,
    which is why the block is on the socket and not on the HTTP library."""
    from clients.csuite import CSuiteClient

    client = CSuiteClient()
    with pytest.raises(OutboundNetworkBlocked):
        client._request("profile/display", {"profile_id": 1})


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
