"""Shared pytest configuration.

Three jobs:

1. Put the repo root on sys.path so tests can import the application
   packages (clients/, content/, intents/) regardless of where pytest
   is invoked from.
2. Guarantee DATABASE_URL is UNSET for every test. Step 1a's whole
   point is that import and collection must not require a database, so
   the suite must never silently pass because a developer happened to
   have DATABASE_URL exported in their shell.
3. Block outbound network. See the note on _block_outbound_network.
"""

import os
import socket
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


@pytest.fixture(autouse=True)
def _unset_database_url(monkeypatch):
    """Remove every database URL for the duration of each test.

    Both of them: since 2026-09-23 clients.database prefers
    DATABASE_PUBLIC_URL when it is set and the process is not running
    inside Railway, so leaving it behind would let a developer's .env
    make "no database configured" tests pass for the wrong reason.
    """
    for name in ("DATABASE_URL", "DATABASE_PUBLIC_URL"):
        monkeypatch.delenv(name, raising=False)


class AuditStore:
    """An in-memory stand-in for the `write_audit` table.

    Since 2026-09-23 auditing is pre-flight: clients.audit.reserve_write
    claims a row BEFORE the request goes out and refuses the write if it
    cannot. That makes an audit store a precondition for any test that
    exercises a write — without one, every write is correctly refused and
    the test ends up asserting on the refusal instead of on its subject.

    Tests that want to see the refusal install their own failing store;
    this one always succeeds.
    """

    def __init__(self):
        self.rows = []

    def __call__(self, sql, params=None, fetch=True):
        text = " ".join(str(sql).split())
        if text.startswith("UPDATE write_audit"):
            status, http_status, error, duration_ms, target_id, row_id = params
            row = self.rows[int(row_id) - 1]
            row.update(status=status, http_status=http_status, error=error,
                       duration_ms=duration_ms)
            if target_id is not None:
                row["target_id"] = target_id
            return 1
        self.rows.append({"params": params})
        if "RETURNING id" in text:
            return [{"id": len(self.rows)}]
        return 1


@pytest.fixture
def audit_store(monkeypatch):
    """A working audit store, so writes are not refused pre-flight."""
    store = AuditStore()
    monkeypatch.setattr("clients.database.execute_query", store)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)
    return store


# ---------------------------------------------------------------------------
# Outbound network
# ---------------------------------------------------------------------------
#
# 2026-10-06: a test that called intents.sync_commands.handle("sync all")
# without stubbing the individual syncs ran the REAL newsletter sync against
# production. It paged live CSuite for every profile with a newsletter opt-in —
# 70 seconds of it — and would then have POSTed a subscription change per
# contact. Nothing was written, but not because the test was careful: writes
# are audited pre-flight, reserve_write needs a database, and job 2 above had
# removed it. The read half had nothing stopping it at all.
#
# Reads are not harmless. They carry live donor data into a test process, they
# spend API rate limit, and a suite that sometimes talks to production is a
# suite whose green does not mean the same thing twice.
#
# The block is at the SOCKET layer, not the HTTP library, because the two
# clients reach the network by different routes: clients/hubspot.py calls the
# module-level requests.get/post/patch helpers, while clients/csuite.py posts
# through a requests.Session. A socket-level guard covers both, plus urllib3
# directly and anything added later.

class OutboundNetworkBlocked(RuntimeError):
    """A test tried to open a connection off this machine.

    Stub the client at its seam, or mark the test
    `@pytest.mark.allow_network` if reaching the outside world really is the
    subject of the test.
    """


# Loopback and unix sockets stay open: a local fixture server is a legitimate
# thing for a test to talk to, and blocking it would be blocking localhost,
# which is not what this is for.
_ALLOWED_HOSTS = frozenset({
    "127.0.0.1", "::1", "localhost", "localhost.localdomain", "0.0.0.0", "",
})


def _is_local(address) -> bool:
    if not isinstance(address, (tuple, list)) or not address:
        return True          # AF_UNIX and anything unrecognised
    host = address[0]
    if isinstance(host, bytes):
        host = host.decode("utf-8", "replace")
    return str(host) in _ALLOWED_HOSTS


def _refuse(address, caller: str):
    host = address[0] if isinstance(address, (tuple, list)) and address \
        else address
    port = address[1] if isinstance(address, (tuple, list)) and \
        len(address) > 1 else "?"
    test = os.environ.get("PYTEST_CURRENT_TEST", "this test").split(" (")[0]
    raise OutboundNetworkBlocked(
        f"{test} tried to reach {host}:{port} via {caller}. The test suite "
        f"does not talk to the outside world: stub the client at its seam, or "
        f"mark the test @pytest.mark.allow_network if the connection IS the "
        f"subject of the test.")


@pytest.fixture(autouse=True)
def _block_outbound_network(request, monkeypatch):
    """Refuse any connection to anything but loopback.

    Autouse, so a test has to opt OUT deliberately. The marker is registered
    in pytest.ini, so a misspelling is an error rather than a silent
    no-op — a guard you can disable by typo is not a guard.
    """
    if request.node.get_closest_marker("allow_network"):
        return

    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create = socket.create_connection
    real_getaddrinfo = socket.getaddrinfo

    def getaddrinfo(host, port, *args, **kwargs):
        # Blocked FIRST, for two reasons. It stops the DNS query itself
        # leaving the machine, and it is the only place the HOSTNAME is still
        # visible: urllib3 resolves before it connects, so a connect-only
        # guard reports "104.16.50.78" where this reports "api.hubapi.com",
        # and only one of those tells you which test to fix.
        if not _is_local((host, port)):
            _refuse((host, port), "socket.getaddrinfo")
        return real_getaddrinfo(host, port, *args, **kwargs)

    # The three below are backstops: they catch code that connects to a
    # literal IP and so never resolves anything.
    def connect(self, address, *args, **kwargs):
        if not _is_local(address):
            _refuse(address, "socket.connect")
        return real_connect(self, address, *args, **kwargs)

    def connect_ex(self, address, *args, **kwargs):
        if not _is_local(address):
            _refuse(address, "socket.connect_ex")
        return real_connect_ex(self, address, *args, **kwargs)

    def create_connection(address, *args, **kwargs):
        if not _is_local(address):
            _refuse(address, "socket.create_connection")
        return real_create(address, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "create_connection", create_connection)
