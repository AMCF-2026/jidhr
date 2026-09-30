"""
Sandbox write guard
===================
The only place in this repository that may write to CSuite, and it may
only ever write to the sandbox.

Two guards, both refusals rather than warnings:

**Where.** `assert_sandbox()` re-derives the environment through
`resolve_csuite_env()` and separately checks the hostname is exactly
`amuslimcf-sandbox.fcsuite.com`. Both, not either: a client whose
attributes were reassigned after construction would still pass a check
that only read `client.env`, and the diagnostic runs in
`reports/csuite_sandbox_reads.md` did exactly that reassignment. The
hostname is the thing that actually decides which database CSuite talks
to — measured 2026-09-30, when the sandbox refused a body `env` of
"live" with "Invalid env (we are [sandbox], you think we are [live]".

**How many.** `WriteBudget` counts every write and raises on the one
after its limit. A cap written in a brief is a cap until someone is
tired; a cap that raises is a cap.

Nothing here retries. CSuite has no idempotency key, so a create whose
outcome is unknown cannot be safely repeated — the recovery is to search
for what you were about to make, not to make it again.
"""

import logging

from clients.csuite import (host_of, is_csuite_write, resolve_csuite_env,
                            ENV_SANDBOX)
from sync.readback import normalise_payload, verify

logger = logging.getLogger(__name__)

# Exact hostname. Not a substring test: `amuslimcf.fcsuite.com.evil.example`
# contains the production host too, and "contains sandbox" would pass a
# host like `sandbox-notreally.fcsuite.com`.
SANDBOX_HOSTNAME = "amuslimcf-sandbox.fcsuite.com"


class NotSandbox(RuntimeError):
    """A CSuite write was attempted somewhere that is not the sandbox."""


class WriteBudgetExceeded(RuntimeError):
    """More writes were attempted than the run was allowed."""


class WriteBudget:
    """A hard cap on CSuite writes, enforced by raising.

    Counts attempts, not successes: a call that was sent and failed still
    consumed the thing the cap exists to limit, which is requests that
    may have changed the far side.
    """

    def __init__(self, limit: int):
        if limit < 0:
            raise ValueError("a write budget cannot be negative")
        self.limit = limit
        self.used = 0
        self.log = []

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def spend(self, endpoint: str) -> int:
        """Claim one write. Raises rather than returning False."""
        if self.used >= self.limit:
            raise WriteBudgetExceeded(
                f"this run is capped at {self.limit} CSuite write(s) and has "
                f"used {self.used}. Refusing {endpoint!r}. Nothing was sent.")
        self.used += 1
        self.log.append(endpoint)
        return self.used


def assert_sandbox(client) -> str:
    """Raise unless this client writes to the sandbox. Returns the host.

    Checked twice over: the resolver's view of the environment, and the
    hostname on the client as it stands right now.
    """
    host = host_of(getattr(client, "base_url", ""))

    if host != SANDBOX_HOSTNAME:
        raise NotSandbox(
            f"CSuite writes are sandbox-only and this client points at "
            f"{host or '(no host)'!r}, not {SANDBOX_HOSTNAME!r}. "
            "Nothing was sent.")

    if getattr(client, "env", None) != ENV_SANDBOX:
        raise NotSandbox(
            f"the host is the sandbox but the client's env is "
            f"{getattr(client, 'env', None)!r}, not {ENV_SANDBOX!r}. CSuite "
            "rejects a mismatched env, and so does this. Nothing was sent.")

    # Re-derived rather than trusted: the client's attributes can be
    # reassigned after construction, and the diagnostics in
    # reports/csuite_sandbox_reads.md did exactly that.
    resolved = resolve_csuite_env(env=ENV_SANDBOX, base_url=client.base_url,
                                  sandbox_key=getattr(client, "api_key", ""),
                                  sandbox_secret=getattr(client, "api_secret",
                                                         ""))
    if resolved["env"] != ENV_SANDBOX or \
            host_of(resolved["base_url"]) != SANDBOX_HOSTNAME:
        raise NotSandbox(
            "resolve_csuite_env() does not agree this is the sandbox. "
            "Nothing was sent.")
    return host


def sandbox_write(client, endpoint: str, data: dict, budget: WriteBudget,
                  verify_with=None, record_id=None):
    """The single door every CSuite write goes through.

    Order matters: sandbox first, then the budget, then the endpoint must
    actually be a write. A budget spent on a call that was about to be
    refused for being aimed at production would be a cap that quietly
    protected the wrong thing.
    """
    assert_sandbox(client)

    if not is_csuite_write(endpoint):
        raise ValueError(
            f"{endpoint!r} is not a write endpoint. Reads do not go through "
            "the write budget.")

    budget.spend(endpoint)
    logger.info("CSuite SANDBOX write %d/%d: %s",
                budget.used, budget.limit, endpoint)

    # No retry, no wrapper, no fallback. One call, one result, whatever
    # it is. CSuite has no idempotency key, so a second attempt is not a
    # recovery — it is a second record.
    #
    # Emails are normalised on the way out, because matching is exact and
    # case-sensitive: what is stored has to be what a later search will
    # look for.
    sent = normalise_payload(data)
    response = client._request(endpoint, sent)

    if verify_with is None:
        return response

    # Read the record back. A 200 from CSuite means the request was
    # accepted, not that the data was stored — see sync/readback.py.
    if not (isinstance(response, dict) and response.get("success")):
        return response
    target = record_id
    if target is None:
        payload = response.get("data")
        if isinstance(payload, dict):
            target = payload.get("profile_id")
    if target is None:
        target = sent.get("profile_id")
    if target is None:
        logger.warning("no record id to read back after %s; verification "
                       "skipped", endpoint)
        return response

    verify(verify_with, endpoint, sent, target)
    return response
