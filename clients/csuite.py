"""
CSuite Client
=============
Client for CSuite Fund Accounting API with HMAC-SHA256 authentication.

Jidhr v1.3 - Complete client covering:
- Profile CRUD (Kods' DAF workflow)
- Fund CRUD + fee types (Muhi's fee calculations)
- Grant queries with date filtering (quarterly reporting)
- Donation queries with date filtering (Ramadan comparisons)
- Check tracking (Muhi's uncashed check reports)
- Voucher lookups (grant disbursement tracking)
- Event management (Lisa's event workflows)
- Task management (CSuite-side tasks)
- Account + investment strategy lookups
"""

import hashlib
import hmac
import base64
import json
import time
import logging
import requests
from config import Config
from clients.audit import (AuditUnavailable, complete_write,
                           record_write, reserve_write)

logger = logging.getLogger(__name__)

# CSuite signs the JSON request body, so EVERY call is an HTTP POST — the
# verb carries no information about whether a call changes anything. The
# endpoint name is the only signal, so the rule lives here where it can be
# read, rather than being inferred at each call site.
#
# Verified against every endpoint string in this file: these five words
# catch every write and match none of the reads.
CSUITE_WRITE_PATTERNS = ("create", "edit", "delete", "complete", "update")

# Version prefixes that carry no meaning for classification. Stripped so
# `/api/v1/note/create` is judged as `note/create`, and so a future
# `/api/v3/` cannot smuggle a write past a rule that only knew about v2.
_API_PREFIX_SEGMENTS = ("api", "v1", "v2", "v3")


def _path_segments(endpoint: str) -> list:
    """The meaningful, lowercased segments of an endpoint path."""
    text = str(endpoint or "").lower().split("?")[0].split("#")[0]
    return [seg for seg in text.replace("\\", "/").split("/")
            if seg and seg not in _API_PREFIX_SEGMENTS]


def is_csuite_write(endpoint: str) -> bool:
    """True if this CSuite endpoint changes something.

    CSuite signs the request body, so every call is an HTTP POST and the
    verb says nothing about what a call does. The endpoint name is the
    only signal, which is why the rule lives here where it can be read
    rather than being re-derived at each call site.

    Matched on path SEGMENTS, not on the whole string, so every sub-path
    is caught: `profile/create/individual`, `task/edit/complete`,
    `custom_field/delete`, and `/api/v1/note/create` after its version
    prefix is stripped.

    The whole-string check is kept alongside the segment check, as a
    union. It is redundant for every endpoint known today, and it means
    this function can never become LESS strict than the substring rule it
    replaced — an endpoint like `profile/createhousehold`, where the word
    is inside a segment rather than equal to it, still reads as a write.
    """
    whole = str(endpoint or "").lower()
    if any(pattern in whole for pattern in CSUITE_WRITE_PATTERNS):
        return True
    return any(pattern in segment
               for segment in _path_segments(endpoint)
               for pattern in CSUITE_WRITE_PATTERNS)


# ---------------------------------------------------------------------------
# Which CSuite are we talking to
# ---------------------------------------------------------------------------
# Three things have to agree: the host, the `env` value inside the signed
# body, and which key/secret pair signs it. They are derived HERE, once,
# and nothing else is allowed to pick any of them independently — a host
# chosen in one place and an `env` chosen in another is how a sandbox run
# writes to the live fund ledger.

ENV_LIVE = "live"
ENV_SANDBOX = "sandbox"
VALID_ENVS = (ENV_LIVE, ENV_SANDBOX)

# A host is sandbox if its hostname says so. Matched on the hostname, not
# the whole URL, so a query string or a path cannot spoof it.
_SANDBOX_HOST_MARKER = "sandbox"


# What happened to a call, beyond "it didn't work".
#
# On 2026-09-30 a deliberate auth failure came back as
# {"success": false, "error": "Unknown error"} — the 401 had been
# swallowed, and telling an expired key from a malformed body meant
# capturing the raw response by hand. An error that does not name its own
# cause sends someone looking in the wrong place.
OUTCOME_OK = "ok"
OUTCOME_AUTH_REJECTED = "auth_rejected"      # 401 (and 403)
OUTCOME_INVALID_REQUEST = "invalid_request"  # 400, 422
OUTCOME_RATE_LIMITED = "rate_limited"        # 429
OUTCOME_SERVER_ERROR = "server_error"        # 5xx
OUTCOME_NETWORK = "network_error"            # timeout, connection refused
OUTCOME_BAD_RESPONSE = "bad_response"        # 2xx that is not JSON
OUTCOME_REJECTED = "rejected"                # HTTP 2xx, success != 1


# Response bodies are echoed back to the caller so a failure can be read
# without re-running it. CSuite never puts a credential in a response —
# the signature travels in a request header — but the cap is here anyway,
# because a body is the one place an unexpected value could appear.
MAX_BODY_CHARS = 600


def _safe_body(response) -> str:
    """The response body, length-capped, for a diagnosable error."""
    try:
        text = getattr(response, "text", "") or ""
    except Exception:  # pragma: no cover
        return ""
    flat = " ".join(str(text).split())
    return flat if len(flat) <= MAX_BODY_CHARS else flat[:MAX_BODY_CHARS] + "…"


def classify_status(status_code, exception=None) -> str:
    """The outcome name for an HTTP status, or for a transport failure."""
    if exception is not None:
        return OUTCOME_NETWORK
    if status_code is None:
        return OUTCOME_NETWORK
    code = int(status_code)
    if code in (401, 403):
        return OUTCOME_AUTH_REJECTED
    if code == 429:
        return OUTCOME_RATE_LIMITED
    if 400 <= code < 500:
        return OUTCOME_INVALID_REQUEST
    if code >= 500:
        return OUTCOME_SERVER_ERROR
    return OUTCOME_OK


# =============================================================================
# CONFIRMED INPUT FIELD NAMES
# =============================================================================
# CSuite validates the fields it recognises and discards the rest without
# comment. A create with an unrecognised field returns HTTP 200 and a
# profile_id, and the value is simply gone — measured 2026-09-30, when
# `primary_email` was sent to profile/create/individual and read back as
# None. The same happens on reads, where an unrecognised profile/list
# filter returned all 18,797 rows.
#
# So an input name is not confirmed because it appears in profile/display,
# and not because it reads like the field it sets. `primary_email` is a
# valid DISPLAY name and an invalid INPUT name: CSuite's input and output
# vocabularies are two different lists. A name goes below only after a
# value sent under it has been read back off the record.
#
# Everything reaching a write method through **kwargs is checked against
# this set BEFORE the request is built, because after the request there is
# nothing left to check: the response to a dropped field is identical to
# the response to a stored one.
CONFIRMED_INPUT_FIELDS = frozenset({
    "first_name",   # profile/create/individual -> first_name
    "last_name",    # profile/create/individual -> last_name
    "email",        # profile/create/individual, profile/edit -> primary_email
    "website",      # profile/edit -> website
    "phone_number", # profile/edit -> primary_phone_number (CSuite punctuates)
    "env",          # every endpoint; supplied by _build_payload
    "profile_id",   # profile/edit, profile/display
})

# Recognised by CSuite but NOT yet confirmed to store a value, so
# deliberately absent from the set above.
#
# 2026-09-30: profile/create/individual was sent phone_number="555-0100"
# and CSuite answered HTTP 400, `phone_number: phone [5550100] is not
# valid` — it parsed the field, validated it, and rejected the whole
# create. That is the opposite of the `primary_email` case, which returned
# 200 and dropped the value. So CSuite VALIDATES the names it recognises
# and SILENTLY DROPS the ones it does not, which makes a deliberately
# invalid value the cheapest way to test a candidate name: a 400 naming
# the field proves the name is recognised, and no record is created.
#
# phone_number graduated on 2026-09-30: profile/edit on sentinel 21626 was
# sent phone_number="7035550100" and profile/display returned
# primary_phone_number "703-555-0100". Stored, and punctuated by CSuite —
# which is why sync/readback.py now tells a reformatted value apart from a
# dropped one. Nothing is left in this set.
RECOGNISED_UNCONFIRMED_FIELDS = frozenset()

# Names PROVEN not to work as inputs, each by a sandbox write and a
# read-back. They are all valid `profile/display` output names, which is the
# trap: the display list reads like a field list and is not one.
#
# 2026-09-30, profile/create/individual on 21626: `primary_email` -> 200,
# value gone. 2026-09-30, profile/edit on 21626: the four address names
# below -> 200, `success: true`, and **0 of 81 fields changed, modified_ts
# included**. CSuite did not touch the record and said nothing.
#
# Kept so the refusal can cite the evidence instead of only saying "not
# confirmed", and so nobody spends another sandbox write proving it twice.
KNOWN_INVALID_INPUT_FIELDS = {
    "primary_email": "use `email` (confirmed 2026-09-30)",
    "primary_phone_number": "use `phone_number` (confirmed 2026-09-30)",
    "primary_address": "dropped by profile/edit, 2026-09-30",
    "primary_city": "dropped by profile/edit, 2026-09-30",
    "primary_state": "dropped by profile/edit, 2026-09-30",
    "primary_zipcode": "dropped by profile/edit, 2026-09-30",
    "primary_address_string": "a display name; no input name confirmed yet",
}


class UnconfirmedField(ValueError):
    """A write was asked to send a field name no read-back has confirmed.

    Raised before the request is built. Sending it instead would be the
    worse outcome: CSuite would accept the call, drop the field, and
    return the same 200 it returns for a field that was stored.
    """

    def __init__(self, unknown, endpoint: str = ""):
        self.unknown = sorted(unknown)
        self.endpoint = endpoint
        names = ", ".join(repr(n) for n in self.unknown)
        # A name already proven wrong gets the evidence and the replacement,
        # rather than the generic "not confirmed yet" — they are different
        # situations and only one of them needs a sandbox write to resolve.
        proven = [f"{n!r}: {KNOWN_INVALID_INPUT_FIELDS[n]}"
                  for n in self.unknown if n in KNOWN_INVALID_INPUT_FIELDS]
        detail = ("  Proven not to work: " + "; ".join(proven) + "."
                  if proven else "")
        super().__init__(
            f"refusing {endpoint or 'this CSuite write'}: {names} "
            f"{'is' if len(self.unknown) == 1 else 'are'} not a confirmed "
            "CSuite input name. CSuite would accept the call, silently drop "
            "the field and return 200. Confirm the name with a sandbox write "
            f"and a read-back, then add it to CONFIRMED_INPUT_FIELDS.{detail} "
            "Nothing was sent.")


def check_input_fields(names, endpoint: str = "") -> None:
    """Raise UnconfirmedField unless every name has been confirmed."""
    unknown = [n for n in (names or ()) if n not in CONFIRMED_INPUT_FIELDS]
    if unknown:
        logger.error("CSuite %s: unconfirmed field name(s) %s — nothing sent",
                     endpoint or "(write)", sorted(unknown))
        raise UnconfirmedField(unknown, endpoint)


class CSuiteEnvMismatch(RuntimeError):
    """The host, the body `env`, and the credentials do not agree.

    Raised at client construction, before anything can be sent. A
    mismatch is not a thing to detect in a log afterwards.
    """


def host_of(url: str) -> str:
    """The hostname of a base URL, lowercased. No scheme, no path."""
    from urllib.parse import urlparse

    text = str(url or "").strip()
    parsed = urlparse(text if "//" in text else f"//{text}")
    return (parsed.hostname or "").lower()


def host_looks_like_sandbox(url: str) -> bool:
    return _SANDBOX_HOST_MARKER in host_of(url)


def resolve_csuite_env(env=None, base_url=None, key=None, secret=None,
                       sandbox_key=None, sandbox_secret=None,
                       sandbox_base_url=None) -> dict:
    """The single place that decides host, body `env`, and credentials.

    Returns {"env", "base_url", "key_var", "secret_var", "api_key",
    "api_secret"}. The *_var entries name the environment variable each
    credential came from, so a report can say where a key came from
    without printing it.

    Raises CSuiteEnvMismatch when the host and the env disagree, or when
    the selected credential pair is missing. Both are refusals rather
    than warnings: the failure they prevent is a write to the wrong
    database, and there is no safe way to continue past either.
    """
    from config import Config

    env = (env if env is not None else Config.CSUITE_ENV)
    env = str(env or ENV_LIVE).strip().lower()
    if env not in VALID_ENVS:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV is {env!r}; allowed values are "
            f"{' | '.join(VALID_ENVS)}")

    if env == ENV_SANDBOX:
        base_url = base_url if base_url is not None else (
            sandbox_base_url if sandbox_base_url is not None
            else Config.CSUITE_SANDBOX_BASE_URL)
        api_key = sandbox_key if sandbox_key is not None \
            else Config.CSUITE_SANDBOX_KEY
        api_secret = sandbox_secret if sandbox_secret is not None \
            else Config.CSUITE_SANDBOX_SECRET
        key_var, secret_var = "CSUITE_SANDBOX_KEY", "CSUITE_SANDBOX_SECRET"
    else:
        base_url = base_url if base_url is not None else Config.CSUITE_BASE_URL
        api_key = key if key is not None else Config.CSUITE_API_KEY
        api_secret = secret if secret is not None else Config.CSUITE_API_SECRET
        key_var, secret_var = "CSUITE_API_KEY", "CSUITE_API_SECRET"

    sandbox_host = host_looks_like_sandbox(base_url)
    if env == ENV_SANDBOX and not sandbox_host:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV=sandbox but the host is {host_of(base_url)!r}, "
            "which is not a sandbox host. Refusing to start: a sandbox "
            "`env` against a production host is a request the live "
            "system may well accept.")
    if env == ENV_LIVE and sandbox_host:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV=live but the host is {host_of(base_url)!r}, "
            "which is a sandbox host. Refusing to start rather than "
            "guessing which one was meant.")

    missing = [name for name, value in ((key_var, api_key),
                                       (secret_var, api_secret)) if not value]
    if missing:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV={env} needs {' and '.join(missing)}, which "
            f"{'is' if len(missing) == 1 else 'are'} not set. Names only — "
            "no value is read or logged here.")

    return {"env": env, "base_url": base_url, "key_var": key_var,
            "secret_var": secret_var, "api_key": api_key,
            "api_secret": api_secret}


class CSuiteClient:
    """Client for CSuite API with proper HMAC authentication"""
    
    def __init__(self, env=None, base_url=None, api_key=None,
                 api_secret=None):
        # One resolver decides host, body `env` and credentials together,
        # and refuses to start if they disagree. The arguments exist for
        # tests and for the deliberate cross-checks in
        # reports/csuite_sandbox_reads.md; nothing in the app passes them.
        resolved = resolve_csuite_env(
            env=env, base_url=base_url, key=api_key, secret=api_secret,
            sandbox_key=api_key if env == ENV_SANDBOX else None,
            sandbox_secret=api_secret if env == ENV_SANDBOX else None)
        self.env = resolved["env"]
        self.base_url = resolved["base_url"]
        self.api_key = resolved["api_key"]
        self.api_secret = resolved["api_secret"]
        # Variable NAMES, kept so a diagnostic can say where a credential
        # came from without reading its value.
        self.key_var = resolved["key_var"]
        self.secret_var = resolved["secret_var"]
        self.session = requests.Session()
        logger.info("CSuite client: env=%s host=%s key from $%s",
                    self.env, host_of(self.base_url), self.key_var)
    
    # =========================================================================
    # AUTHENTICATION & HTTP
    # =========================================================================
    
    def _generate_signature(self, body: str) -> str:
        """Generate HMAC-SHA256 Base64 signature"""
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            body.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(signature.digest()).decode('utf-8')
    
    def _build_payload(self, data: dict = None) -> dict:
        """Build request payload with required fields"""
        payload = {
            "env": self.env,
            "epoch": int(time.time())
        }
        if data:
            payload.update(data)
        return payload
    
    # Read-back verification on the PRODUCTION path, ON by default.
    #
    # It costs one extra profile/display per profile write. That is the
    # price of knowing whether the write did anything: CSuite returns the
    # same 200 and the same profile_id whether it stored a field or
    # discarded it, so without the read-back a lost field is invisible
    # forever. Only profile writes are covered — see READBACK_TARGETS.
    #
    # A drop does NOT raise here. It annotates the response and logs at
    # ERROR. Raising after a create that succeeded would tell the caller
    # the write failed when a record now exists, and CSuite has no
    # idempotency key, so the natural response to that — try again — makes
    # a second profile. A wrong record is recoverable; a duplicate pair is
    # worse. The sandbox path (sync/sandbox_writes.py) still raises,
    # because there the whole point is to stop.
    verify_writes = True

    # Which write endpoints can be read back, and how. An endpoint absent
    # from this map is not verified — not because it is safe, but because
    # nothing here knows how to look it up.
    READBACK_TARGETS = {
        "profile/create/individual": ("profile/display", "profile_id"),
        "profile/create/org": ("profile/display", "profile_id"),
        "profile/create/household": ("profile/display", "profile_id"),
        "profile/edit": ("profile/display", "profile_id"),
    }

    def _verify_write(self, endpoint: str, sent: dict, response: dict) -> dict:
        """Read the record back; annotate `response` if anything was lost.

        Returns `response`, with `verified` set, and `fields_dropped` added
        when CSuite kept less than it was sent.
        """
        target = self.READBACK_TARGETS.get(str(endpoint or "").strip("/"))
        if not target:
            return response
        display_endpoint, id_field = target

        record_id = None
        payload = response.get("data")
        if isinstance(payload, dict):
            record_id = payload.get(id_field)
        if record_id is None:
            record_id = (sent or {}).get(id_field)
        if record_id is None:
            logger.warning("no %s to read back after %s; write not verified",
                           id_field, endpoint)
            response["verified"] = None
            return response

        # Imported here, not at module scope: sync.readback is a pure
        # module today and this keeps clients.csuite importable on its own
        # if that ever stops being true.
        from sync.readback import FieldDropped, ReadBackUnavailable, verify

        try:
            verify(self._request, endpoint, sent or {}, record_id,
                   id_field=id_field, display_endpoint=display_endpoint)
        except ReadBackUnavailable as e:
            # "Not checked" is not "field lost". Caught before FieldDropped
            # because it is a subclass of it.
            logger.error("could not read %s %s back after CSuite %s: %s",
                         id_field, record_id, endpoint, e)
            response["verified"] = None
            return response
        except FieldDropped as dropped:
            logger.error("CSuite %s on %s %s DID NOT STORE: %s", endpoint,
                         id_field, record_id, sorted(dropped.dropped))
            response["verified"] = False
            response["fields_dropped"] = dropped.dropped
            return response
        except Exception as e:  # the read-back itself failed
            logger.error("read-back after CSuite %s on %s failed: %s",
                         endpoint, record_id, e)
            response["verified"] = None
            return response

        response["verified"] = True
        return response

    def _request(self, endpoint: str, data: dict = None) -> dict:
        """Make authenticated POST request to CSuite API
        
        All CSuite API calls are POST with HMAC-SHA256 signature.
        
        Returns:
            dict with keys: success (bool), data (dict/None), error (str/None),
                           errors (list), messages (list)
        """
        if not self.api_key or not self.api_secret:
            logger.error("CSuite API credentials not configured")
            if is_csuite_write(endpoint):
                # 'skipped', not 'failed': nothing was attempted, so a
                # missing audit row here costs nothing.
                try:
                    record_write(
                        "csuite", "POST", endpoint, payload=data,
                        status="skipped",
                        error="CSuite API credentials not configured",
                        duration_ms=0)
                except AuditUnavailable as e:
                    logger.warning("skipped write not audited: %s", e)
            return {"error": "CSuite API credentials not configured",
                    "success": False, "http_status": None,
                    "outcome": OUTCOME_AUTH_REJECTED}
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        payload = self._build_payload(data)
        body = json.dumps(payload)
        
        headers = {
            "Content-Type": "application/json",
            "SIGNER": self.api_key,
            "SIGNATURE": self._generate_signature(body)
        }
        
        audited = is_csuite_write(endpoint)
        reservation = None
        if audited:
            # Pre-flight: no audit row, no request. Every CSuite call is a
            # POST, so `audited` is decided by endpoint name, not verb —
            # see is_csuite_write.
            try:
                reservation = reserve_write(
                    "csuite", "POST", endpoint, payload=data)
            except AuditUnavailable as e:
                # Raised, not returned — see the matching note in
                # clients/hubspot._send_with_status.
                logger.error("CSuite POST %s REFUSED: %s", endpoint, e)
                raise

        logger.info(f"CSuite POST: {endpoint} | data keys: {list((data or {}).keys())}")

        started = time.perf_counter()

        def audit(status, http_status=None, error=None):
            if audited:
                complete_write(
                    reservation, status=status, http_status=http_status,
                    error=error,
                    duration_ms=(time.perf_counter() - started) * 1000)

        try:
            response = self.session.post(
                url,
                data=body,
                headers=headers,
                timeout=30
            )
            logger.info(f"CSuite Response: {response.status_code}")
            status_code = getattr(response, "status_code", None)

            try:
                json_response = response.json()

                if json_response.get("success") == 1:
                    audit("success", status_code)
                    result = {
                        "success": True,
                        "data": json_response.get("data"),
                        "messages": json_response.get("messages", []),
                        "http_status": status_code,
                        "outcome": OUTCOME_OK,
                    }
                    # A 200 means the request was accepted, not that the
                    # data was stored. Read it back before calling it done.
                    if audited and self.verify_writes:
                        result = self._verify_write(endpoint, data, result)
                    return result
                else:
                    errors = json_response.get("errors", [])
                    logger.warning(f"CSuite API error: {errors}")
                    error_text = errors[0] if errors else "Unknown error"
                    # HTTP 200 with success != 1 is still a failed write.
                    audit("failed", status_code, error_text)
                    # A 2xx whose body says success != 1 is a
                    # rejection by the application, not by HTTP. Named
                    # separately so it is not read as a transport fault.
                    outcome = classify_status(status_code)
                    if outcome == OUTCOME_OK:
                        outcome = OUTCOME_REJECTED
                    return {
                        "success": False,
                        "error": error_text,
                        "errors": errors,
                        "http_status": status_code,
                        "outcome": outcome,
                        "body": _safe_body(response),
                    }

            except json.JSONDecodeError as e:
                logger.error(f"CSuite JSON decode error: {str(e)}")
                audit("failed", status_code, f"Invalid JSON response: {e}")
                return {"error": f"Invalid JSON response: {str(e)}",
                        "success": False, "http_status": status_code,
                        "outcome": (classify_status(status_code)
                                    if classify_status(status_code)
                                    != OUTCOME_OK else OUTCOME_BAD_RESPONSE),
                        "body": _safe_body(response)}

        except requests.exceptions.RequestException as e:
            logger.error(f"CSuite Request error: {str(e)}")
            audit("failed", None, str(e))
            return {"error": str(e), "success": False, "http_status": None,
                    "outcome": OUTCOME_NETWORK}
    
    # =========================================================================
    # PAGINATION HELPER
    # =========================================================================
    
    def _get_all_pages(self, endpoint: str, data: dict = None,
                       max_iterations: int = 200, batch_size: int = 100) -> list:
        """Fetch all pages of a paginated endpoint.
        
        CSuite uses view_offset (not cur_page) for pagination.
        
        Args:
            endpoint: API endpoint
            data: Additional request data (filters, etc.)
            max_iterations: Safety limit to prevent infinite loops
            batch_size: Records per page
            
        Returns:
            list of all result objects across all pages
        """
        all_results = []
        offset = 0
        base_data = data or {}
        
        for _ in range(max_iterations):
            request_data = {
                **base_data,
                "view_limit": batch_size,
                "view_offset": offset
            }
            
            result = self._request(endpoint, request_data)
            
            if not result.get("success"):
                logger.error(f"Pagination failed at offset {offset}: {result.get('error')}")
                break
            
            results = result.get("data", {}).get("results", [])
            if not results:
                break
            
            all_results.extend(results)
            
            if len(results) < batch_size:
                break
            
            offset += batch_size
            
            # Log progress every 500 records
            if len(all_results) % 500 == 0:
                logger.info(f"Fetched {len(all_results)} records from {endpoint}...")
        
        logger.info(f"Retrieved {len(all_results)} total records from {endpoint}")
        return all_results
    
    # =========================================================================
    # PROFILES
    # =========================================================================
    
    def get_profiles(self, limit: int = 100, offset: int = 0) -> dict:
        """Get profiles (donors, vendors, etc.)"""
        return self._request("profile/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_profile(self, profile_id: int) -> dict:
        """Get specific profile details"""
        return self._request("profile/display", {"profile_id": profile_id})
    
    def search_profiles(self, query: str) -> dict:
        """Search profiles by name
        
        Note: Returns mixed results - profiles AND funds matching the query.
        Filter by result['object'] == 'profile' for profiles only.
        """
        return self._request("profile/list/search", {"q": query})
    
    def get_all_profiles(self, max_iterations: int = 200) -> list:
        """Get all profiles across all pages"""
        return self._get_all_pages("profile/list", max_iterations=max_iterations)
    
    def create_individual_profile(self, first_name: str, last_name: str,
                                   email: str = None, phone: str = None,
                                   address: str = None, **kwargs) -> dict:
        """Create an individual profile in CSuite.
        
        Used by: DAF/Endowment inquiry workflow (Kods)
        
        Args:
            first_name: First name (required)
            last_name: Last name (required)
            email: Primary email
            phone: Primary phone number
            address: Primary address
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {
            "first_name": first_name,
            "last_name": last_name,
        }
        if email:
            data["primary_email"] = email
        if phone:
            data["primary_phone_number"] = phone
        if address:
            data["primary_address_string"] = address
        # Checked before the payload is built, so an unconfirmed name is a
        # refusal rather than a silent drop. The three named fields above
        # are the method's own contract and are NOT gated here: two of them
        # are known-wrong and fixing them is a separate change. With
        # verify_writes on, a drop is now reported after the fact.
        check_input_fields(kwargs, "profile/create/individual")
        data.update(kwargs)
        
        logger.info(f"Creating individual profile: {first_name} {last_name}")
        return self._request("profile/create/individual", data)
    
    def create_org_profile(self, organization: str, email: str = None,
                           phone: str = None, **kwargs) -> dict:
        """Create an organization profile in CSuite.
        
        Used by: Nonprofit/org onboarding workflows (Ola)
        
        Args:
            organization: Organization name (required)
            email: Primary email
            phone: Primary phone number
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {"organization": organization}
        if email:
            data["primary_email"] = email
        if phone:
            data["primary_phone_number"] = phone
        check_input_fields(kwargs, "profile/create/org")
        data.update(kwargs)
        
        logger.info(f"Creating org profile: {organization}")
        return self._request("profile/create/org", data)
    
    def create_household_profile(self, household: str, **kwargs) -> dict:
        """Create a household profile in CSuite.
        
        Args:
            household: Household name (required)
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {"household": household}
        check_input_fields(kwargs, "profile/create/household")
        data.update(kwargs)
        
        logger.info(f"Creating household profile: {household}")
        return self._request("profile/create/household", data)
    
    def edit_profile(self, profile_id: int, **kwargs) -> dict:
        """Edit an existing profile.
        
        Args:
            profile_id: CSuite profile ID
            **kwargs: Fields to update (e.g., primary_email, primary_phone_number)
            
        Returns:
            dict with success status
        """
        check_input_fields(kwargs, "profile/edit")
        data = {"profile_id": profile_id, **kwargs}
        logger.info(f"Editing profile {profile_id}: {list(kwargs.keys())}")
        return self._request("profile/edit", data)
    
    # =========================================================================
    # FUNDS
    # =========================================================================
    
    def get_funds(self, limit: int = 100, offset: int = 0) -> dict:
        """Get list of funds"""
        return self._request("funit/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_fund(self, fund_id: int) -> dict:
        """Get specific fund details including balance"""
        return self._request("funit/display", {"funit_id": fund_id})
    
    def search_funds(self, query: str) -> dict:
        """Search funds by name"""
        return self._request("funit/list/search", {"q": query})
    
    def get_all_funds(self, max_iterations: int = 10) -> list:
        """Get all funds across all pages"""
        return self._get_all_pages("funit/list", max_iterations=max_iterations)
    
    def create_fund(self, name: str, fgroup_id: int,
                    cash_account_id: int = None, **kwargs) -> dict:
        """Create a new fund in CSuite.
        
        Used by: DAF/Endowment inquiry workflow (Kods)
        
        Args:
            name: Fund name (required) - e.g., "Smith Family Fund-(DAF0XXX)"
            fgroup_id: Fund group ID (required) - 1002 for DAF, use Config.FUND_GROUP_*
            cash_account_id: Cash account (defaults to Config.DEFAULT_CASH_ACCOUNT_ID)
            **kwargs: Additional fund fields (e.g., fund_type_id, invest_id)
            
        Returns:
            dict with 'data': {'funit_id': int} on success
        """
        data = {
            "name": name,
            "fgroup_id": fgroup_id,
            "cash_account_id": cash_account_id or Config.DEFAULT_CASH_ACCOUNT_ID,
        }
        check_input_fields(kwargs, "funit/create")
        data.update(kwargs)
        
        logger.info(f"Creating fund: {name} (group: {fgroup_id})")
        return self._request("funit/create", data)
    
    def get_fund_groups(self) -> dict:
        """Get fund groups (DAF, Endowment, Fiscal Sponsorship, etc.)"""
        return self._request("funit/list/fgroup")
    
    def get_fund_types(self) -> dict:
        """Get fund types (Permanently Restricted, Temporarily Restricted, etc.)"""
        return self._request("funit/list/fundtype")
    
    def get_fund_fee_types(self) -> dict:
        """Get fund admin fee types and schedules.
        
        Used by: Fee calculation on fund balances (Muhi)
        
        Returns fee structure including:
        - admin_fee_type_name: e.g., "Fund Admin Fees"
        - admin_fee_apply_fee: "quarterly", "annually", etc.
        - admin_fee_min_fee: Minimum fee amount
        - admin_fee_percent: Fee percentage (if flat rate)
        - admin_fee_type_type: "percent_range", "flat", etc.
        - admin_fee_ladder: Whether fees are tiered
        - admin_fee_use_adb: Whether to use average daily balance
        """
        return self._request("funit/feetype")
    
    def get_fund_subgroups(self) -> dict:
        """Get fund subgroups"""
        return self._request("funit/list/fsubgroup")
    
    # =========================================================================
    # DONATIONS
    # =========================================================================
    
    def get_donations(self, limit: int = 100, offset: int = 0) -> dict:
        """Get donations list"""
        return self._request("donation/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_donation(self, donation_id: int) -> dict:
        """Get specific donation details"""
        return self._request("donation/display", {"donation_id": donation_id})
    
    def get_donations_by_profile(self, profile_id: int) -> dict:
        """Get donations for a specific profile"""
        return self._request("donation/list", {"profile_id": profile_id})
    
    def get_donations_by_fund(self, funit_id: int, limit: int = 100, offset: int = 0) -> dict:
        """Get donations for a specific fund"""
        return self._request("donation/list", {
            "funit_id": funit_id,
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_all_donations(self, max_iterations: int = 300) -> list:
        """Get all donations across all pages (24,910+ records)
        
        Warning: This fetches a LOT of data. Use sparingly.
        For targeted queries, use get_donations_by_profile() or get_donations_by_fund().
        """
        return self._get_all_pages("donation/list", max_iterations=max_iterations)
    
    def get_donations_with_limit(self, limit: int = None) -> list:
        """Get donations with optional cap on total records.
        
        Used by: Donation sync, Ramadan comparisons, reporting
        
        Args:
            limit: Max total donations to fetch (None = all)
        """
        all_donations = []
        offset = 0
        batch_size = 100
        
        while True:
            result = self.get_donations(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get donations at offset {offset}")
                break
            
            data = result.get("data", {})
            donations = data.get("results", [])
            
            if not donations:
                break
            
            all_donations.extend(donations)
            
            if limit and len(all_donations) >= limit:
                all_donations = all_donations[:limit]
                break
            
            if len(donations) < batch_size:
                break
            
            offset += batch_size
            
            if offset % 500 == 0:
                logger.info(f"Fetched {offset} donations so far...")
        
        logger.info(f"Retrieved {len(all_donations)} donations")
        return all_donations
    
    # =========================================================================
    # GRANTS
    # =========================================================================
    
    def get_grants(self, limit: int = 100, offset: int = 0) -> dict:
        """Get grants list"""
        return self._request("grant/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_grant(self, grant_id: int) -> dict:
        """Get specific grant details"""
        return self._request("grant/display", {"grant_id": grant_id})
    
    def get_grants_by_fund(self, funit_id: int = None, fund_name_link_id: int = None,
                           limit: int = 100, offset: int = 0) -> dict:
        """Get grants for a specific fund.
        
        Used by: Fund activity summaries, grant reporting
        
        Args:
            funit_id: Fund unit ID
            fund_name_link_id: Fund name link ID (sometimes used instead of funit_id)
            limit: Records per page
            offset: Pagination offset
        """
        data = {
            "view_limit": limit,
            "view_offset": offset
        }
        if funit_id:
            data["funit_id"] = funit_id
        if fund_name_link_id:
            data["fund_name_link_id"] = fund_name_link_id
        return self._request("grant/list", data)
    
    def get_grants_by_profile(self, profile_id: int, limit: int = 100, offset: int = 0) -> dict:
        """Get grants associated with a specific profile"""
        return self._request("grant/list", {
            "profile_id": profile_id,
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_all_grants(self, max_iterations: int = 100) -> list:
        """Get all grants across all pages (5,338+ records)
        
        Used by: Quarterly grant reports, inactive fund analysis
        """
        return self._get_all_pages("grant/list", max_iterations=max_iterations)
    
    # =========================================================================
    # CHECKS
    # =========================================================================
    
    def get_checks(self, limit: int = 100, offset: int = 0) -> dict:
        """Get checks list.
        
        Used by: Uncashed check reports (Muhi)
        
        Check fields include:
        - check_id, check_num, check_date, amount
        - cleared (0/1): Whether the check has been cashed
        - voided (0/1), void_date, void_reason
        - account_name, account_id
        - is_electronic (0/1), memo
        """
        return self._request("check/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_check(self, check_id: int) -> dict:
        """Get specific check details"""
        return self._request("check/display", {"check_id": check_id})
    
    def get_all_checks(self, max_iterations: int = 60) -> list:
        """Get all checks across all pages (5,324+ records)"""
        return self._get_all_pages("check/list", max_iterations=max_iterations)
    
    def get_uncashed_checks(self, max_pages: int = 5) -> list:
        """Get checks that haven't been cleared (not cashed yet).

        Used by: Muhi's "which charities have cashed their checks" query

        Capped at max_pages (default 5 = 500 checks) to avoid tying up
        gunicorn workers. Full check list is 5750+ records.

        Returns:
            list of check dicts where cleared == 0 and voided == 0
        """
        all_checks = []
        offset = 0
        batch_size = 100

        for _ in range(max_pages):
            result = self._request("check/list", {
                "view_limit": batch_size,
                "view_offset": offset
            })
            if not result.get("success") or not result.get("data"):
                break
            page = result["data"].get("results", [])
            if not page:
                break
            all_checks.extend(page)
            if len(page) < batch_size:
                break
            offset += batch_size

        uncashed = [
            c for c in all_checks
            if c.get("cleared") == 0 and c.get("voided") == 0
            and not c.get("unused", 0)
        ]

        logger.info(f"Found {len(uncashed)} uncashed checks out of {len(all_checks)} fetched (capped at {max_pages} pages)")
        return uncashed
    
    # =========================================================================
    # VOUCHERS
    # =========================================================================
    
    def get_vouchers(self, limit: int = 100, offset: int = 0) -> dict:
        """Get vouchers list"""
        return self._request("voucher/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_voucher(self, voucher_id: int) -> dict:
        """Get specific voucher details"""
        return self._request("voucher/display", {"voucher_id": voucher_id})
    
    # =========================================================================
    # EVENTS
    # =========================================================================
    
    def get_event_dates(self, limit: int = 100) -> dict:
        """Get event dates list (campaigns)"""
        return self._request("event/list/dates", {"view_limit": limit})
    
    def get_event_date(self, event_date_id: int) -> dict:
        """Get specific event date details including attendees"""
        return self._request("event/display/eventdate", {"event_date_id": event_date_id})
    
    def get_event(self, event_id: int) -> dict:
        """Get specific event details"""
        return self._request("event/display", {"event_id": event_id})
    
    def create_event_date(self, event_id: int, **kwargs) -> dict:
        """Create a new event date.
        
        Args:
            event_id: Parent event ID (required)
            **kwargs: event_date, start_time, location, event_description, etc.
        """
        check_input_fields(kwargs, "event/create/eventdate")
        data = {"event_id": event_id, **kwargs}
        logger.info(f"Creating event date for event {event_id}")
        return self._request("event/create/eventdate", data)
    
    def edit_event_date(self, event_date_id: int, **kwargs) -> dict:
        """Edit an existing event date.
        
        Args:
            event_date_id: Event date ID (required)
            **kwargs: Fields to update
        """
        check_input_fields(kwargs, "event/edit/eventdate")
        data = {"event_date_id": event_date_id, **kwargs}
        return self._request("event/edit/eventdate", data)
    
    # =========================================================================
    # TASKS
    # =========================================================================
    
    def get_tasks(self, limit: int = 100) -> dict:
        """Get CSuite tasks list"""
        return self._request("task/list", {"view_limit": limit})
    
    def get_task(self, task_id: int) -> dict:
        """Get specific task details"""
        return self._request("task/display", {"task_id": task_id})
    
    def create_task(self, name: str, employee_id: int, due_date: str = None,
                    description: str = None, **kwargs) -> dict:
        """Create a task in CSuite.
        
        Args:
            name: Task name (required)
            employee_id: Assigned employee's name_link_id (required)
            due_date: Due date in YYYY-MM-DD format
            description: Task description
            **kwargs: Additional task fields
        """
        data = {"name": name, "employee_id": employee_id}
        if due_date:
            data["due_date"] = due_date
        if description:
            data["task_description"] = description
        # NOT gated by check_input_fields. This is task/create, outside the
        # profile/fund/event surface, and no task input name has been
        # confirmed by a read-back yet — gating it against
        # CONFIRMED_INPUT_FIELDS would refuse `name` and `employee_id` and
        # break the method. It carries the same silent-drop risk; confirming
        # task field names is its own sandbox task.
        data.update(kwargs)
        
        logger.info(f"Creating CSuite task: {name}")
        return self._request("task/create", data)
    
    def complete_task(self, task_id: int = None, task_guid: str = None) -> dict:
        """Mark a CSuite task as complete.
        
        Args:
            task_id: Task ID (use one or the other)
            task_guid: Task GUID (use one or the other)
        """
        data = {}
        if task_id:
            data["task_id"] = task_id
        if task_guid:
            data["task_guid"] = task_guid
        return self._request("task/edit/complete", data)
    
    # =========================================================================
    # ACCOUNTS
    # =========================================================================
    
    def get_accounts(self, limit: int = 100) -> dict:
        """Get accounts list"""
        return self._request("account/list", {"view_limit": limit})
    
    def get_investment_strategies(self) -> dict:
        """Get investment strategies (e.g., Saturna)"""
        return self._request("account/list/strategy")
    
    # =========================================================================
    # ACCOUNTS PAYABLE
    # =========================================================================
    
    def get_ap_summary(self) -> dict:
        """Get accounts payable summary by vendor.
        
        Returns AP and SP (scholarship payable) totals with aging buckets
        (30/60/90/91+ days).
        """
        return self._request("ap/list")
    
    def get_ap_open_vouchers(self) -> dict:
        """Get open vouchers that can be paid"""
        return self._request("ap/list/openvouchers")
    
    # =========================================================================
    # VENDORS & GRANTEES
    # =========================================================================
    
    def make_vendor(self, profile_id: int) -> dict:
        """Make a profile a vendor (required before creating vouchers for them)"""
        return self._request("vendor/create", {"profile_id": profile_id})
    
    def make_grantee(self, profile_id: int) -> dict:
        """Make a profile a grantee (required before creating grants for them)"""
        return self._request("grantee/create", {"profile_id": profile_id})
    
    # =========================================================================
    # GRANT TYPES & DISTRIBUTION TYPES
    # =========================================================================
    
    def get_grant_types(self) -> dict:
        """Get grant types (NTEE categories: Education, Human Services, etc.)"""
        return self._request("grant_type/list")
    
    def get_distribution_types(self) -> dict:
        """Get distribution types"""
        return self._request("distribution/list/type")
