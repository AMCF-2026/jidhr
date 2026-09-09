"""Step 1b: users table, auth wiring, and startup/cookie hardening.

No database and no network. `clients.users.execute_query` is mocked in every
test that touches it, so these assert on the SQL and parameters we send
rather than on a live schema.

A note on "unset" environment variables: config.py calls `load_dotenv()` at
import, which repopulates anything missing from the repo's .env. Deleting a
variable and reloading would therefore silently get it back. Setting it to
the empty string is what actually reads as unset here — python-dotenv will
not override a key already present in os.environ — and empty is falsy, which
is exactly the condition the production code checks.
"""

import importlib
import sys
from unittest.mock import patch

import pytest

# Modules these tests reload. Restored afterwards so ordering cannot leak.
_RELOADED = ("app", "auth", "assistant", "config", "clients.users")


@pytest.fixture
def fresh_modules():
    """Snapshot and restore the modules a reload would clobber."""
    saved = {name: sys.modules.get(name) for name in _RELOADED}
    yield
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def _import_app(monkeypatch, secret_key, database_url=""):
    """Import app.py fresh under a given environment."""
    monkeypatch.setenv("SECRET_KEY", secret_key)
    monkeypatch.setenv("DATABASE_URL", database_url)
    for name in _RELOADED:
        sys.modules.pop(name, None)
    return importlib.import_module("app")


# ---------------------------------------------------------------------------
# a. get_or_create_user lowercases and issues the ON CONFLICT upsert
# ---------------------------------------------------------------------------

def _upsert_row(**overrides):
    row = {
        "id": 42,
        "email": "carl@amuslimcf.org",
        "display_name": "Carl",
        "role": "staff",
        "csuite_profile_id": None,
        "hubspot_contact_id": None,
        "is_active": True,
        "last_login_at": "2026-09-09T11:00:00+00:00",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-09-09T11:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_get_or_create_user_lowercases_the_email_before_querying():
    from clients import users

    with patch.object(users, "execute_query", return_value=[_upsert_row()]) as q:
        users.get_or_create_user("Carl@AMuslimCF.org", "Carl")

    _sql, params = q.call_args[0]
    assert params[0] == "carl@amuslimcf.org"


def test_get_or_create_user_strips_surrounding_whitespace():
    from clients import users

    with patch.object(users, "execute_query", return_value=[_upsert_row()]) as q:
        users.get_or_create_user("  Carl@AMuslimCF.org  ")

    assert q.call_args[0][1][0] == "carl@amuslimcf.org"


def test_get_or_create_user_uses_the_on_conflict_upsert():
    from clients import users

    with patch.object(users, "execute_query", return_value=[_upsert_row()]) as q:
        users.get_or_create_user("carl@amuslimcf.org", "Carl")

    sql = " ".join(q.call_args[0][0].split()).lower()
    assert "insert into users" in sql
    assert "on conflict (email) do update" in sql
    assert "last_login_at = now()" in sql
    # COALESCE so a nameless Google profile cannot blank a stored display_name.
    assert "coalesce(excluded.display_name, users.display_name)" in sql
    assert "returning *" in sql


def test_get_or_create_user_returns_the_row_including_the_access_flags():
    from clients import users

    row = _upsert_row(role="admin", is_active=False)
    with patch.object(users, "execute_query", return_value=[row]):
        result = users.get_or_create_user("carl@amuslimcf.org")

    # The client reports; it does not decide. Both flags must reach the caller.
    assert result["role"] == "admin"
    assert result["is_active"] is False


def test_get_or_create_user_rejects_an_empty_address():
    from clients import users

    with patch.object(users, "execute_query") as q:
        with pytest.raises(ValueError):
            users.get_or_create_user("   ")
    q.assert_not_called()


def test_get_user_by_email_lowercases_too():
    from clients import users

    with patch.object(users, "execute_query", return_value=[]) as q:
        assert users.get_user_by_email("Carl@AMuslimCF.org") is None

    assert q.call_args[0][1][0] == "carl@amuslimcf.org"


def test_get_user_by_id_coerces_the_id_and_returns_none_when_absent():
    from clients import users

    with patch.object(users, "execute_query", return_value=[]) as q:
        assert users.get_user_by_id("42") is None

    assert q.call_args[0][1] == (42,)


# ---------------------------------------------------------------------------
# b. An inactive user is rejected at login
# ---------------------------------------------------------------------------

def test_inactive_user_is_rejected_at_the_oauth_callback(monkeypatch,
                                                         fresh_modules):
    app_module = _import_app(monkeypatch, "test-secret-key")
    import auth

    class _StubGoogle:
        def authorize_access_token(self):
            return {"userinfo": {"email": "gone@amuslimcf.org",
                                 "name": "Former Staffer"}}

    monkeypatch.setattr(auth.oauth, "google", _StubGoogle(), raising=False)
    monkeypatch.setattr(auth, "get_or_create_user",
                        lambda email, display_name=None: _upsert_row(
                            email="gone@amuslimcf.org", is_active=False))

    client = app_module.app.test_client()
    response = client.get("/auth/callback")

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]
    assert "deactivated" in response.headers["Location"].lower()

    # And the rejection is real: the home page still bounces to login.
    assert "/login" in client.get("/").headers.get("Location", "")


def test_active_user_is_accepted_at_the_oauth_callback(monkeypatch,
                                                       fresh_modules):
    app_module = _import_app(monkeypatch, "test-secret-key")
    import auth

    class _StubGoogle:
        def authorize_access_token(self):
            return {"userinfo": {"email": "carl@amuslimcf.org",
                                 "name": "Carl"}}

    monkeypatch.setattr(auth.oauth, "google", _StubGoogle(), raising=False)
    monkeypatch.setattr(auth, "get_or_create_user",
                        lambda email, display_name=None: _upsert_row())
    monkeypatch.setattr(auth, "get_user_by_id",
                        lambda user_id: _upsert_row())

    client = app_module.app.test_client()
    response = client.get("/auth/callback")

    assert response.status_code == 302
    assert "error" not in response.headers["Location"]


def test_wrong_domain_is_still_rejected_before_any_database_call(monkeypatch,
                                                                fresh_modules):
    app_module = _import_app(monkeypatch, "test-secret-key")
    import auth

    class _StubGoogle:
        def authorize_access_token(self):
            return {"userinfo": {"email": "someone@gmail.com", "name": "X"}}

    called = []
    monkeypatch.setattr(auth.oauth, "google", _StubGoogle(), raising=False)
    monkeypatch.setattr(auth, "get_or_create_user",
                        lambda *a, **k: called.append(1))

    response = app_module.app.test_client().get("/auth/callback")

    assert response.status_code == 302
    assert "restricted" in response.headers["Location"].lower()
    assert called == [], "domain check must run before the user upsert"


def test_deactivated_user_cannot_be_loaded_from_a_session_cookie(monkeypatch,
                                                                 fresh_modules):
    _import_app(monkeypatch, "test-secret-key")
    import auth

    monkeypatch.setattr(auth, "get_user_by_id",
                        lambda user_id: _upsert_row(is_active=False))
    assert auth.load_user("42") is None


def test_user_loader_fails_closed_when_the_database_is_unreachable(monkeypatch,
                                                                   fresh_modules):
    _import_app(monkeypatch, "test-secret-key")
    import auth

    def _boom(user_id):
        raise RuntimeError("DATABASE_URL environment variable is not set.")

    monkeypatch.setattr(auth, "get_user_by_id", _boom)
    assert auth.load_user("42") is None


def test_user_object_carries_the_id_email_display_name_and_role(monkeypatch,
                                                                fresh_modules):
    _import_app(monkeypatch, "test-secret-key")
    import auth

    user = auth.User.from_row(_upsert_row(role="admin"))
    assert user.id == 42 and isinstance(user.id, int)
    assert user.email == "carl@amuslimcf.org"
    assert user.display_name == "Carl"
    assert user.role == "admin"
    # Flask-Login serialises the id as a string; it must round-trip.
    assert user.get_id() == "42"


def test_auth_no_longer_keeps_an_in_memory_user_store(monkeypatch,
                                                      fresh_modules):
    _import_app(monkeypatch, "test-secret-key")
    import auth

    assert not hasattr(auth, "_users"), (
        "the in-memory user dict was replaced by the users table")


# ---------------------------------------------------------------------------
# c/d. Startup refuses to run without SECRET_KEY, and needs no database
# ---------------------------------------------------------------------------

def test_app_import_raises_runtime_error_when_secret_key_is_unset(monkeypatch,
                                                                  fresh_modules):
    with pytest.raises(RuntimeError) as excinfo:
        _import_app(monkeypatch, "")

    assert "SECRET_KEY" in str(excinfo.value)


def test_app_import_succeeds_with_a_secret_key_and_no_database_url(monkeypatch,
                                                                   fresh_modules):
    app_module = _import_app(monkeypatch, "dummy-secret", database_url="")

    assert app_module.app is not None
    assert app_module.app.secret_key == "dummy-secret"

    from clients.database import is_configured
    assert is_configured() is False


def test_config_no_longer_ships_a_development_secret_key(monkeypatch,
                                                         fresh_modules):
    monkeypatch.setenv("SECRET_KEY", "")
    sys.modules.pop("config", None)
    config = importlib.import_module("config")

    assert config.Config.SECRET_KEY in (None, "")
    assert "SECRET_KEY" in config.Config.validate()


# ---------------------------------------------------------------------------
# e. Session cookie hardening
# ---------------------------------------------------------------------------

def test_session_cookies_are_secure_httponly_and_samesite(monkeypatch,
                                                          fresh_modules):
    monkeypatch.setenv("DEBUG", "False")
    app_module = _import_app(monkeypatch, "test-secret-key")
    cfg = app_module.app.config

    assert cfg["SESSION_COOKIE_SECURE"] is True
    assert cfg["SESSION_COOKIE_HTTPONLY"] is True
    assert cfg["SESSION_COOKIE_SAMESITE"] == "Lax"
    assert cfg["REMEMBER_COOKIE_SECURE"] is True
    assert cfg["REMEMBER_COOKIE_HTTPONLY"] is True


def test_session_lifetime_is_twelve_hours(monkeypatch, fresh_modules):
    from datetime import timedelta

    monkeypatch.setenv("DEBUG", "False")
    app_module = _import_app(monkeypatch, "test-secret-key")

    assert app_module.app.config["PERMANENT_SESSION_LIFETIME"] == timedelta(hours=12)


def test_secure_cookies_relax_under_debug_so_local_http_login_works(monkeypatch,
                                                                    fresh_modules):
    # A Secure cookie is never sent over plain http, so leaving this on in
    # local debug would look like "login does nothing" rather than an error.
    monkeypatch.setenv("DEBUG", "True")
    app_module = _import_app(monkeypatch, "test-secret-key")
    cfg = app_module.app.config

    assert cfg["SESSION_COOKIE_SECURE"] is False
    assert cfg["SESSION_COOKIE_HTTPONLY"] is True
    assert cfg["SESSION_COOKIE_SAMESITE"] == "Lax"


def test_login_does_not_issue_a_remember_me_cookie():
    """A remember-me cookie would outlive the session cookie by weeks.

    The banned literal is assembled at runtime rather than written out, so a
    repo-wide grep for it stays clean. A test that spells out the string it
    forbids would trip the very check it exists to support.
    """
    import pathlib

    forbidden = "remember=" + "True"
    source = (pathlib.Path(__file__).resolve().parent.parent / "auth.py").read_text()
    assert "remember=False" in source
    assert forbidden not in source
