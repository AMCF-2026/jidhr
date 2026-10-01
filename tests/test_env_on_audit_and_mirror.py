"""The audit records which environment a write ran under, and the mirror
refuses to run outside production.

Both exist because of 2026-10-01, when a HubSpot PATCH wrote sandbox CSuite
profile 21663 onto a live HubSpot contact. Answering "did any HubSpot write ever
run outside production?" afterwards meant reconstructing it from payload_meta id
values and from memory of who ran what. It happened to be answerable.

`csuite_mirror` is the same shape of exposure, untriggered: one table, no
environment column, production reports reading it.

No network, no database.
"""

import pytest

import clients.audit as audit

# Patched by dotted path, not through an imported class: a reloaded `config`
# rebinds `config.Config`, so a class captured at import time can be the stale
# one. See clients.audit.current_csuite_env.


@pytest.fixture(autouse=True)
def forget_the_column_probe():
    """The probe caches per process; each test states its own world."""
    audit._HAS_CSUITE_ENV = None
    yield
    audit._HAS_CSUITE_ENV = None


# ---------------------------------------------------------------------------
# STEP 1 — csuite_env on the audit row
# ---------------------------------------------------------------------------

def test_the_migration_exists_and_is_additive_and_reversible():
    import pathlib

    sql = pathlib.Path("migrations/002_write_audit_csuite_env.sql").read_text()
    assert "ADD COLUMN IF NOT EXISTS csuite_env text" in sql
    assert "DROP COLUMN csuite_env" in sql, "reversible"
    assert "NOT NULL" not in sql, "existing rows must keep NULL"
    assert "UPDATE write_audit" not in sql, "no back-fill of a guess"


def test_the_env_is_read_from_config(monkeypatch):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")
    assert audit.current_csuite_env() == "sandbox"
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    assert audit.current_csuite_env() == "live"


def test_an_unset_env_is_none_not_a_guess(monkeypatch):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "")
    assert audit.current_csuite_env() is None


def test_with_the_column_the_env_is_appended_to_the_row(monkeypatch):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")
    audit._HAS_CSUITE_ENV = True

    sql, with_env = audit._insert_sql()
    assert with_env is True
    assert "csuite_env" in sql
    assert sql.count("%s") == 15, "fourteen base columns plus the env"


def test_without_the_column_the_old_insert_is_used():
    audit._HAS_CSUITE_ENV = False

    sql, with_env = audit._insert_sql()
    assert with_env is False
    assert "csuite_env" not in sql
    assert sql.count("%s") == 14


def test_the_reserve_variant_still_returns_the_id():
    audit._HAS_CSUITE_ENV = True
    sql, _ = audit._insert_sql(reserve=True)
    assert sql.strip().endswith("RETURNING id")
    assert "csuite_env" in sql


def test_the_probe_is_asked_once_and_remembered():
    asked = []

    def query(sql, params=None):
        asked.append(sql)
        return [{"?column?": 1}]

    assert audit._csuite_env_column_exists(query=query) is True
    assert audit._csuite_env_column_exists(query=query) is True
    assert len(asked) == 1, "one probe per process, not per write"
    assert "information_schema.columns" in asked[0]


def test_a_missing_column_is_detected_and_names_the_migration(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="clients.audit"):
        assert audit._csuite_env_column_exists(
            query=lambda sql, params=None: []) is False
    assert any("002_write_audit_csuite_env.sql" in r.getMessage()
               for r in caplog.records)


def test_a_failed_probe_assumes_ABSENT_rather_than_blocking_writes():
    """reserve_write REFUSES the write when it cannot record a row, so writing
    a column the table might not have would stop every CSuite and HubSpot
    write. Fail-closed is right for a missing audit and wrong for a column
    ordering mistake."""
    def broken(sql, params=None):
        raise RuntimeError("information_schema unavailable")

    assert audit._csuite_env_column_exists(query=broken) is False


def test_the_column_is_the_last_value_so_the_base_row_is_untouched(monkeypatch):
    """A new column at the end cannot shift an existing one."""
    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")
    audit._HAS_CSUITE_ENV = True
    sql, _ = audit._insert_sql()
    columns = sql.split("(", 1)[1].split(")", 1)[0]
    assert columns.strip().endswith("csuite_env")


# ---------------------------------------------------------------------------
# STEP 2 — the mirror refuses outside production
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("env", ["sandbox", "SANDBOX", " sandbox ", "",
                                 "staging", None])
def test_the_mirror_refuses_anything_but_live(monkeypatch, env):
    from sync.mirror import (MirrorEnvironmentRefused,
                             assert_mirror_environment, mirror_writes_allowed)

    monkeypatch.setattr("config.Config.CSUITE_ENV", env)
    assert mirror_writes_allowed() is False
    with pytest.raises(MirrorEnvironmentRefused) as caught:
        assert_mirror_environment()
    assert "no environment column" in str(caught.value)
    assert "Nothing was written" in str(caught.value)


def test_the_mirror_allows_live(monkeypatch):
    from sync.mirror import assert_mirror_environment, mirror_writes_allowed

    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    assert mirror_writes_allowed() is True
    assert_mirror_environment()          # no raise


def test_the_writer_itself_refuses(monkeypatch):
    """_upsert is the seam every path that writes rows funnels through, so the
    check there holds even for a caller that bypasses refresh()."""
    from sync.mirror import MirrorEnvironmentRefused, _upsert

    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")
    with pytest.raises(MirrorEnvironmentRefused):
        _upsert("profile", [{"profile_id": 1}], 1, None)


def test_refresh_refuses_before_spending_a_csuite_call(monkeypatch):
    """A refused run must cost nothing. The check is before any fetch."""
    from sync.mirror import MirrorEnvironmentRefused, refresh

    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")

    class Exploding:
        def __getattr__(self, name):
            raise AssertionError(f"CSuite was called: {name}")

    with pytest.raises(MirrorEnvironmentRefused):
        refresh(record_types=["profile"], client=Exploding())


def test_a_dry_run_is_allowed_in_sandbox(monkeypatch):
    """It writes nothing, so there is nothing to protect — and being able to
    see what a refresh WOULD do from a sandbox is useful."""
    from sync.mirror import refresh

    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")
    # It will fail for want of a client, but NOT with the environment refusal.
    from sync.mirror import MirrorEnvironmentRefused
    try:
        refresh(record_types=["profile"], dry_run=True, client=None)
    except MirrorEnvironmentRefused:
        pytest.fail("a dry run must not be refused on environment grounds")
    except Exception:
        pass


def test_it_is_the_same_rule_as_the_hubspot_seam(monkeypatch):
    """One store, two source environments — the same shape, so the same rule,
    and neither is a feature flag."""
    from clients.hubspot import hubspot_writes_allowed
    from sync.mirror import mirror_writes_allowed

    for env in ("live", "sandbox", "", "staging"):
        monkeypatch.setattr("config.Config.CSUITE_ENV", env)
        assert mirror_writes_allowed() == hubspot_writes_allowed(), env
