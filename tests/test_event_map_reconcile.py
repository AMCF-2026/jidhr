"""V5.96: reconcile pending event_map rows HubSpot already has. Map-only.

Every CSuite row and HubSpot record here is a TEST DOUBLE; no network, no
database (execute_query is captured).
"""

import pytest

from intents import sync_commands
from sync import event_apply as ea
from sync import event_hubspot as eh
from sync import event_map_reconcile as recon
from sync.event_apply import DEFAULT_ORGANIZER


def row(eid, day, title):
    return {"event_date_id": eid, "event_date": day, "start_time": "4:00 pm EST",
            "archived": 1, "event_name": "Event - Other",
            "event_description": title, "location": "Zoom"}


ROWS = [row("1155", "2026-04-10", "Film Screening"),
        row("1157", "2026-07-21", "Sign up for updates"),
        row("1159", "2026-04-11", "Spring Lunch"),
        row("1200", "2026-05-01", "Not in HubSpot")]


def hs_record(r, object_id, **change):
    payload = dict(eh.map_event_date(r, DEFAULT_ORGANIZER).payload)
    payload.update(change)
    return dict(payload, objectId=object_id,
                externalEventId=eh.external_id(r["event_date_id"]))


MAP = {"1155": {"status": "pending", "hubspot_event_id": None},
       "1157": {"status": "pending", "hubspot_event_id": None},
       "1159": {"status": "pending", "hubspot_event_id": None},
       "1200": {"status": "pending", "hubspot_event_id": None},
       "1300": {"status": "synced", "hubspot_event_id": "999"}}
INDEX = {"csuite-1155": hs_record(ROWS[0], "801"),
         "csuite-1157": hs_record(ROWS[1], "802"),
         "csuite-1159": hs_record(ROWS[2], "803", eventName="Renamed in HubSpot")}


class Fetched:
    def __init__(self, rows):
        self.rows, self.calls, self.error, self.complete = rows, 1, None, True


@pytest.fixture
def world(monkeypatch):
    sql = []

    def execute(query, params=None, fetch=True):
        sql.append((" ".join(query.split()), params))
        if "INSERT INTO hubsync.run_log" in query:
            return [{"id": 140}]
        if "UPDATE hubsync.event_map" in query:
            return [{"csuite_eventdate_id": params[2]}]
        return []

    monkeypatch.setattr(ea, "migration_applied", lambda: True)
    monkeypatch.setattr(ea, "load_map", lambda: {k: dict(v) for k, v in
                                                 MAP.items()})
    monkeypatch.setattr(eh, "fetch_event_dates", lambda csuite: Fetched(ROWS))
    monkeypatch.setattr(eh, "hubspot_index", lambda hub: (dict(INDEX), 1, None))
    monkeypatch.setattr("clients.database.execute_query", execute)
    monkeypatch.setattr(recon.database, "execute_query", execute)
    return sql


class NoWrites:
    def __getattr__(self, name):
        raise AssertionError(f"reconcile called {name}()")


def go(apply=False):
    return recon.run(apply=apply, hubspot=NoWrites(), csuite=NoWrites())


def test_the_preview_sorts_each_row_and_writes_nothing(world):
    out = go()
    assert [e["id"] for e in out["match"]] == ["1155", "1157"]
    assert [(e["id"], e["differs"]) for e in out["differ"]] == \
        [("1159", ["name"])]
    assert [e["id"] for e in out["not_in_hubspot"]] == ["1200"]
    assert not any("UPDATE" in q or "INSERT" in q for q, _ in world), \
        "a preview writes nothing, not even a run_log row"
    text = recon.report(out)
    assert "PREVIEW" in text and "csuite-1155 → HubSpot 801" in text
    assert "differs in name" in text


def test_a_synced_row_is_never_considered(world):
    out = go()
    every = [e["id"] for k in ("match", "differ", "not_in_hubspot",
                               "not_in_csuite") for e in out[k]]
    assert "1300" not in every


def test_apply_writes_only_the_map_with_the_right_hash(world):
    out = go(apply=True)
    updates = [p for q, p in world if q.startswith("UPDATE hubsync.event_map")]
    hashes = {p[2]: p[1] for p in updates}
    ids = {p[2]: p[0] for p in updates}
    assert ids == {"1155": "801", "1157": "802", "1159": "803"}
    assert hashes["1155"] == eh.map_event_date(ROWS[0], DEFAULT_ORGANIZER) \
        .content_hash
    assert hashes["1159"] is None, "a difference leaves the fingerprint empty"
    assert out["written"] == 3


def test_the_update_only_lands_on_a_still_pending_unlinked_row(world):
    go(apply=True)
    [sql] = {q for q, _ in world if q.startswith("UPDATE hubsync.event_map")}
    assert "status = 'pending'" in sql and "hubspot_event_id IS NULL" in sql


def test_apply_opens_and_closes_its_own_run_log(world):
    go(apply=True)
    assert any("VALUES ('event_map_reconcile'" in q for q, _ in world)
    closes = [p for q, p in world if q.startswith("UPDATE hubsync.run_log")]
    assert closes and closes[-1][0] == "complete" and closes[-1][-1] == 140


def test_a_failed_update_stops_and_is_not_retried(world, monkeypatch):
    seen = []

    def execute(query, params=None, fetch=True):
        if "UPDATE hubsync.event_map" in query:
            seen.append(params[2])
            raise RuntimeError("down")
        return [{"id": 141}] if "INSERT" in query else []

    monkeypatch.setattr(recon.database, "execute_query", execute)
    out = go(apply=True)
    assert seen == ["1155"] and out["written"] == 0
    assert "stopped" in out["stopped"]


def test_it_refuses_without_the_map(world, monkeypatch):
    monkeypatch.setattr(ea, "migration_applied", lambda: False)
    with pytest.raises(recon.Refused):
        go()


def test_a_start_time_difference_counts():
    mapped = eh.map_event_date(ROWS[0], DEFAULT_ORGANIZER)
    same = dict(mapped.payload)
    assert recon.differences(mapped, same) == []
    moved = dict(same, startDateTime="2026-04-10T22:00:00Z")
    assert recon.differences(mapped, moved) == ["start time"]


def test_the_chat_routes_it():
    assert sync_commands.can_handle("reconcile event map")
    assert sync_commands.can_handle("reconcile event map apply")
