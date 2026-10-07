-- ---------------------------------------------------------------------------
-- 005_registration_map.sql
-- What the registrations sync has asserted to HubSpot, per event per person.
-- ---------------------------------------------------------------------------
-- NOT EXECUTED by anything in this repository. Run it yourself in
-- TablePlus. Until it is run, the registrations sync previews and refuses
-- to write, naming this file.
--
-- Why it exists
-- -------------
-- HubSpot cannot be asked cheaply "did I already register this person for
-- this event?". marketing-events/participations/{objectId}/breakdown reads,
-- but it returns participation as HUBSPOT computed it, not as this sync
-- asserted it — and the gap between those two is the thing a run needs to
-- reason about. Without this table every run either re-asserts every row or
-- guesses. The same argument as hubsync.event_map, whose UNIQUE constraint
-- is what stops a second create.
--
-- Why the email is stored normalised, and why there is also a hash
-- ---------------------------------------------------------------
-- CSuite stores primary_email as it was typed, and matching in HubSpot is by
-- exact address, so the join key has to be normalised the same way on both
-- sides — see sync.readback.normalise_email, and the DAF duplicate guard
-- that created a second profile for one donor because it was not.
--
-- email_sha1 exists so a RUN LOG can record which people a preview was
-- about without the log becoming a second copy of the contact database.
-- clients/audit.payload_meta makes the same distinction: ids are the point
-- of an audit trail, values are not.
--
-- Cancellation is INFERRED, which is why last_seen_at exists
-- ---------------------------------------------------------
-- CSuite has no cancelled state. A cancellation is a registrant row that
-- stops being returned, which is indistinguishable from a read that failed
-- or came back short. So a row that disappears is not cancelled here; it
-- stops being refreshed, and the run decides — see the guard in
-- sync/registrations.py, which refuses to infer anything from an event whose
-- registrant count has collapsed.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hubsync.registration_map (
    id                      BIGSERIAL PRIMARY KEY,

    -- The CSuite side.
    csuite_eventdate_id     TEXT NOT NULL,
    csuite_profile_id       TEXT,

    -- The join key: lower-cased and trimmed, the same way on both sides.
    contact_email           TEXT NOT NULL,
    -- sha1 of contact_email, for run logs that must not hold addresses.
    email_sha1              TEXT,

    -- The HubSpot side. NULL until the contact is known, which for a
    -- registrant with no contact yet is the normal state.
    external_event_id       TEXT,
    hubspot_contact_id      TEXT,

    -- What this sync last asserted, and what CSuite last said.
    last_state              TEXT
        CHECK (last_state IN ('REGISTERED', 'ATTENDED', 'CANCELLED')),
    last_state_at           TIMESTAMPTZ,
    csuite_rsvp             TEXT,
    csuite_attended         TEXT,

    --   synced   HubSpot was told, and agreed
    --   pending  planned, nothing sent yet
    --   withheld deliberately not sent; why is in last_error
    --   unknown  a write came back ambiguous. NEVER retried.
    --   review   a person needs to look
    --   error    the last write failed for a reason that is safe to retry
    status                  TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('synced', 'pending', 'withheld', 'unknown',
                          'review', 'error')),
    last_error              TEXT,

    -- The write this row belongs to, so a row and an audit line can be
    -- reconciled without a timestamp search.
    write_audit_id          BIGINT,

    -- When CSuite last returned this person for this event. A row whose
    -- last_seen_at stops advancing is a cancellation CANDIDATE and nothing
    -- more.
    last_seen_at            TIMESTAMPTZ,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- One row per person per event. This is the duplicate guard.
    UNIQUE (csuite_eventdate_id, contact_email)
);

-- The three questions a run asks.
CREATE INDEX IF NOT EXISTS registration_map_event_idx
    ON hubsync.registration_map (csuite_eventdate_id);
CREATE INDEX IF NOT EXISTS registration_map_status_idx
    ON hubsync.registration_map (status);
CREATE INDEX IF NOT EXISTS registration_map_seen_idx
    ON hubsync.registration_map (csuite_eventdate_id, last_seen_at);
