CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT        NOT NULL,
    depends_on    TEXT[]      NOT NULL DEFAULT '{}',
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 3,
    leased_by     TEXT,
    lease_token   TEXT,
    lease_expiry  TIMESTAMPTZ,
    error         TEXT        NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

-- The lease path filters on status and orders by created_at; this index
-- covers exactly that query and keeps lease cost independent of how many
-- completed jobs are sitting in the table.
CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON jobs (created_at)
    WHERE status = 'pending';

-- The reaper scans for leases that have run out.
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry
    ON jobs (lease_expiry)
    WHERE status = 'leased';