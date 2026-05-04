-- PhrenForge remote-subagent blackboard schema (v1).
-- Designed so a future modules/remote_subagent/ can absorb this file in
-- place without migration.

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_version (version, applied_at)
VALUES (1, datetime('now'));
INSERT OR IGNORE INTO schema_version (version, applied_at)
VALUES (2, datetime('now'));

-- ----------------------------------------------------------------------
-- tasks: sealed task records dispatched from main agent to remote runner
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    todo_id         TEXT,                       -- optional human ref (e.g. "109-jobs")
    title           TEXT NOT NULL,
    prompt          TEXT NOT NULL,              -- sealed brief shipped to runner
    scope_globs     TEXT NOT NULL,              -- json array of glob strings
    base_commit     TEXT NOT NULL,              -- commit sha task is based on
    branch          TEXT NOT NULL,              -- e.g. agent/optiplex/109-jobs
    timeout_minutes INTEGER NOT NULL DEFAULT 60,
    priority        INTEGER NOT NULL DEFAULT 100,
    status          TEXT NOT NULL DEFAULT 'queued',
                    -- queued | claimed | running | reporting | done |
                    -- failed | cancelled | timed_out
    worker_id       TEXT,                       -- runner that claimed it
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    claimed_at      TEXT,
    started_at      TEXT,
    completed_at    TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    metadata        TEXT NOT NULL DEFAULT '{}'  -- json for forward-compat
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
    ON tasks (status, priority DESC, id ASC);
CREATE INDEX IF NOT EXISTS idx_tasks_branch ON tasks (branch);
CREATE INDEX IF NOT EXISTS idx_tasks_todo_id ON tasks (todo_id);

-- ----------------------------------------------------------------------
-- results: terminal report from the runner
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS results (
    task_id         INTEGER PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    status          TEXT NOT NULL,              -- mirrors final tasks.status
    branch          TEXT NOT NULL,
    head_commit     TEXT,
    commits_json    TEXT NOT NULL DEFAULT '[]',
    files_touched   TEXT NOT NULL DEFAULT '[]',
    test_summary    TEXT,                       -- short string e.g. "12 pass / 0 fail"
    log_tail        TEXT,                       -- last N lines of runner log
    error           TEXT,                       -- present iff status in (failed, timed_out)
    reported_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------
-- progress: streaming progress beats from runner -> blackboard -> dispatcher
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS progress (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    message         TEXT NOT NULL,
    files_touched   TEXT NOT NULL DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_progress_task_seq
    ON progress (task_id, seq);

-- ----------------------------------------------------------------------
-- notes: bidirectional free-form back-channel
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS notes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    author          TEXT NOT NULL,              -- 'dispatcher' | 'runner' | <freeform>
    body            TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_notes_task ON notes (task_id, id);

-- ----------------------------------------------------------------------
-- workers: heartbeat/registry for runners
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workers (
    worker_id       TEXT PRIMARY KEY,
    hostname        TEXT,
    capabilities    TEXT NOT NULL DEFAULT '{}', -- json
    first_seen      TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen       TEXT NOT NULL DEFAULT (datetime('now')),
    current_task_id INTEGER REFERENCES tasks(id) ON DELETE SET NULL
);

-- ----------------------------------------------------------------------
-- runners (v2): rich registry with capabilities, identity, scope affinity,
-- heartbeat-driven state machine, and authenticated registration.
--
-- A runner row is the durable identity (UUID + public_key). The legacy
-- ``workers`` table is still maintained for backward compat with the
-- ``claim_next_task`` code path; new code reads from ``runners``.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runners (
    runner_id        TEXT PRIMARY KEY,           -- UUID4 generated once per host
    public_key       TEXT NOT NULL,              -- ed25519 hex (32 bytes)
    hostname         TEXT NOT NULL,
    os               TEXT NOT NULL,              -- e.g. "Windows-10-10.0.19045"
    arch             TEXT NOT NULL,              -- e.g. "AMD64"
    cpu_model        TEXT,
    cpu_count        INTEGER,
    ram_mb           INTEGER,
    gpu              TEXT,                       -- short label or null
    tools            TEXT NOT NULL DEFAULT '[]', -- json array of tool names
    tags             TEXT NOT NULL DEFAULT '[]', -- json array of tag strings
    scope_prefixes   TEXT NOT NULL DEFAULT '[]', -- json array of allowed path prefixes
    tenant           TEXT,                       -- optional tenant/workspace key
    workspace_root   TEXT,                       -- absolute path on the runner
    runner_version   TEXT NOT NULL,              -- runner code version string
    protocol_version INTEGER NOT NULL,           -- handshake major version
    max_concurrent   INTEGER NOT NULL DEFAULT 1,
    state            TEXT NOT NULL DEFAULT 'online',
                     -- online | degraded | offline | draining
    drain_requested  INTEGER NOT NULL DEFAULT 0,
    cpu_load_pct     REAL,
    ram_free_mb      INTEGER,
    battery_pct      INTEGER,                    -- null on AC/desktop
    on_battery       INTEGER NOT NULL DEFAULT 0,
    last_known_commit TEXT,                      -- HEAD on the runner's clone
    metadata         TEXT NOT NULL DEFAULT '{}',
    first_seen       TEXT NOT NULL DEFAULT (datetime('now')),
    last_heartbeat   TEXT NOT NULL DEFAULT (datetime('now')),
    last_nonce       TEXT                        -- replay protection
);

CREATE INDEX IF NOT EXISTS idx_runners_state    ON runners (state);
CREATE INDEX IF NOT EXISTS idx_runners_tenant   ON runners (tenant);
CREATE INDEX IF NOT EXISTS idx_runners_hostname ON runners (hostname);

-- ----------------------------------------------------------------------
-- task_streams (v2): structured stdout/stderr lines from runner -> hub.
-- Separate from ``progress`` so high-volume process output doesn't drown
-- the human-readable progress beat stream.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS task_streams (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    channel         TEXT NOT NULL,              -- 'stdout' | 'stderr' | 'info'
    line            TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_task_streams_task_seq
    ON task_streams (task_id, seq);

-- ----------------------------------------------------------------------
-- v2 task columns: capability requirements + tenant routing.
-- These ALTERs are idempotent via Python-side migration in server.py
-- (sqlite has no IF NOT EXISTS for ALTER TABLE ADD COLUMN before 3.35).
-- ----------------------------------------------------------------------
-- (tasks.required_tools, tasks.required_tags, tasks.tenant, tasks.workspace_root,
--  tasks.required_base_commit_present added at runtime)
