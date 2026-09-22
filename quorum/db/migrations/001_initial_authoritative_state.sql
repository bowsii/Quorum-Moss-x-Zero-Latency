-- Migration 001: Initial Authoritative Transactional State
-- Establishes PostgreSQL as the single source of truth for Stage 2 coordination entities:
-- runs, tasks, claims, workers, findings metadata, idempotency records, and audit events.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 1. Runs table (Stage 2 authoritative coordination representation)
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    question TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('created', 'running', 'completed', 'failed', 'cancelled')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Tasks table
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_key TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'retrying', 'completed', 'failed', 'cancelled', 'dead_lettered')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tasks_run_task_key UNIQUE (run_id, task_key)
);

-- 3. Workers table
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
    worker_type TEXT NOT NULL DEFAULT 'agent',
    status TEXT NOT NULL CHECK (status IN ('starting', 'ready', 'busy', 'draining', 'stopped', 'failed')),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Claims table
CREATE TABLE IF NOT EXISTS claims (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL,
    owner_type TEXT NOT NULL CHECK (owner_type IN ('agent', 'human', 'adjudicator', 'reaper')),
    status TEXT NOT NULL CHECK (status IN ('active', 'superseded', 'expired', 'released', 'completed')),
    lease_id TEXT NOT NULL,
    lease_version INTEGER NOT NULL DEFAULT 1 CHECK (lease_version >= 1),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Findings metadata table (authoritative metadata; semantic vectors remain in Chroma)
CREATE TABLE IF NOT EXISTS findings (
    id TEXT PRIMARY KEY,
    task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    claim_id TEXT REFERENCES claims(id) ON DELETE SET NULL,
    participant_id TEXT NOT NULL,
    participant_type TEXT NOT NULL CHECK (participant_type IN ('agent', 'human', 'adjudicator', 'reaper')),
    title TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    sources JSONB NOT NULL DEFAULT '[]'::jsonb,
    supersedes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6. Idempotency records table
CREATE TABLE IF NOT EXISTS idempotency_records (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    scope TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('in_progress', 'completed', 'failed')),
    payload JSONB,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_idempotency_key_scope UNIQUE (idempotency_key, scope)
);

-- 7. Audit events table (append-only)
CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for high-frequency access patterns
CREATE INDEX IF NOT EXISTS idx_tasks_run_key ON tasks(run_id, task_key);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_claims_task_id ON claims(task_id);
CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_active_expires ON claims(status, expires_at);
CREATE INDEX IF NOT EXISTS idx_claims_run_id ON claims(run_id);
CREATE INDEX IF NOT EXISTS idx_workers_run_id ON workers(run_id);
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers(status, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_findings_run_id ON findings(run_id);
CREATE INDEX IF NOT EXISTS idx_idempotency_lookup ON idempotency_records(idempotency_key, scope);
CREATE INDEX IF NOT EXISTS idx_audit_events_run_id ON audit_events(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_events_entity ON audit_events(entity_type, entity_id);
