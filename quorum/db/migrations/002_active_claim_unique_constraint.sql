-- Migration 002: Active Claim Unique Constraint (Defense-in-Depth)
-- Enforces at most one active claim per task at the database level.
-- The invariant is: at most one active claim per task.
-- Note: This partial unique index serves as defense-in-depth behind
-- row-level serialization via SELECT ... FOR UPDATE.

CREATE UNIQUE INDEX IF NOT EXISTS uq_claims_active_task
ON claims(task_id)
WHERE status = 'active';
