"""
quorum/db/repositories/__init__.py
------------------------------------
Authoritative persistence repository layer.
"""

from db.repositories.run_repo import RunRepository
from db.repositories.task_repo import TaskRepository
from db.repositories.claim_repo import ClaimRepository
from db.repositories.worker_repo import WorkerRepository
from db.repositories.finding_repo import FindingRepository
from db.repositories.idempotency_repo import IdempotencyRepository
from db.repositories.audit_repo import AuditRepository

run_repo = RunRepository()
task_repo = TaskRepository()
claim_repo = ClaimRepository()
worker_repo = WorkerRepository()
finding_repo = FindingRepository()
idempotency_repo = IdempotencyRepository()
audit_repo = AuditRepository()

__all__ = [
    "RunRepository",
    "TaskRepository",
    "ClaimRepository",
    "WorkerRepository",
    "FindingRepository",
    "IdempotencyRepository",
    "AuditRepository",
    "run_repo",
    "task_repo",
    "claim_repo",
    "worker_repo",
    "finding_repo",
    "idempotency_repo",
    "audit_repo",
]
