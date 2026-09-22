"""
quorum/db/migrations/runner.py
--------------------------------
Deterministic, repeatable migration runner for Quorum PostgreSQL database.
Applies versioned SQL migrations in order within transactions and tracks
applied versions in the `schema_migrations` table.
Uses PostgreSQL advisory locks to guarantee concurrency safety across independent processes.
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent
# Application-wide 64-bit advisory lock key for Quorum database schema migrations
MIGRATION_ADVISORY_LOCK_KEY = 82749182


async def apply_migrations(conn: asyncpg.Connection, migrations_dir: Optional[Path] = None) -> list[int]:
    """Apply any pending SQL migrations in ascending numerical order.

    Acquires a PostgreSQL session-level advisory lock to guarantee that
    multiple concurrent worker processes cannot interleave migration runs.

    Parameters
    ----------
    conn:
        An open asyncpg Connection.
    migrations_dir:
        Path to migrations directory. Defaults to db/migrations.

    Returns
    -------
    list[int]
        List of newly applied migration version numbers.
    """
    directory = migrations_dir or MIGRATIONS_DIR

    # 1. Acquire advisory lock to serialize concurrent process migrations
    await conn.execute("SELECT pg_advisory_lock($1);", MIGRATION_ADVISORY_LOCK_KEY)
    try:
        # 2. Ensure schema_migrations table exists
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # 3. Fetch already applied versions
        rows = await conn.fetch("SELECT version FROM schema_migrations ORDER BY version ASC;")
        applied_versions = {r["version"] for r in rows}

        # 4. Discover migration files
        migration_files = []
        for f in directory.glob("*.sql"):
            match = re.match(r"^(\d+)_(.+)\.sql$", f.name)
            if match:
                version = int(match.group(1))
                name = match.group(2)
                migration_files.append((version, name, f))

        migration_files.sort(key=lambda x: x[0])

        newly_applied = []
        for version, name, filepath in migration_files:
            if version in applied_versions:
                continue

            logger.info(f"Applying migration {version:03d}_{name}...")
            sql_content = filepath.read_text(encoding="utf-8")

            async with conn.transaction():
                await conn.execute(sql_content)
                await conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, NOW())",
                    version,
                    name,
                )

            newly_applied.append(version)
            logger.info(f"Migration {version:03d}_{name} applied successfully.")

        return newly_applied

    finally:
        # Release advisory lock
        await conn.execute("SELECT pg_advisory_unlock($1);", MIGRATION_ADVISORY_LOCK_KEY)
