"""Quorum FastAPI Application — main entry point.

Single-process: agents, Reaper, Adjudicator, and Moss (ChromaDB in-process)
all run as asyncio coroutines in this process. No internal HTTP hops.
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.middleware.rate_limit import RateLimitMiddleware
from api.middleware.security_headers import SecurityHeadersMiddleware
from api.routes import runs, bus
from db.models import init_db
from db.connection import init_pool, close_pool, get_connection
from db.migrations.runner import apply_migrations
from observability.otel_setup import setup_otel
from config.settings import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup:
    1. Init PostgreSQL pool
    2. Apply PostgreSQL migrations
    3. Initialize required legacy SQLite state
    4. Start background workers

    If PostgreSQL initialization or migrations fail:
    APPLICATION STARTUP FAILS.
    """
    # 1. Authoritative PostgreSQL pool & migrations
    if settings.ENVIRONMENT == "production" or settings.POSTGRES_URL:
        logger.info("Initializing authoritative PostgreSQL pool...")
        pool = await init_pool()
        logger.info("Authoritative PostgreSQL pool initialized successfully.")

        logger.info("Applying authoritative PostgreSQL migrations...")
        async with get_connection(pool=pool) as conn:
            applied = await apply_migrations(conn)
        logger.info("PostgreSQL migrations applied successfully: %s", applied)
    else:
        logger.warning(
            "Starting without authoritative PostgreSQL (ENVIRONMENT=%s, POSTGRES_URL is empty).",
            settings.ENVIRONMENT,
        )

    # 2. Initialize required legacy SQLite state
    await init_db()
    logger.info("Legacy SQLite state initialized")

    # 3. Setup OpenTelemetry (separate from ring buffer)
    setup_otel(service_name="quorum")
    logger.info("OTel initialized")

    # 4. Start Reaper as a background coroutine
    from reaper.reaper import reaper
    reaper_task = asyncio.create_task(reaper.run(), name="reaper")
    logger.info("Reaper started")

    # 5. Start Adjudicator as a background coroutine
    from adjudicator.adjudicator import adjudicator
    adjudicator_task = asyncio.create_task(adjudicator.run(), name="adjudicator")
    logger.info("Adjudicator started")

    yield

    # Graceful shutdown: stop background workers
    reaper_task.cancel()
    adjudicator_task.cancel()
    try:
        await asyncio.gather(reaper_task, adjudicator_task, return_exceptions=True)
    except Exception:
        pass

    # Close PostgreSQL pool cleanly
    await close_pool()
    logger.info("Quorum shutdown complete")


app = FastAPI(
    title="Quorum",
    description="Multi-agent semantic coordination system with in-process Moss semantic board.",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Compute allowed CORS origins based on configuration (never wildcard with credentials in prod)
allowed_origins: list[str] = []
if settings.FRONTEND_URL:
    allowed_origins.append(settings.FRONTEND_URL.rstrip("/"))
if settings.ENVIRONMENT != "production":
    allowed_origins.extend([
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ])
cors_origins = list(dict.fromkeys(allowed_origins))

# Middleware (applied in reverse order)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*"],
)

# Routes
app.include_router(runs.router, prefix="/runs", tags=["runs"])
app.include_router(bus.router, prefix="/bus", tags=["bus"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "quorum"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
