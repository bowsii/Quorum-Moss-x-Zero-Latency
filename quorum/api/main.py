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
from observability.otel_setup import setup_otel
from config.settings import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB, OTel, Reaper, Adjudicator coroutines."""
    # Initialize SQLite
    await init_db()
    logger.info("Database initialized")

    # Setup OpenTelemetry (separate from ring buffer)
    setup_otel(service_name="quorum")
    logger.info("OTel initialized")

    # Start Reaper as a background coroutine
    from reaper.reaper import reaper
    reaper_task = asyncio.create_task(reaper.run(), name="reaper")
    logger.info("Reaper started")

    # Start Adjudicator as a background coroutine
    from adjudicator.adjudicator import adjudicator
    adjudicator_task = asyncio.create_task(adjudicator.run(), name="adjudicator")
    logger.info("Adjudicator started")

    yield

    # Graceful shutdown
    reaper_task.cancel()
    adjudicator_task.cancel()
    try:
        await asyncio.gather(reaper_task, adjudicator_task, return_exceptions=True)
    except Exception:
        pass
    logger.info("Quorum shutdown complete")


app = FastAPI(
    title="Quorum",
    description="Multi-agent semantic coordination system with in-process Moss semantic board.",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Middleware (applied in reverse order)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
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
