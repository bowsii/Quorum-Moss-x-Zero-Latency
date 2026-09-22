"""
quorum/config/settings.py
--------------------------
Centralised configuration loaded from environment variables and an optional
.env file.  All sensitive values MUST be supplied via environment — nothing is
hard-coded here.
"""

from typing import Literal
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict



class Settings(BaseSettings):
    """Application-wide settings resolved from the environment.

    Precedence (highest → lowest):
      1. Real environment variables
      2. Values in the ``.env`` file (if present)
      3. Declared defaults below
    """

    # ------------------------------------------------------------------
    # Security / JWT
    # ------------------------------------------------------------------
    SECRET_KEY: str = Field(
        default="dev_secret_key_quorum_32bytes_min_jwt_token_development",
        description="HMAC secret used to sign JWT tokens. Must be overridden in production.",
    )
    ALGORITHM: str = Field(default="HS256", description="JWT signing algorithm.")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(
        default=30,
        description="Lifetime of an access token in minutes.",
    )
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(
        default=7,
        description="Lifetime of a refresh token in days.",
    )

    # ------------------------------------------------------------------
    # External LLM / AI provider keys
    # ------------------------------------------------------------------
    GROQ_API_KEY: str = Field(default="", description="API key for Groq.")
    OPENROUTER_API_KEY: str = Field(
        default="", description="API key for OpenRouter."
    )
    HF_API_KEY: str = Field(
        default="", description="Hugging Face inference API key."
    )
    TAVILY_API_KEY: str = Field(default="", description="API key for Tavily search.")

    # ------------------------------------------------------------------
    # LiveKit (real-time voice / video)
    # ------------------------------------------------------------------
    LIVEKIT_URL: str = Field(default="", description="LiveKit server WebSocket URL.")
    LIVEKIT_API_KEY: str = Field(default="", description="LiveKit API key.")
    LIVEKIT_API_SECRET: str = Field(default="", description="LiveKit API secret.")

    # ------------------------------------------------------------------
    # OpenTelemetry
    # ------------------------------------------------------------------
    OTEL_EXPORTER_OTLP_ENDPOINT: str = Field(
        default="http://localhost:4317",
        description="OTLP gRPC endpoint for trace/metric export.",
    )

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    DATABASE_URL: str = Field(
        default="sqlite+aiosqlite:///./quorum.db",
        description="Async-compatible SQLAlchemy database URL.",
    )
    POSTGRES_URL: str = Field(
        default="",
        description="PostgreSQL connection string for authoritative transactional state (e.g. postgresql://user:pass@localhost:5432/quorum).",
    )

    # ------------------------------------------------------------------
    # ChromaDB namespace identifiers
    # ------------------------------------------------------------------
    MOSS_CLAIMS_NAMESPACE: str = Field(
        default="quorum_claims",
        description="ChromaDB collection name for MOSS agent claims.",
    )
    MOSS_FINDINGS_NAMESPACE: str = Field(
        default="quorum_findings",
        description="ChromaDB collection name for MOSS agent findings.",
    )

    # ------------------------------------------------------------------
    # Agent heartbeat / reaper
    # ------------------------------------------------------------------
    HEARTBEAT_TTL_SECONDS: int = Field(
        default=15,
        description="Seconds after which a silent agent is considered dead.",
    )
    REAPER_SCAN_INTERVAL_SECONDS: int = Field(
        default=1,
        description="How often (seconds) the reaper task checks for dead agents.",
    )

    # ------------------------------------------------------------------
    # Runtime environment
    # ------------------------------------------------------------------
    ENVIRONMENT: Literal["development", "test", "staging", "production"] = Field(
        default="development",
        description=(
            "Runtime environment label (development | test | staging | production). "
            "Used to gate hard assertions and debug behaviour."
        ),
    )

    @model_validator(mode="after")
    def validate_production_secrets(self) -> "Settings":
        if self.ENVIRONMENT == "production":
            if self.SECRET_KEY == "dev_secret_key_quorum_32bytes_min_jwt_token_development":
                raise ValueError(
                    "SECRET_KEY must be overridden with a secure random key when ENVIRONMENT='production'."
                )
        return self

    # ------------------------------------------------------------------
    # Pydantic-settings configuration
    # ------------------------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )



# ---------------------------------------------------------------------------
# Module-level singleton — import this everywhere instead of re-instantiating.
# ---------------------------------------------------------------------------
settings: Settings = Settings()
