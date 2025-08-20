# config.py
import logging, os
try:
    # Pydantic v2
    from pydantic_settings import BaseSettings, SettingsConfigDict
    from pydantic import Field, model_validator
    V2 = True
except ImportError:
    # Pydantic v1 fallback
    from pydantic import BaseSettings, Field  # type: ignore
    V2 = False

class Settings(BaseSettings):
    # --- v2 style config ---
    if V2:
        model_config = SettingsConfigDict(
            env_file=".env",
            extra="ignore",          # <-- ignore unknown env vars (like old keys)
        )
    else:
        # --- v1 style config ---
        class Config:
            env_file = ".env"
            extra = "ignore"

    DISCORD_TOKEN: str
    GUILD_ID: int | None = None
    LOG_DIR: str | None = None

    LOG_LEVEL: str = "DEBUG"

    # Background pending worker cadence (new key: PENDING_SCAN_SECONDS)
    PENDING_SCAN_SECONDS: int = Field(30)  # legacy mapping handled below

    # Persistent retry settings
    PENDING_MAX_ATTEMPTS: int = 12
    PENDING_BASE_BACKOFF: int = 60
    PENDING_CONCURRENCY: int = 2

    SQLITE_PATH: str = "events.db"

    # Session improvements
    EVENT_GRACE_MINUTES: int = 10
    FILE_SETTLE_SECONDS: int = 5

    # Map legacy env var PENDING_SCAN_MIN (minutes) -> PENDING_SCAN_SECONDS
    if V2:
        @model_validator(mode="before")
        @classmethod
        def _apply_legacy_env(cls, values):
            # values contains env-derived fields (strings) before parsing
            if "PENDING_SCAN_SECONDS" not in values:
                legacy = os.getenv("PENDING_SCAN_MIN")
                if legacy:
                    try:
                        values["PENDING_SCAN_SECONDS"] = int(legacy) * 60
                    except Exception:
                        pass
            return values

settings = Settings()

def setup_logging():
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")