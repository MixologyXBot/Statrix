# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import json
import logging

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):

    DATABASE_URL: str

    ENCRYPTION_KEY: str

    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 168

    OWNER_EMAIL: str
    OWNER_PASSWORD: str
    OWNER_NAME: str = ""

    CORS_ORIGINS: str = '["http://localhost:8000","http://127.0.0.1:8000"]'

    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASS: str = ""
    SMTP_FROM: str = ""
    NOTIFICATION_EMAIL: str = ""

    OFFLINE_NOTIFICATION_MINUTES: int = 3

    APP_NAME: str = "Statrix"
    APP_URL: str = "http://localhost:8000"
    COMPANY_NAME: str = "Statrix"
    LOG_LEVEL: str = "INFO"
    UVICORN_ACCESS_LOG: bool = False

    STATUS_LOGO: str = ""
    STATUS_PAGE_TITLE: str = "Statrix Status"

    CHECK_INTERVAL_SECONDS: int = 60
    NOTIFICATION_CHECK_INTERVAL_SECONDS: int = 30

    CACHE_BACKEND: str = "redis"
    REDIS_URL: str = ""
    CACHE_FAIL_FAST: bool = True
    CACHE_WARMUP_FULL: bool = True
    CACHE_KEY_PREFIX: str = "statrix:v1"
    CACHE_WARMUP_BATCH_SIZE: int = 500
    CACHE_REBUILD_INTERVAL_SECONDS: int = 30
    MONITOR_LEADER_LOCK_ENABLED: bool = True
    MONITOR_LEADER_LOCK_TTL_SECONDS: int = 90

    PG_POOL_MIN_SIZE: int = 2
    PG_POOL_MAX_SIZE: int = 5

    DATA_RETENTION_DAYS: int = 7
    DATA_COMPRESSION_HOUR_UTC: int = 2

    CACHE_ONLY: bool = False
    ENABLE_IN_MEMORY_CACHE: bool = True
    PUBLIC_STATUS_CACHE_TTL_SECONDS: int = 10
    STATUS_SUMMARY_ENABLED: bool = True
    STATUS_SUMMARY_WARMUP_DELAY_SECONDS: int = 90
    STATUS_SUMMARY_COLD_WAIT_SECONDS: int = 5
    STATUS_SUMMARY_FLUSH_INTERVAL_SECONDS: int = 10
    STATUS_SUMMARY_MAX_TIMELINE_SEGMENTS: int = 32
    STATUS_SUMMARY_PARTIAL_DOWNTIME_MINUTES: float = 15.0
    STATUS_SUMMARY_REDIS_PREFIX: str = "status:summary:v1"

    def get_cors_origins(self) -> list[str]:
        """Parse CORS_ORIGINS string into a list.  Raises on invalid JSON."""
        try:
            origins = json.loads(self.CORS_ORIGINS)
            if not isinstance(origins, list):
                raise ValueError("CORS_ORIGINS must be a JSON array of strings")
            return origins
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(f"Invalid CORS_ORIGINS configuration: {exc}") from exc

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

if settings.CACHE_ONLY:
    logger.warning("CACHE_ONLY is deprecated and will be removed. Use CACHE_BACKEND instead.")
if not settings.ENABLE_IN_MEMORY_CACHE:
    logger.warning("ENABLE_IN_MEMORY_CACHE=false is deprecated. Set CACHE_BACKEND='inmemory' instead.")
