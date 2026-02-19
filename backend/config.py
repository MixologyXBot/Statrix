# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import json
import logging

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):

    # Database
    DATABASE_URL: str

    # Encryption
    ENCRYPTION_KEY: str

    # JWT Authentication
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 168  # 7 days

    # Owner Credentials (single admin user)
    OWNER_EMAIL: str
    OWNER_PASSWORD: str
    OWNER_NAME: str = ""

    # CORS Origins
    CORS_ORIGINS: str = '["http://localhost:8000","http://127.0.0.1:8000"]'

    # Email Notification (SMTP)
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASS: str = ""
    SMTP_FROM: str = ""
    NOTIFICATION_EMAIL: str = ""

    # Notification Settings
    OFFLINE_NOTIFICATION_MINUTES: int = 3

    # App Settings
    APP_NAME: str = "Statrix"
    APP_URL: str = "http://localhost:8000"
    COMPANY_NAME: str = "Statrix"

    # Status Page Settings
    STATUS_LOGO: str = ""
    STATUS_PAGE_TITLE: str = "Statrix Status"

    # Background Task Settings
    CHECK_INTERVAL_SECONDS: int = 60
    NOTIFICATION_CHECK_INTERVAL_SECONDS: int = 30

    # Cache Settings
    CACHE_BACKEND: str = "redis"
    REDIS_URL: str = ""
    CACHE_FAIL_FAST: bool = True
    CACHE_WARMUP_FULL: bool = True
    CACHE_KEY_PREFIX: str = "statrix:v1"
    CACHE_WARMUP_BATCH_SIZE: int = 500
    CACHE_REBUILD_INTERVAL_SECONDS: int = 30
    MONITOR_LEADER_LOCK_ENABLED: bool = True
    MONITOR_LEADER_LOCK_TTL_SECONDS: int = 90

    # Deprecated compatibility flags (kept for older deployments)
    CACHE_ONLY: bool = False
    ENABLE_IN_MEMORY_CACHE: bool = True
    PUBLIC_STATUS_CACHE_TTL_SECONDS: int = 10

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
