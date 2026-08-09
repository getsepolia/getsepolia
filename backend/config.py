from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent


class Settings(BaseSettings):
    app_name: str = "Sepolia Market API"
    app_environment: str = "development"
    api_prefix: str = "/api"

    database_path: Path = Field(
        default=BASE_DIR / "sepolia_market.sqlite3"
    )

    frontend_origin: str = "http://localhost:8080"
    order_expiry_minutes: int = 15

    custom_min: int = 1
    custom_max: int = 500
    daily_limit_per_wallet: int = 500

    payment_enabled: bool = False
    service_status: str = "development"
    enable_dev_status_endpoint: bool = True

    model_config = SettingsConfigDict(
        env_file=PROJECT_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
