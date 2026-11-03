"""Runtime configuration loaded exclusively from environment variables."""

from functools import lru_cache

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service settings.

    ``DATABASE_URL`` is the only setting required in a deployed environment.  A
    local default is retained so the container and developer commands fail at
    readiness (rather than import) when PostgreSQL is not running.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    database_url: str = Field(
        default="postgresql://debugger:debugger@postgres:5432/debugger",
        validation_alias="DATABASE_URL",
    )
    database_min_pool_size: int = Field(default=1, ge=0, le=50)
    database_max_pool_size: int = Field(default=10, ge=1, le=100)
    database_connect_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    database_command_timeout_seconds: float = Field(default=20.0, gt=0, le=300)
    max_request_body_bytes: int = Field(
        default=20 * 1024 * 1024,
        ge=1024,
        validation_alias=AliasChoices("MAX_REQUEST_BODY_BYTES", "OTLP_MAX_REQUEST_BYTES"),
    )
    max_batch_events: int = Field(default=10_000, ge=1, le=100_000)
    cors_origins: list[str] = Field(default_factory=list)
    log_level: str = Field(default="INFO")

    @field_validator("database_url")
    @classmethod
    def database_url_must_be_postgresql(cls, value: str) -> str:
        if not value.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must use the postgresql:// scheme")
        return value

    @field_validator("database_max_pool_size")
    @classmethod
    def pool_max_must_be_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("database_max_pool_size must be at least one")
        return value

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @model_validator(mode="after")
    def valid_pool_range(self) -> "Settings":
        if self.database_min_pool_size > self.database_max_pool_size:
            raise ValueError("database_min_pool_size cannot exceed database_max_pool_size")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
