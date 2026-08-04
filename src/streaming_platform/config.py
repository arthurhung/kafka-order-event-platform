"""Environment-backed application configuration."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration shared by local scripts and future applications."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )

    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_ORDER_TOPIC: str
    KAFKA_LOG_TOPIC: str
    KAFKA_DLQ_TOPIC: str
    KAFKA_ORDER_CONSUMER_GROUP: str
    KAFKA_LOG_CONSUMER_GROUP: str
    KAFKA_UI_PORT: int = Field(ge=1, le=65535)

    POSTGRES_HOST: str
    POSTGRES_PORT: int = Field(ge=1, le=65535)
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: SecretStr

    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    @property
    def database_url(self) -> str:
        """Build the SQLAlchemy URL without exposing it in logs or reprs."""
        password = self.POSTGRES_PASSWORD.get_secret_value()
        return (
            f"postgresql+psycopg://{self.POSTGRES_USER}:{password}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


@lru_cache
def get_settings() -> Settings:
    """Return one validated settings instance per process."""
    return Settings()
