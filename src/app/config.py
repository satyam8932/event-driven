from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "pipeline"
    postgres_user: str = "pipeline"
    postgres_password: str = "pipeline"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # RabbitMQ
    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_vhost: str = "/"

    @property
    def rabbitmq_url(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}{self.rabbitmq_vhost}"
        )

    # Redis
    redis_url: str = "redis://redis:6379/0"

    # MinIO
    minio_endpoint: str = "minio:9000"
    minio_public_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "pipeline"
    minio_secure: bool = False

    # Worker
    worker_stages: str = "parse,tts,stitch,notify"
    worker_prefetch: int = 8

    # Relay
    relay_poll_interval: float = 0.5
    relay_batch_size: int = 50

    # Janitor
    janitor_interval: int = 30
    janitor_lease_timeout: int = 120
    janitor_outbox_prune_age: int = 86400

    # Pipeline
    tts_max_concurrent: int = 3
    tts_lease_seconds: int = 60
    retry_max_attempts: int = 3
    retry_base_ms: int = 2000
    retry_max_ms: int = 8000

    # Webhook
    webhook_url: str = Field(default="")


@lru_cache
def get_settings() -> Settings:
    return Settings()
