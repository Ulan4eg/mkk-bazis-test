from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "secret-api-key"

    # Retry / DLQ
    max_attempts: int = 3  # всего попыток обработки сообщения
    retry_base_delay_seconds: int = 5  # задержка перед n-й повторной попыткой: base * 2^(n-1)

    # Webhook
    webhook_timeout_seconds: float = 10.0

    # Outbox relay
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100

    # Эмуляция платёжного шлюза
    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = 0.9


settings = Settings()
