"""Топология RabbitMQ и хелперы публикации.

    producer (outbox relay)
          │  rk=payments.new
          ▼
    [payments]  direct ──► payments.new ──(reject/nack)──► [payments.dlx] ──► payments.dlq
                              ▲   │ ошибка, попытка n < MAX
                              │   ▼
                              │ [payments.retry] rk=retry.n ──► payments.new.retry.n
                              │                                    (TTL = base * 2^(n-1))
                              └─────────── dead-letter по истечении TTL ◄──┘

Ошибка обработки на попытке n < MAX -> сообщение уходит в retry-очередь n,
где «отлёживается» TTL и через DLX возвращается в payments.new.
На попытке MAX сообщение публикуется напрямую в DLX -> payments.dlq.
Номер попытки хранится в заголовке x-attempt.
"""
import asyncio
import logging
import uuid

import aio_pika
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

logger = logging.getLogger(__name__)

# --- имена ---
EXCHANGE_NAME = "payments"
RETRY_EXCHANGE_NAME = "payments.retry"
DLX_NAME = "payments.dlx"

QUEUE_NEW = "payments.new"
QUEUE_DLQ = "payments.dlq"

RK_NEW = "payments.new"
RK_DEAD = "payments.dead"

ATTEMPT_HEADER = "x-attempt"

# --- параметры очередей (общие для topology и подписчика, чтобы объявления совпадали) ---
NEW_QUEUE_ARGS = {
    "x-dead-letter-exchange": DLX_NAME,
    "x-dead-letter-routing-key": RK_DEAD,
}


def retry_queue_name(n: int) -> str:
    return f"payments.new.retry.{n}"


def retry_routing_key(n: int) -> str:
    return f"retry.{n}"


def retry_delay_seconds(n: int) -> int:
    """Задержка перед повтором после n-й неудачной попытки: base * 2^(n-1)."""
    return settings.retry_base_delay_seconds * 2 ** (n - 1)


# --- объекты FastStream ---
exchange = RabbitExchange(EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange(DLX_NAME, type=ExchangeType.DIRECT, durable=True)

new_queue = RabbitQueue(
    QUEUE_NEW, durable=True, routing_key=RK_NEW, arguments=NEW_QUEUE_ARGS
)


async def _connect_with_retry(
        url: str, attempts: int = 30, delay: float = 2.0
) -> aio_pika.abc.AbstractConnection:
    for attempt in range(1, attempts + 1):
        try:
            return await aio_pika.connect(url)
        except(aio_pika.exceptions.AMQPError, OSError) as exc:
            if attempt == attempts:
                raise
            logger.warning(
                "RabbitMQ not ready (%d/%d): %s. Retrying in %.0fs",
                attempt, attempts, exc, delay,
            )
            await asyncio.sleep(delay)


async def declare_topology(url: str | None = None) -> None:
    """Идемпотентно объявляет все обменники, очереди и биндинги.

    Вызывается и API (чтобы relay не публиковал в «никуда»), и consumer'ом.
    """
    connection = await _connect_with_retry(url or settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()

        ex_main = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
        )
        ex_retry = await channel.declare_exchange(
            RETRY_EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
        )
        ex_dlx = await channel.declare_exchange(
            DLX_NAME, aio_pika.ExchangeType.DIRECT, durable=True
        )

        q_new = await channel.declare_queue(
            QUEUE_NEW, durable=True, arguments=NEW_QUEUE_ARGS
        )
        await q_new.bind(ex_main, routing_key=RK_NEW)

        q_dlq = await channel.declare_queue(QUEUE_DLQ, durable=True)
        await q_dlq.bind(ex_dlx, routing_key=RK_DEAD)

        # По одной retry-очереди на каждую задержку (TTL задаётся на очередь, а не на
        # сообщение, иначе сообщения с разным TTL блокировали бы друг друга).
        for n in range(1, settings.max_attempts):
            q_retry = await channel.declare_queue(
                retry_queue_name(n),
                durable=True,
                arguments={
                    "x-message-ttl": retry_delay_seconds(n) * 1000,
                    "x-dead-letter-exchange": EXCHANGE_NAME,
                    "x-dead-letter-routing-key": RK_NEW,
                },
            )
            await q_retry.bind(ex_retry, routing_key=retry_routing_key(n))

    logger.info("RabbitMQ topology declared")


# --- публикация ---
async def publish_new(
    broker: RabbitBroker, payload: dict, *, message_id: str | None = None
) -> None:
    await broker.publish(
        payload,
        exchange=exchange,
        routing_key=RK_NEW,
        persist=True,
        message_id=message_id or str(uuid.uuid4()),
        headers={ATTEMPT_HEADER: 1},
    )


async def schedule_retry(broker: RabbitBroker, payload: dict, attempt: int) -> None:
    """Отправить на повтор после неудачной попытки `attempt`."""
    await broker.publish(
        payload,
        exchange=retry_exchange,
        routing_key=retry_routing_key(attempt),
        persist=True,
        headers={ATTEMPT_HEADER: attempt + 1},
    )


async def send_to_dlq(
    broker: RabbitBroker, payload: dict, attempt: int, error: str
) -> None:
    await broker.publish(
        payload,
        exchange=dlx_exchange,
        routing_key=RK_DEAD,
        persist=True,
        headers={ATTEMPT_HEADER: attempt, "x-last-error": error[:500]},
    )
