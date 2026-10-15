"""Consumer: один обработчик, который делает всё.

Запуск: faststream run app.consumer:app
"""
import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.config import settings
from app.messaging import (
    ATTEMPT_HEADER,
    declare_topology,
    exchange,
    new_queue,
    schedule_retry,
    send_to_dlq,
)
from app.processing import process_payment
from app.schemas import PaymentEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("consumer")

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)


@app.on_startup
async def _declare_topology() -> None:
    # выполняется до подключения брокера и старта подписчиков
    await declare_topology()


@broker.subscriber(new_queue, exchange)
async def handle_new_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    attempt = int(message.headers.get(ATTEMPT_HEADER, 1))
    payload = {"payment_id": str(event.payment_id)}

    try:
        await process_payment(event.payment_id)
    except Exception as exc:
        logger.exception(
            "Payment %s: attempt %d/%d failed", event.payment_id, attempt, settings.max_attempts
        )
        if attempt < settings.max_attempts:
            await schedule_retry(broker, payload, attempt)
        else:
            logger.error("Payment %s: giving up, sending to DLQ", event.payment_id)
            await send_to_dlq(broker, payload, attempt, f"{type(exc).__name__}: {exc}")
        # Возвращаемся без исключения -> сообщение ack'ается только после того, как
        # копия безопасно лежит в retry-очереди / DLQ. Если публикация выше упадёт,
        # исключение приведёт к reject, и сообщение уйдёт в DLQ через DLX очереди.
