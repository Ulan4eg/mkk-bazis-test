"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Гарантия: at-least-once. Событие помечается published_at только после того, как
брокер подтвердил приём (publisher confirms). Если процесс упадёт между confirm
и commit, событие уйдёт повторно - consumer идемпотентен.

SELECT ... FOR UPDATE SKIP LOCKED позволяет запускать несколько реплик relay
без двойной публикации и без блокировок друг друга.
"""
import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.config import settings
from app.db import session_factory
from app.messaging import publish_new
from app.models import OutboxEvent, utcnow

logger = logging.getLogger(__name__)


async def publish_pending_batch(broker: RabbitBroker) -> int:
    """Публикует одну пачку неотправленных событий. Возвращает число опубликованных."""
    published = 0
    async with session_factory() as session, session.begin():
        events = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()

        for event in events:
            try:
                await publish_new(broker, event.payload, message_id=str(event.id))
            except Exception:
                # Останавливаемся, но уже опубликованные события всё равно помечаем -
                # транзакция завершится коммитом. Остальные уйдут на следующей итерации.
                logger.exception("Failed to publish outbox event %s", event.id)
                break
            event.published_at = utcnow()
            published += 1
    return published


async def run_outbox_relay(broker: RabbitBroker, stop: asyncio.Event) -> None:
    logger.info("Outbox relay started")
    while not stop.is_set():
        try:
            published = await publish_pending_batch(broker)
        except Exception:
            logger.exception("Outbox relay iteration failed")
            published = 0

        if published >= settings.outbox_batch_size:
            continue  # очередь не пуста - сразу следующая пачка
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
    logger.info("Outbox relay stopped")
