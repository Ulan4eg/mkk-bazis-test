"""Бизнес-логика обработки платежа. Идемпотентна: безопасно вызывать повторно."""
import logging
import uuid

from sqlalchemy import update

from app import gateway, webhook
from app.db import session_factory
from app.models import Payment, PaymentStatus, utcnow

logger = logging.getLogger(__name__)


class PaymentNotFound(Exception):
    pass


async def _load(payment_id: uuid.UUID) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFound(str(payment_id))
    return payment


async def process_payment(payment_id: uuid.UUID) -> None:
    payment = await _load(payment_id)

    # Шаг 1. Обработка шлюзом - только если платёж ещё не финализирован.
    # При повторной доставке сообщения (retry / redelivery) шлюз второй раз не дёргаем.
    if payment.status == PaymentStatus.PENDING:
        success = await gateway.charge()
        new_status = PaymentStatus.SUCCEEDED if success else PaymentStatus.FAILED

        async with session_factory() as session:
            # compare-and-set: если параллельный обработчик уже финализировал платёж,
            # наше обновление не затронет ни одной строки.
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=new_status, processed_at=utcnow())
            )
            await session.commit()
        payment = await _load(payment_id)
        logger.info("Payment %s -> %s", payment_id, payment.status.value)

    # Шаг 2. Webhook - только если ещё не доставлен.
    if payment.webhook_delivered_at is None:
        await webhook.deliver(payment)  # при ошибке бросит исключение -> retry
        async with session_factory() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(webhook_delivered_at=utcnow())
            )
            await session.commit()
        logger.info("Webhook delivered for payment %s", payment_id)
