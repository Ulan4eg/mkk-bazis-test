import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.security import require_api_key
from app.db import get_session
from app.messaging import RK_NEW
from app.models import OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)

IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
]


def _accepted(p: Payment) -> PaymentAccepted:
    return PaymentAccepted(payment_id=p.id, status=p.status, created_at=p.created_at)


def _same_request(p: Payment, body: PaymentCreate) -> bool:
    return (
        p.amount == body.amount
        and p.currency == body.currency
        and p.description == body.description
        and p.webhook_url == str(body.webhook_url)
        and p.meta == body.metadata
    )


async def _find_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _replay(existing: Payment, body: PaymentCreate) -> PaymentAccepted:
    """Повторный запрос с тем же ключом: отдаём уже созданный платёж."""
    if not _same_request(existing, body):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        )
    return _accepted(existing)


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: AsyncSession = Depends(get_session),
) -> PaymentAccepted:
    existing = await _find_by_key(session, idempotency_key)
    if existing is not None:
        return _replay(existing, body)

    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        meta=body.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(body.webhook_url),
    )
    session.add(payment)
    try:
        # Платёж и outbox-событие фиксируются ОДНОЙ транзакцией:
        # либо есть и то и другое, либо ничего.
        await session.flush()
        session.add(
            OutboxEvent(event_type=RK_NEW, payload={"payment_id": str(payment.id)})
        )
        await session.commit()
    except IntegrityError:
        # Гонка: параллельный запрос с тем же ключом успел закоммититься первым.
        await session.rollback()
        existing = await _find_by_key(session, idempotency_key)
        if existing is None:
            raise
        return _replay(existing, body)

    return _accepted(payment)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentDetail:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentDetail(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
