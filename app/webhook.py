import httpx

from app.config import settings
from app.models import Payment


class WebhookDeliveryError(Exception):
    """Webhook не доставлен (сетевая ошибка, таймаут или не-2xx ответ)."""


def build_payload(payment: Payment) -> dict:
    return {
        "event": f"payment.{payment.status.value}",
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),  # строкой, чтобы не терять точность decimal
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata": payment.meta,
        "created_at": payment.created_at.isoformat(),
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


async def deliver(payment: Payment) -> None:
    try:
        async with httpx.AsyncClient(
            timeout=settings.webhook_timeout_seconds, follow_redirects=False
        ) as client:
            response = await client.post(payment.webhook_url, json=build_payload(payment))
    except httpx.HTTPError as exc:
        raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc

    if not response.is_success:
        raise WebhookDeliveryError(f"unexpected status {response.status_code}")
