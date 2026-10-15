"""Эмуляция внешнего платёжного шлюза."""
import asyncio
import random

from app.config import settings


async def charge() -> bool:
    """Обработка 2-5 секунд, 90% успех / 10% отказ."""
    await asyncio.sleep(
        random.uniform(settings.gateway_min_delay_seconds, settings.gateway_max_delay_seconds)
    )
    return random.random() < settings.gateway_success_rate
