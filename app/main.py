import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

from app.api.payments import router as payments_router
from app.config import settings
from app.messaging import declare_topology
from app.outbox import run_outbox_relay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await declare_topology()
    broker = RabbitBroker(settings.rabbitmq_url)
    await broker.connect()
    await broker.start()

    stop = asyncio.Event()
    relay_task = asyncio.create_task(run_outbox_relay(broker, stop))
    try:
        yield
    finally:
        stop.set()
        await relay_task
        await broker.close()


app = FastAPI(title="Payment processing service", version="1.0.0", lifespan=lifespan)
app.include_router(payments_router)


@app.get("/health", tags=["health"])
async def health() -> dict:
    return {"status": "ok"}
