"""Простой приёмник webhook'ов для ручного тестирования.

POST /hook  -> 200 (логирует тело)
POST /fail  -> 500 (для проверки retry и DLQ)
"""
import logging

from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook-mock")

app = FastAPI(title="webhook-mock")


@app.post("/hook")
async def hook(request: Request) -> dict:
    logger.info("WEBHOOK RECEIVED: %s", await request.json())
    return {"ok": True}


@app.post("/fail")
async def fail(request: Request) -> None:
    logger.warning("WEBHOOK (will fail): %s", await request.json())
    raise HTTPException(status_code=500, detail="simulated failure")
