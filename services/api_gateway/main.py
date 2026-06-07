"""
MAMI API Gateway
================
The edge service. It owns no risk math — it tails the bus, reads shared state
from Redis, and serves clients.

  • Consumes `market.ticks`; writes latest prices + capped history to Redis and
    pushes an assembled snapshot to every WebSocket client.
  • REST /api/v1/* reads the same shared state from Redis.
  • POST /api/v1/trigger-crash publishes a command back onto `market.commands`,
    which the ingestion service applies — a full round-trip through the bus.
  • Serves the (unchanged) dashboard; the assembled snapshot is byte-compatible
    with the original single-process payload.
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Set

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from mami_core.config import (
    KAFKA_BOOTSTRAP, TOPIC_TICKS, TOPIC_COMMANDS, REDIS_URL,
    RKEY_PRICES, RKEY_HISTORY, RKEY_GREEKS, RKEY_PORTFOLIO, RKEY_VAR,
    RKEY_SCORES, RKEY_FORECASTS, RKEY_ALERTS, RKEY_TICK, VAR_CONFIDENCE,
)
from mami_core import schemas

HISTORY_LEN = 120
DASHBOARD = os.getenv("DASHBOARD_PATH", os.path.join(os.path.dirname(__file__), "dashboard", "index.html"))

clients: Set[WebSocket] = set()
state = {"redis": None, "consumer": None, "producer": None}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def assemble_snapshot() -> dict:
    """Reassemble the original snapshot dict from distributed Redis state."""
    r = state["redis"]
    prices    = json.loads(await r.get(RKEY_PRICES)    or "{}")
    greeks    = json.loads(await r.get(RKEY_GREEKS)    or "{}")
    portfolio = json.loads(await r.get(RKEY_PORTFOLIO) or "{}")
    var       = json.loads(await r.get(RKEY_VAR)       or json.dumps(
        {"portfolio_var": 0.0, "portfolio_cvar": 0.0, "confidence": VAR_CONFIDENCE}))
    forecasts = json.loads(await r.get(RKEY_FORECASTS) or "{}")
    scores    = await r.hgetall(RKEY_SCORES)
    tick      = int(await r.get(RKEY_TICK) or 0)
    alerts    = [json.loads(x) for x in await r.lrange(RKEY_ALERTS, 0, 19)]

    for sym, block in prices.items():
        hist = await r.lrange(RKEY_HISTORY + sym, -HISTORY_LEN, -1)
        block["history"] = [float(p) for p in hist]
        block["anomaly_score"] = float(scores.get(sym, 0.0))
        block["forecast"] = forecasts.get(sym, {})

    return {
        "timestamp": _now(), "tick": tick, "prices": prices,
        "greeks": greeks, "portfolio": portfolio, "var": var, "alerts": alerts,
    }


async def tick_loop():
    """Tail market.ticks: persist prices/history to Redis, broadcast snapshots."""
    consumer = state["consumer"]
    async for msg in consumer:
        frame = msg.value
        prices = frame["prices"]
        r = state["redis"]
        pipe = r.pipeline()
        pipe.set(RKEY_PRICES, json.dumps(prices))
        pipe.set(RKEY_TICK, frame["tick"])
        for sym, block in prices.items():
            pipe.rpush(RKEY_HISTORY + sym, block["price"])
            pipe.ltrim(RKEY_HISTORY + sym, -HISTORY_LEN, -1)
        await pipe.execute()

        if clients:
            snap = json.dumps(await assemble_snapshot())
            dead = set()
            for ws in list(clients):
                try:
                    await ws.send_text(snap)
                except Exception:
                    dead.add(ws)
            clients.difference_update(dead)


async def _connect_with_retry():
    state["redis"] = aioredis.from_url(REDIS_URL, decode_responses=True)
    while True:
        try:
            consumer = AIOKafkaConsumer(
                TOPIC_TICKS, bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest", group_id="api-gateway",
            )
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await consumer.start()
            await producer.start()
            state["consumer"], state["producer"] = consumer, producer
            print("[gateway] connected to Kafka + Redis", flush=True)
            return
        except Exception as exc:
            print(f"[gateway] waiting for Kafka: {exc}", flush=True)
            await asyncio.sleep(3)


app = FastAPI(title="MAMI API Gateway", version="2.0.0")


@app.on_event("startup")
async def startup():
    await _connect_with_retry()
    app.state.task = asyncio.create_task(tick_loop())


@app.on_event("shutdown")
async def shutdown():
    app.state.task.cancel()
    if state["consumer"]:
        await state["consumer"].stop()
    if state["producer"]:
        await state["producer"].stop()


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_text(json.dumps(await assemble_snapshot()))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)


@app.get("/", include_in_schema=False)
async def root():
    if os.path.exists(DASHBOARD):
        return FileResponse(DASHBOARD)
    return JSONResponse({"status": "MAMI gateway running", "docs": "/docs"})


@app.get("/api/v1/snapshot")
async def snapshot():
    return await assemble_snapshot()


@app.get("/api/v1/greeks/{symbol}")
async def greeks(symbol: str):
    g = json.loads(await state["redis"].get(RKEY_GREEKS) or "{}")
    sym = symbol.upper()
    if sym not in g:
        return JSONResponse({"error": f"No position for {sym}"}, status_code=404)
    return g[sym]


@app.get("/api/v1/var")
async def var_endpoint():
    r = state["redis"]
    portfolio = json.loads(await r.get(RKEY_PORTFOLIO) or "{}")
    var = json.loads(await r.get(RKEY_VAR) or "{}")
    return {**portfolio, **var}


@app.get("/api/v1/alerts")
async def alerts():
    return [json.loads(x) for x in await state["redis"].lrange(RKEY_ALERTS, 0, 49)]


@app.get("/api/v1/history/{symbol}")
async def history(symbol: str):
    sym = symbol.upper()
    hist = await state["redis"].lrange(RKEY_HISTORY + sym, 0, -1)
    return {"symbol": sym, "prices": [float(p) for p in hist], "count": len(hist)}


@app.post("/api/v1/trigger-crash")
async def trigger_crash(symbol: str = None):
    sym = symbol.upper() if symbol else None
    await state["producer"].send_and_wait(TOPIC_COMMANDS, schemas.command_msg("trigger_crash", sym))
    return {"message": f"Flash crash command sent for {sym or 'random symbol'}", "symbol": sym}
