"""
MAMI FastAPI Application
========================
REST API + WebSocket endpoint.

Endpoints:
  GET  /                          → dashboard HTML
  WS   /ws/live                   → real-time JSON stream (500 ms)
  GET  /api/v1/snapshot           → current full state
  GET  /api/v1/greeks/{symbol}    → Greeks for one position
  GET  /api/v1/var                → portfolio VaR / CVaR
  GET  /api/v1/alerts             → recent anomaly alerts
  GET  /api/v1/history/{symbol}   → price + return history
  POST /api/v1/trigger-crash      → inject flash crash (demo)
  GET  /docs                      → auto-generated API documentation
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from mami_core.config import TICK_INTERVAL_SECONDS
from .engine import SimulationEngine

# ── Globals ────────────────────────────────────────────────────────────────────
engine: SimulationEngine = SimulationEngine()
clients: Set[WebSocket]  = set()

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")


# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_broadcast_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _broadcast_loop():
    """Generate one tick per interval; push JSON to all WebSocket clients."""
    while True:
        try:
            state = engine.step()
            payload = json.dumps(state)

            dead: Set[WebSocket] = set()
            for ws in list(clients):
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            clients.difference_update(dead)

            await asyncio.sleep(TICK_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[broadcast] {exc}")
            await asyncio.sleep(1)


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="MAMI Risk Platform",
    description="Real-Time Multi-Asset Risk & Market Intelligence — local demo",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.isdir(DASHBOARD_DIR):
    app.mount("/static", StaticFiles(directory=DASHBOARD_DIR), name="static")


# ── WebSocket ──────────────────────────────────────────────────────────────────
@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    # Send snapshot immediately so dashboard doesn't wait for first tick
    await websocket.send_text(json.dumps(engine.snapshot()))
    try:
        while True:
            await websocket.receive_text()   # keep-alive ping handler
    except WebSocketDisconnect:
        clients.discard(websocket)


# ── REST Endpoints ─────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root():
    idx = os.path.join(DASHBOARD_DIR, "index.html")
    if os.path.exists(idx):
        return FileResponse(idx)
    return JSONResponse({"status": "MAMI running", "docs": "/docs"})


@app.get("/api/v1/snapshot", summary="Full current state")
async def snapshot():
    return engine.snapshot()


@app.get("/api/v1/greeks/{symbol}", summary="Greeks for a portfolio position")
async def greeks(symbol: str):
    sym = symbol.upper()
    g = engine.greeks.get(sym)
    if g is None:
        return JSONResponse({"error": f"No position for {sym}"}, status_code=404)
    return g


@app.get("/api/v1/var", summary="Portfolio VaR and CVaR")
async def var_endpoint():
    return {**engine.portfolio, **engine.var}


@app.get("/api/v1/alerts", summary="Recent anomaly alerts")
async def alerts():
    return list(engine.alerts)


@app.get("/api/v1/history/{symbol}", summary="Price history for a symbol")
async def history(symbol: str):
    return engine.history(symbol.upper())


@app.post("/api/v1/trigger-crash", summary="Inject a flash crash (demo)")
async def trigger_crash(symbol: str = None):
    sym = symbol.upper() if symbol else None
    target = engine.trigger_crash(sym)
    return {"message": f"Flash crash injected for {target}", "symbol": target}
