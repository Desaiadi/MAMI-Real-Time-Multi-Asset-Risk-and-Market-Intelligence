"""
Message contracts for the MAMI event bus.

All Kafka payloads are JSON (one document per message). These helpers define and
document the shapes so producers and consumers agree. Keeping them in the shared
library is the single source of truth for the wire format.

Topics & flow:
  market.ticks    ingestion  -> risk-engine, ml-service, api-gateway   (TickFrame)
  risk.metrics    risk-engine -> api-gateway                           (MetricsMsg)
  ml.alerts       ml-service  -> api-gateway                           (AlertMsg)
  market.commands api-gateway -> ingestion                             (CommandMsg)
"""
import json
from typing import Any, Dict


def encode(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


def decode(raw: bytes) -> Dict[str, Any]:
    return json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw)


# ── TickFrame (market.ticks) ────────────────────────────────────────────────────
# One message per simulation tick, carrying every symbol's latest quote + return.
#   {
#     "tick": int, "ts": iso8601,
#     "prices":  {sym: {price, bid, ask, spread, change_pct}},
#     "returns": {sym: float},      # per-tick fractional return
#   }
def tick_frame(tick: int, ts: str, prices: Dict[str, Any], returns: Dict[str, float]) -> Dict[str, Any]:
    return {"tick": tick, "ts": ts, "prices": prices, "returns": returns}


# ── MetricsMsg (risk.metrics) ───────────────────────────────────────────────────
#   { "tick": int, "greeks": {...}, "portfolio": {...}, "var": {...} }
def metrics_msg(tick: int, greeks: Dict, portfolio: Dict, var: Dict) -> Dict[str, Any]:
    return {"tick": tick, "greeks": greeks, "portfolio": portfolio, "var": var}


# ── AlertMsg (ml.alerts) ────────────────────────────────────────────────────────
#   { "tick": int, "scores": {sym: float}, "forecasts": {sym: {...}}, "alerts": [ {...} ] }
def alert_msg(tick: int, scores: Dict, forecasts: Dict, alerts: list) -> Dict[str, Any]:
    return {"tick": tick, "scores": scores, "forecasts": forecasts, "alerts": alerts}


# ── CommandMsg (market.commands) ────────────────────────────────────────────────
#   { "command": "trigger_crash", "symbol": str | null }
def command_msg(command: str, symbol: str | None = None) -> Dict[str, Any]:
    return {"command": command, "symbol": symbol}
