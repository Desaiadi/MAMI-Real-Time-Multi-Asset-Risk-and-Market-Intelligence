"""
MAMI Ingestion Service
======================
Replaces the in-process price generator with a standalone Kafka producer.

  • Generates GBM price ticks for every symbol (the "market data feed").
  • Publishes one TickFrame per tick to `market.ticks`.
  • Consumes `market.commands` (e.g. trigger_crash) on a background thread.

In production this is where FIX / WebSocket market-data connectors live. Here a
Geometric Brownian Motion simulator stands in, but it now emits onto a real
message bus that any number of downstream consumers can read independently.
"""
import json
import threading
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaConsumer, KafkaProducer

from mami_core.config import (
    INITIAL_PRICES, VOLATILITIES, RISK_FREE_RATE, TICK_INTERVAL_SECONDS,
    WARMUP_TICKS, FLASH_CRASH_PROB, KAFKA_BOOTSTRAP, TOPIC_TICKS, TOPIC_COMMANDS,
)
from mami_core import schemas


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MarketSimulator:
    def __init__(self):
        self.prices = {s: float(p) for s, p in INITIAL_PRICES.items()}
        self.prev   = dict(self.prices)
        self._crash: dict[str, int] = {}
        # dt = one tick in trading-year units
        self._dt = TICK_INTERVAL_SECONDS / (252 * 6.5 * 3600)
        self._lock = threading.Lock()

    def trigger_crash(self, symbol: str | None):
        with self._lock:
            target = symbol if symbol in self.prices else np.random.choice(list(self.prices))
            self._crash[target] = int(np.random.randint(4, 8))
        return target

    def advance(self, silent: bool = False) -> dict:
        """Advance every symbol one GBM step; return a TickFrame payload."""
        prices_out, returns_out = {}, {}
        with self._lock:
            for sym in INITIAL_PRICES:
                S, sigma = self.prices[sym], VOLATILITIES[sym]
                if self._crash.get(sym, 0) > 0:
                    shock = np.log(1 - np.random.uniform(0.012, 0.028))
                    self._crash[sym] -= 1
                elif not silent and np.random.random() < FLASH_CRASH_PROB:
                    shock = np.log(1 - np.random.uniform(0.018, 0.040))
                    self._crash[sym] = int(np.random.randint(3, 7))
                else:
                    drift     = (RISK_FREE_RATE - 0.5 * sigma ** 2) * self._dt
                    diffusion = sigma * np.sqrt(self._dt) * np.random.standard_normal()
                    shock     = drift + diffusion

                new_S = max(S * np.exp(shock), 1.0)
                ret   = (new_S - self.prev[sym]) / self.prev[sym] if self.prev[sym] else 0.0
                half_spread = float(np.random.uniform(0.01, 0.06))

                prices_out[sym] = {
                    "price":      round(new_S, 2),
                    "bid":        round(new_S - half_spread, 2),
                    "ask":        round(new_S + half_spread, 2),
                    "spread":     round(2 * half_spread, 4),
                    "change_pct": round((new_S - self.prev[sym]) / self.prev[sym] * 100, 4) if self.prev[sym] else 0.0,
                }
                returns_out[sym] = round(ret, 6)
                self.prev[sym]   = self.prices[sym]
                self.prices[sym] = new_S
        return prices_out, returns_out


def connect_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5, linger_ms=20,
            )
        except Exception as exc:
            print(f"[ingestion] waiting for Kafka: {exc}", flush=True)
            time.sleep(3)


def command_listener(sim: MarketSimulator):
    """Background thread: apply commands from the bus (e.g. flash crash)."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_COMMANDS, bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest", group_id="ingestion-commands",
            )
            print("[ingestion] command listener ready", flush=True)
            for msg in consumer:
                cmd = msg.value or {}
                if cmd.get("command") == "trigger_crash":
                    target = sim.trigger_crash(cmd.get("symbol"))
                    print(f"[ingestion] flash crash injected -> {target}", flush=True)
        except Exception as exc:
            print(f"[ingestion] command listener error: {exc}", flush=True)
            time.sleep(3)


def main():
    print(f"[ingestion] starting; bootstrap={KAFKA_BOOTSTRAP}", flush=True)
    sim = MarketSimulator()

    # Warm up price paths a little before broadcasting (no produce).
    for _ in range(min(WARMUP_TICKS, 200)):
        sim.advance(silent=True)

    producer = connect_producer()
    threading.Thread(target=command_listener, args=(sim,), daemon=True).start()

    tick = 0
    print(f"[ingestion] producing TickFrames to '{TOPIC_TICKS}' every {TICK_INTERVAL_SECONDS}s", flush=True)
    while True:
        prices, returns = sim.advance()
        frame = schemas.tick_frame(tick, _now(), prices, returns)
        producer.send(TOPIC_TICKS, frame)
        tick += 1
        if tick % 20 == 0:
            producer.flush()
        time.sleep(TICK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
