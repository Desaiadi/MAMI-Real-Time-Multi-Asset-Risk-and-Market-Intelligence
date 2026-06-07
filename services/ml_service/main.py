"""
MAMI ML Service
===============
Consumes the tick stream and runs the anomaly / forecasting layer.

  • Isolation Forest anomaly score per symbol (sklearn).
  • EWMA volatility forecaster per symbol.
  • Emits alerts (FLASH_CRASH / VOL_SPIKE / ANOMALY) to Redis + `ml.alerts`.

This is a stateful stream processor: it keeps a rolling return/volume buffer per
symbol in memory as it consumes. (In production that state would live in a
stream-processing framework's managed state or in Redis; that's a later step.)
"""
import json
import time
from collections import deque
from datetime import datetime, timezone

import numpy as np
import redis
from kafka import KafkaConsumer, KafkaProducer

from mami_core.config import (
    INITIAL_PRICES, ANOMALY_THRESHOLD, KAFKA_BOOTSTRAP, TOPIC_TICKS, TOPIC_ALERTS,
    REDIS_URL, RKEY_SCORES, RKEY_FORECASTS, RKEY_ALERTS,
)
from mami_core import schemas
from mami_core.anomaly import AnomalyDetector, VolatilityForecaster

ALERTS_CAP = 50


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MLState:
    def __init__(self):
        self.ret_hist = {s: deque(maxlen=300) for s in INITIAL_PRICES}
        self.vol_hist = {s: deque(maxlen=120) for s in INITIAL_PRICES}
        self.detectors = {s: AnomalyDetector() for s in INITIAL_PRICES}
        self.forecaster = VolatilityForecaster()

    def process(self, tick: int, returns: dict) -> tuple[dict, dict, list]:
        scores, forecasts, alerts = {}, {}, []
        for sym in INITIAL_PRICES:
            if sym not in returns:
                continue
            self.ret_hist[sym].append(returns[sym])
            self.vol_hist[sym].append(int(np.random.randint(200, 6000)))
            rets = list(self.ret_hist[sym])
            vols = list(self.vol_hist[sym])
            if len(rets) < 15:
                continue

            price_vel = rets[-1] * 100
            if len(vols) >= 10:
                v_mean, v_std = np.mean(vols), np.std(vols) + 1e-6
                vol_z = (vols[-1] - v_mean) / v_std
            else:
                vol_z = 0.0
            spread_ratio = 1.0 + abs(rets[-1]) * 80

            score = self.detectors[sym].score(price_vel, vol_z, spread_ratio)
            fc = self.forecaster.update(sym, rets)
            scores[sym] = score
            forecasts[sym] = fc

            if score >= ANOMALY_THRESHOLD:
                alert_type = ("FLASH_CRASH" if price_vel < -1.5
                              else "VOL_SPIKE" if fc.get("status") == "SPIKE"
                              else "ANOMALY")
                alerts.append({
                    "id": f"{sym}_{tick}", "symbol": sym, "timestamp": _now(),
                    "type": alert_type, "severity": "HIGH" if score > 0.75 else "MEDIUM",
                    "score": score, "change_pct": round(price_vel, 4),
                    "spike_prob": fc.get("spike_probability", 0.0),
                })
        return scores, forecasts, alerts


def connect():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_TICKS, bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest", group_id="ml-service",
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return r, consumer, producer
        except Exception as exc:
            print(f"[ml] waiting for Kafka: {exc}", flush=True)
            time.sleep(3)


def main():
    print("[ml] starting", flush=True)
    r, consumer, producer = connect()
    state = MLState()
    print(f"[ml] consuming '{TOPIC_TICKS}'", flush=True)

    for msg in consumer:
        frame = msg.value
        tick = frame["tick"]
        scores, forecasts, alerts = state.process(tick, frame["returns"])

        pipe = r.pipeline()
        if scores:
            pipe.hset(RKEY_SCORES, mapping={k: v for k, v in scores.items()})
        if forecasts:
            pipe.set(RKEY_FORECASTS, json.dumps(forecasts))
        for a in alerts:
            # Dedup: max 2 alerts per symbol in the most recent 10.
            recent = [json.loads(x)["symbol"] for x in r.lrange(RKEY_ALERTS, 0, 9)]
            if recent.count(a["symbol"]) < 2:
                pipe.lpush(RKEY_ALERTS, json.dumps(a))
        pipe.ltrim(RKEY_ALERTS, 0, ALERTS_CAP - 1)
        pipe.execute()

        if alerts:
            producer.send(TOPIC_ALERTS, schemas.alert_msg(tick, scores, forecasts, alerts))
            for a in alerts:
                print(f"[ml] ALERT {a['type']} {a['symbol']} score={a['score']}", flush=True)


if __name__ == "__main__":
    main()
