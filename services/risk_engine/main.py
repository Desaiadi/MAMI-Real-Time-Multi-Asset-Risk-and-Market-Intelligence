"""
MAMI Risk Engine Service
========================
Consumes market ticks and computes portfolio risk in real time.

  • Black-Scholes Greeks for every option position (CPU).
  • Monte Carlo portfolio VaR / CVaR — GPU when available, else CPU
    (see mami_core/compute.py). Recomputed every VAR_EVERY ticks.
  • Writes results to Redis (shared state) and publishes `risk.metrics`.

This is one of the horizontally-scalable compute services: add replicas in the
same consumer group and Kafka spreads tick partitions across them.
"""
import json
import os
import time

import redis
from kafka import KafkaConsumer, KafkaProducer

from mami_core.config import (
    PORTFOLIO, VOLATILITIES, RISK_FREE_RATE, MONTE_CARLO_PATHS, VAR_CONFIDENCE,
    KAFKA_BOOTSTRAP, TOPIC_TICKS, TOPIC_METRICS, REDIS_URL,
    RKEY_GREEKS, RKEY_PORTFOLIO, RKEY_VAR,
)
from mami_core import schemas
from mami_core.black_scholes import compute_greeks, scale_to_position
from mami_core.var_engine import portfolio_var
from mami_core.compute import backend_name

VAR_EVERY = int(os.getenv("VAR_EVERY", "10"))


def compute_metrics(prices: dict) -> tuple[dict, dict]:
    """Greeks for each position + portfolio aggregate, from current prices."""
    greeks, net_delta, total_theta, total_vega = {}, 0.0, 0.0, 0.0
    for pos in PORTFOLIO:
        block = prices.get(pos.symbol)
        if not block:
            continue
        S = block["price"]
        T = max(pos.expiry_days / 365.0, 1e-6)
        raw = compute_greeks(S, pos.strike, T, pos.implied_vol, RISK_FREE_RATE, pos.option_type)
        pg  = scale_to_position(raw, pos.contracts)
        net_delta   += pg["position_delta"]
        total_theta += pg["position_theta"]
        total_vega  += pg["position_vega"]
        greeks[pos.symbol] = {
            **pg, "option_type": pos.option_type, "strike": pos.strike,
            "contracts": pos.contracts, "stock_price": round(S, 2),
        }
    portfolio = {
        "net_delta": round(net_delta, 1),
        "total_theta": round(total_theta, 2),
        "total_vega": round(total_vega, 2),
    }
    return greeks, portfolio


def compute_var(prices: dict, greeks: dict) -> dict:
    positions = [
        {"price": prices[p.symbol]["price"], "vol": VOLATILITIES[p.symbol],
         "position_delta": greeks.get(p.symbol, {}).get("position_delta", 0)}
        for p in PORTFOLIO if p.symbol in prices
    ]
    return portfolio_var(positions, VAR_CONFIDENCE, MONTE_CARLO_PATHS, RISK_FREE_RATE)


def connect():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_TICKS, bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest", group_id="risk-engine",
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return r, consumer, producer
        except Exception as exc:
            print(f"[risk] waiting for Kafka: {exc}", flush=True)
            time.sleep(3)


def main():
    print(f"[risk] starting; compute backend = {backend_name()}", flush=True)
    r, consumer, producer = connect()
    print(f"[risk] consuming '{TOPIC_TICKS}', VaR every {VAR_EVERY} ticks", flush=True)

    last_var = {"portfolio_var": 0.0, "portfolio_cvar": 0.0, "confidence": VAR_CONFIDENCE}
    for msg in consumer:
        frame = msg.value
        tick, prices = frame["tick"], frame["prices"]

        greeks, portfolio = compute_metrics(prices)
        if tick % VAR_EVERY == 0:
            last_var = compute_var(prices, greeks)

        pipe = r.pipeline()
        pipe.set(RKEY_GREEKS, json.dumps(greeks))
        pipe.set(RKEY_PORTFOLIO, json.dumps(portfolio))
        pipe.set(RKEY_VAR, json.dumps(last_var))
        pipe.execute()

        producer.send(TOPIC_METRICS, schemas.metrics_msg(tick, greeks, portfolio, last_var))
        if tick % 50 == 0:
            print(f"[risk] tick {tick} | net_delta={portfolio['net_delta']} "
                  f"VaR={last_var['portfolio_var']}", flush=True)


if __name__ == "__main__":
    main()
