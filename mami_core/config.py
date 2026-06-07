"""
MAMI shared configuration.

Portfolio + market parameters are the same domain settings as the original
single-process demo. Infrastructure settings (Kafka, Redis, topics) are added
here and driven by environment variables so every service reads the same names.
"""
import os
from dataclasses import dataclass
from typing import Dict, List


# ── Domain: Portfolio ───────────────────────────────────────────────────────────
@dataclass
class Position:
    """A single option position in the portfolio."""
    symbol: str
    option_type: str   # 'call' or 'put'
    strike: float
    expiry_days: int
    contracts: int     # 1 contract = 100 shares
    implied_vol: float


PORTFOLIO: List[Position] = [
    Position("AAPL", "call", 185.00, 30, 10, 0.28),
    Position("NVDA", "call", 900.00, 45,  5, 0.55),
    Position("TSLA", "put",  175.00, 20,  8, 0.65),
    Position("SPY",  "put",  520.00, 60, 15, 0.16),
]

INITIAL_PRICES: Dict[str, float] = {
    "AAPL": 185.50, "NVDA": 884.60, "TSLA": 177.20, "SPY": 523.80, "QQQ": 452.30,
}

VOLATILITIES: Dict[str, float] = {
    "AAPL": 0.28, "NVDA": 0.55, "TSLA": 0.65, "SPY": 0.16, "QQQ": 0.22,
}

# ── Domain: Risk model ──────────────────────────────────────────────────────────
RISK_FREE_RATE     = 0.053
VAR_CONFIDENCE     = 0.95
MONTE_CARLO_PATHS  = int(os.getenv("MONTE_CARLO_PATHS", "5000"))
SHARES_PER_CONTRACT = 100

# ── Domain: Simulation ──────────────────────────────────────────────────────────
HISTORY_SIZE          = 300
TICK_INTERVAL_SECONDS = float(os.getenv("TICK_INTERVAL_SECONDS", "0.5"))
WARMUP_TICKS          = int(os.getenv("WARMUP_TICKS", "200"))
ANOMALY_THRESHOLD     = 0.60
FLASH_CRASH_PROB      = 0.003

# ── Infrastructure: Kafka ───────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Topic names — partition by symbol where it makes sense (config later).
TOPIC_TICKS    = os.getenv("TOPIC_TICKS",    "market.ticks")      # ingestion -> risk/ml/gateway
TOPIC_METRICS  = os.getenv("TOPIC_METRICS",  "risk.metrics")      # risk -> gateway
TOPIC_ALERTS   = os.getenv("TOPIC_ALERTS",   "ml.alerts")         # ml  -> gateway
TOPIC_COMMANDS = os.getenv("TOPIC_COMMANDS", "market.commands")   # gateway -> ingestion

# ── Infrastructure: Redis (externalized state) ──────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Redis keys — the shared snapshot is assembled from these.
RKEY_PRICES    = "mami:prices"      # hash  symbol -> json price block
RKEY_HISTORY   = "mami:history:"    # list  prefix + symbol -> capped price history
RKEY_GREEKS    = "mami:greeks"      # string json {symbol: greeks}
RKEY_PORTFOLIO = "mami:portfolio"   # string json portfolio aggregate
RKEY_VAR       = "mami:var"         # string json VaR/CVaR
RKEY_SCORES    = "mami:scores"      # hash  symbol -> anomaly score
RKEY_FORECASTS = "mami:forecasts"   # string json {symbol: forecast}
RKEY_ALERTS    = "mami:alerts"      # list  json alerts (capped)
RKEY_TICK      = "mami:tick"        # string latest tick number
