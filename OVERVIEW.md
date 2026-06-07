# MAMI — Project Overview

**MAMI** (Real-Time Multi-Asset Risk & Market Intelligence) is a self-contained,
runs-on-your-laptop demo of an institutional **derivatives risk platform**. It
simulates a live options-trading desk: a market data feed streams in, and the
system continuously re-prices an options portfolio, measures how much money it
could lose, and watches for abnormal market behavior — all in real time, pushed
to a live browser dashboard.

The infrastructure is faked (no cloud, no GPU, no Kafka), but the **quantitative
finance and machine-learning algorithms are the real, production-grade ones**.
The point of the project is to demonstrate the *math and the data flow* of a risk
platform without needing a data center.

---

## 1. The mental model

Imagine you run an options desk holding four bets:

| Position | Bet | Strike | Expiry | Contracts |
|---|---|---|---|---|
| **AAPL call** | Apple goes up | $185 | 30 days | 10 |
| **NVDA call** | NVIDIA goes up | $900 | 45 days | 5 |
| **TSLA put** | Tesla goes down (hedge) | $175 | 20 days | 8 |
| **SPY put** | Market-crash protection | $520 | 60 days | 15 |

Every half-second, prices move. The moment they do, three questions must be
answered *instantly*:

1. **How sensitive is my portfolio right now?** → the **Greeks** (Delta, Gamma, Vega, Theta)
2. **How much could I lose today?** → **Value at Risk (VaR)** and **Conditional VaR (CVaR)**
3. **Is something abnormal happening?** → **anomaly detection** (flash-crash / volatility-spike alerts)

MAMI answers all three on every tick and streams the results to a dashboard.

---

## 2. Architecture at a glance

```
                          ┌─────────────────────────────────────────────┐
                          │              SimulationEngine               │
                          │            (src/engine.py)                  │
                          │                                             │
   GBM price simulator ──▶│  prices ─┬─▶ Black-Scholes ─▶ Greeks        │
   (every 0.5s tick)      │          │   (black_scholes.py)             │
                          │          ├─▶ Monte Carlo VaR ─▶ VaR/CVaR    │
                          │          │   (var_engine.py)                │
                          │          └─▶ Isolation Forest + EWMA ─▶     │
                          │              (anomaly.py)        alerts      │
                          └───────────────────┬─────────────────────────┘
                                              │ snapshot() dict (JSON)
                                              ▼
                          ┌─────────────────────────────────────────────┐
                          │            FastAPI app (src/api.py)         │
                          │  • broadcast loop pushes state every 0.5s   │
                          │  • WebSocket  /ws/live                       │
                          │  • REST       /api/v1/*                      │
                          └───────────────────┬─────────────────────────┘
                                              │
                                              ▼
                          ┌─────────────────────────────────────────────┐
                          │     dashboard/index.html (single file)      │
                          │  live prices • Greeks • VaR • alerts panel  │
                          └─────────────────────────────────────────────┘
```

Everything runs **in one Python process**. State lives in memory (Python
`deque`s). There is no database, no message queue, and no network feed.

---

## 3. The data flow, tick by tick

Each "tick" is 0.5 seconds (2 ticks/second). On every tick the engine's
`step()` method (`src/engine.py:72`) does the following:

1. **`_advance_prices()`** — moves every symbol's price one step using
   **Geometric Brownian Motion** (the standard model for stock prices), with an
   occasional injected **flash crash** (a sudden large negative jump that decays
   over several ticks).
2. **`_run_ml()`** — feeds the latest price move into the per-symbol
   **Isolation Forest** anomaly detector and the **EWMA volatility forecaster**,
   and raises an **alert** if the anomaly score crosses a threshold.
3. **`_compute_greeks()`** — re-prices all four option positions with
   **Black-Scholes** and aggregates portfolio-level Delta/Theta/Vega.
4. **`_compute_var()`** — every 10th tick, runs a **5,000-path Monte Carlo**
   simulation to estimate portfolio VaR/CVaR (it's the expensive step, so it
   runs less often).
5. **`snapshot()`** — packages all of the above into one JSON-serializable dict.

The FastAPI broadcast loop (`src/api.py:51`) calls `step()` on a timer and pushes
the snapshot to every connected WebSocket client. The dashboard receives it and
redraws.

**Warm-up:** before serving any data, the engine runs 200 silent ticks
(`WARMUP_TICKS`) so price history and the ML models have enough data to be
meaningful from the first frame.

---

## 4. The algorithms (the real part)

### Black-Scholes Greeks — `src/black_scholes.py`
The 1973 Nobel-winning option-pricing formula, implemented exactly with
NumPy + SciPy. From five inputs (spot price `S`, strike `K`, time-to-expiry `T`,
implied volatility `σ`, risk-free rate `r`) it computes:

- **Delta** — $ change in the option per $1 move in the stock
- **Gamma** — how fast Delta itself changes
- **Vega** — sensitivity to a 1% change in implied volatility
- **Theta** — daily time decay (what the option loses just from a day passing)

`scale_to_position()` then scales these per-share Greeks up to the full position
(contracts × 100 shares). Edge cases (expiry, deep in/out-of-the-money) are
handled explicitly.

### Monte Carlo VaR / CVaR — `src/var_engine.py`
- **`historical_var()`** — replays actual past returns to find the loss
  threshold at a confidence level (e.g. the 5th-percentile loss for 95% VaR).
- **`monte_carlo_var()`** — simulates thousands of possible end-of-day prices
  with GBM and measures the loss distribution. **VaR** is the threshold loss at
  the confidence level; **CVaR** (a.k.a. Expected Shortfall, required under Basel
  IV) is the *average* loss in the tail *beyond* that threshold — i.e. "how bad
  do the bad days get."
- **`portfolio_var()`** — aggregates simulated P&L across all positions to get a
  single portfolio-level VaR/CVaR (simplified to assume independence between
  assets).

### Anomaly detection & volatility forecasting — `src/anomaly.py`
- **`AnomalyDetector`** — a scikit-learn **Isolation Forest** per symbol. It's
  *unsupervised*: it learns what "normal" market microstructure looks like
  (price velocity, volume z-score, bid-ask spread ratio) and scores how much an
  outlier the current tick is, on a 0→1 scale. It re-trains periodically to adapt
  to new regimes. Scores ≥ 0.60 (`ANOMALY_THRESHOLD`) raise an alert.
- **`VolatilityForecaster`** — an **EWMA** (exponentially weighted moving
  average) variance model using the RiskMetrics λ=0.94 standard. It compares
  current volatility to a rolling baseline and outputs a **spike probability**
  and a status (`NORMAL` / `ELEVATED` / `SPIKE`).

When an alert fires, the engine classifies it as `FLASH_CRASH`, `VOL_SPIKE`, or
generic `ANOMALY`, assigns a severity, and adds it to a rolling alert feed
(de-duplicated so one event doesn't spam the panel).

---

## 5. The web layer — `src/api.py`

A single **FastAPI** app exposes:

| Method | Endpoint | What it returns |
|---|---|---|
| GET | `/` | The dashboard HTML |
| GET | `/docs` | Auto-generated interactive API docs (Swagger) |
| WS  | `/ws/live` | Real-time JSON state stream, pushed every 0.5s |
| GET | `/api/v1/snapshot` | The full current state in one call |
| GET | `/api/v1/greeks/{symbol}` | Greeks for one position |
| GET | `/api/v1/var` | Portfolio VaR / CVaR + aggregate Greeks |
| GET | `/api/v1/alerts` | Recent anomaly alerts |
| GET | `/api/v1/history/{symbol}` | Price + return history for a symbol |
| POST | `/api/v1/trigger-crash` | **Demo button:** inject a flash crash |

The `trigger-crash` endpoint is the showpiece: hitting it (or the ⚡ button on
the dashboard) forces a flash crash on a symbol, and you can watch the Isolation
Forest score spike and an alert appear within a second or two.

---

## 6. The dashboard — `dashboard/index.html`

A single self-contained HTML file (no build step). On load it opens a WebSocket
to `/ws/live` and renders, in real time: live prices with bid/ask/spread, the
per-position and portfolio Greeks, the VaR/CVaR figures, and the scrolling
anomaly-alerts panel — plus the ⚡ flash-crash trigger.

---

## 7. Local demo vs. production

The README frames MAMI as a local stand-in for a much larger production system.
The **algorithms are identical**; only the **infrastructure** is swapped out:

| Production component | Local equivalent in this repo |
|---|---|
| FIX / WebSocket market-data feed | Geometric Brownian Motion price simulator |
| Apache Kafka | In-process asyncio event loop |
| Spark Structured Streaming | In-process tick aggregation |
| Delta Lake / Databricks | In-memory `deque` per symbol |
| RAPIDS / cuDF (GPU compute) | NumPy (CPU) |
| Kubernetes microservices | A single FastAPI process |
| LSTM volatility model | EWMA volatility forecaster |

---

## 8. Project layout

```
mami/
├── run.py                 ← Entry point: starts the uvicorn server on :8000
├── run.sh                 ← Convenience launcher (creates venv, installs, runs)
├── requirements.txt       ← Runtime dependencies
├── Makefile               ← `make install` / `make run` / `make clean`
├── docker-compose.yml     ← OPTIONAL real Kafka+Redis (not needed for the demo)
├── setup.cfg              ← pytest config
├── src/
│   ├── config.py          ← Portfolio, prices, vols, and simulation settings
│   ├── black_scholes.py   ← Delta / Gamma / Vega / Theta
│   ├── var_engine.py      ← Historical + Monte Carlo VaR / CVaR
│   ├── anomaly.py         ← Isolation Forest + EWMA forecaster
│   ├── engine.py          ← Simulation orchestrator (the heart of MAMI)
│   └── api.py             ← FastAPI app + WebSocket broadcast loop
├── dashboard/
│   └── index.html         ← Live risk dashboard (single file)
└── tests/
    ├── test_greeks.py     ← Black-Scholes Greeks tests
    └── test_var.py        ← VaR / CVaR tests
```

**Tuning knobs** all live in `src/config.py`: the portfolio positions, initial
prices, volatilities, risk-free rate, VaR confidence level, Monte Carlo path
count, tick interval, and the anomaly/flash-crash thresholds.

---

## 9. How to run it

Requires **Python 3.10+**.

```bash
# from the mami/ directory
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python run.py
```

Then open **http://localhost:8000** for the dashboard, or
**http://localhost:8000/docs** for the API explorer. (`./run.sh` does all of the
above in one command.)

Trigger the demo flash crash from the terminal:

```bash
curl -X POST "http://localhost:8000/api/v1/trigger-crash"           # random symbol
curl -X POST "http://localhost:8000/api/v1/trigger-crash?symbol=NVDA" # specific one
```

Run the test suite:

```bash
pip install pytest
pytest        # 32 tests covering the Greeks and VaR/CVaR math
```

---

## 10. A note on the codebase history

This repo originally shipped **two overlapping implementations** of MAMI (a flat
`src/*.py` layout and a nested `src/api/`, `src/risk/`, `src/ml/` package
layout) that had drifted apart and conflicted — `python run.py` loaded an empty
package and crashed. It has since been consolidated down to the **single flat
implementation** documented here, which the dashboard, README, and `run.py` all
target. The test suite was repointed to match, and the server, WebSocket stream,
flash-crash demo, and all 32 tests are verified working.
