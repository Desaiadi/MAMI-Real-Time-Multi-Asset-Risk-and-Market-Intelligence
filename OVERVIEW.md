# MAMI — Project Overview

**MAMI** (Real-Time Multi-Asset Risk & Market Intelligence) is a real-time
institutional **derivatives risk platform**. It simulates a live options-trading
desk: a market data feed streams in, and the system continuously re-prices an
options portfolio, measures how much money it could lose, and watches for
abnormal market behavior — all in real time, pushed to a live browser dashboard.

It runs as a **distributed microservice system** — Apache Kafka as the event
bus, Redis as shared state, and four independent services — orchestrated by
Docker Compose so the whole thing comes up on a laptop with one command. The
market feed is simulated, but the **quantitative finance and machine-learning
algorithms are the real, production-grade ones**.

> This is the narrative "what it does" doc. For the run guide see
> [ARCHITECTURE.md](ARCHITECTURE.md); for the full engineering spec (HLD/LLD,
> data models, sequence diagrams) see [DESIGN.md](DESIGN.md).

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

The work is split across four independent services that communicate only through
a **Kafka** event bus and a shared **Redis** state store — no service calls
another directly.

```
   ┌───────────┐   market.ticks   ┌────────────┐
   │ ingestion │─────────┬───────▶│ risk-engine│  Greeks + Monte Carlo VaR
   │  GBM feed │         │        └─────┬──────┘
   └─────▲─────┘         │              │ writes
         │               │        ┌─────▼──────┐
         │               └───────▶│ ml-service │  Isolation Forest + EWMA
         │ market.commands        └─────┬──────┘
         │                              │ writes
   ┌─────┴────────┐   reads/writes  ┌───▼──────────────────────────┐
   │ api-gateway  │◀───────────────▶│            REDIS             │
   │ FastAPI/WS   │                 │  prices, greeks, var,        │
   └─────┬────────┘                 │  scores, forecasts, alerts   │
         │ WebSocket + REST         └──────────────────────────────┘
         ▼
   dashboard/index.html  ·  live prices • Greeks • VaR • alerts
```

**7 containers:** Zookeeper + Kafka (bus), Redis (state), and the four services.
Each runs as its own process, restarts independently, and can be scaled by adding
replicas. State lives in Redis, so no single process holds anything unrecoverable.

---

## 3. The data flow, tick by tick

Each "tick" is 0.5 seconds (2 ticks/second). A tick flows through the system
like this:

1. **ingestion** advances every symbol's price one step using **Geometric
   Brownian Motion** (the standard model for stock prices), occasionally
   injecting a **flash crash**, and publishes a *TickFrame* to `market.ticks`.
2. **risk-engine** consumes the frame, re-prices all four positions with
   **Black-Scholes**, aggregates portfolio Delta/Theta/Vega, and — every 10th
   tick — runs a **5,000-path Monte Carlo** VaR/CVaR (the expensive step, so it
   runs less often). Results go to Redis.
3. **ml-service** consumes the same frame, feeds the price move into the
   per-symbol **Isolation Forest** detector and the **EWMA forecaster**, and
   raises an **alert** if the anomaly score crosses a threshold. Results go to
   Redis.
4. **api-gateway** consumes the frame too, writes the latest prices + history to
   Redis, then **reassembles the full state snapshot** from Redis and pushes it
   to every connected WebSocket client. The dashboard redraws.

Because the three consumers read the same topic independently, they all see every
tick — and any of them can fall behind or restart without blocking the others
(Kafka buffers).

---

## 4. The algorithms (the real part)

All algorithms live in the shared [`mami_core`](mami_core/) library — one source
of truth used by every service.

### Black-Scholes Greeks — `mami_core/black_scholes.py`
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

### Monte Carlo VaR / CVaR — `mami_core/var_engine.py`
- **`historical_var()`** — replays actual past returns to find the loss
  threshold at a confidence level (e.g. the 5th-percentile loss for 95% VaR).
- **`monte_carlo_var()`** — simulates thousands of possible end-of-day prices
  with GBM and measures the loss distribution. **VaR** is the threshold loss at
  the confidence level; **CVaR** (a.k.a. Expected Shortfall, required under Basel
  IV) is the *average* loss in the tail *beyond* that threshold — i.e. "how bad
  do the bad days get." This is the heavy kernel and runs on a **GPU** (CuPy)
  when one is available, or NumPy otherwise (see §7).
- **`portfolio_var()`** — aggregates simulated P&L across all positions to get a
  single portfolio-level VaR/CVaR (simplified to assume independence between
  assets).

### Anomaly detection & volatility forecasting — `mami_core/anomaly.py`
- **`AnomalyDetector`** — a scikit-learn **Isolation Forest** per symbol. It's
  *unsupervised*: it learns what "normal" market microstructure looks like
  (price velocity, volume z-score, bid-ask spread ratio) and scores how much an
  outlier the current tick is, on a 0→1 scale. It re-trains periodically to adapt
  to new regimes. Scores ≥ 0.60 (`ANOMALY_THRESHOLD`) raise an alert.
- **`VolatilityForecaster`** — an **EWMA** (exponentially weighted moving
  average) variance model using the RiskMetrics λ=0.94 standard. It compares
  current volatility to a rolling baseline and outputs a **spike probability**
  and a status (`NORMAL` / `ELEVATED` / `SPIKE`).

When an alert fires, the ml-service classifies it as `FLASH_CRASH`, `VOL_SPIKE`,
or generic `ANOMALY`, assigns a severity, and adds it to a rolling alert feed
(de-duplicated so one event doesn't spam the panel).

---

## 5. The edge layer — api-gateway

The **api-gateway** is a **FastAPI** service that owns no risk math. It tails the
bus, reads shared state from Redis, and serves clients:

| Method | Endpoint | What it returns |
|---|---|---|
| GET | `/` | The dashboard HTML |
| GET | `/docs` | Auto-generated interactive API docs (Swagger) |
| WS  | `/ws/live` | Real-time JSON state stream, pushed every tick |
| GET | `/api/v1/snapshot` | The full current state in one call |
| GET | `/api/v1/greeks/{symbol}` | Greeks for one position |
| GET | `/api/v1/var` | Portfolio VaR / CVaR + aggregate Greeks |
| GET | `/api/v1/alerts` | Recent anomaly alerts |
| GET | `/api/v1/history/{symbol}` | Price history for a symbol |
| POST | `/api/v1/trigger-crash` | **Demo button:** inject a flash crash |

The `trigger-crash` endpoint is the showpiece — and a nice demonstration of the
architecture. Hitting it (or the ⚡ button on the dashboard) publishes a command
to `market.commands`; the **ingestion** service consumes it and applies the
crash; the price drops; **ml-service** detects it and raises a `FLASH_CRASH`
alert — a full round-trip across the bus, visible on the dashboard within a
second or two.

---

## 6. The dashboard — `dashboard/index.html`

A single self-contained HTML file (no build step). On load it opens a WebSocket
to `/ws/live` and renders, in real time: live prices with bid/ask/spread, the
per-position and portfolio Greeks, the VaR/CVaR figures, and the scrolling
anomaly-alerts panel — plus the ⚡ flash-crash trigger.

---

## 7. What maps to what (vs. a production platform)

The **algorithms are identical** to a production desk; the infrastructure here is
real but laptop-scale, and the market feed is simulated:

| Production component | In this project |
|---|---|
| FIX / WebSocket market-data feed | Geometric Brownian Motion price simulator (ingestion) |
| Apache Kafka | **Apache Kafka** (real) |
| Spark Structured Streaming | Stateful stream consumers (ml-service) |
| Delta Lake / Databricks | Redis shared state |
| RAPIDS / cuDF (GPU compute) | **CuPy on GPU when available, else NumPy** |
| Kubernetes | Docker Compose (K8s is the documented next step) |
| LSTM volatility model | EWMA volatility forecaster |

The Monte Carlo VaR kernel is written against a backend abstraction
([`mami_core/compute.py`](mami_core/compute.py)): it runs on a GPU via CuPy if
one is present and **transparently falls back to NumPy** otherwise — GPU is
opt-in, never required.

---

## 8. Project layout

```
mami/
├── docker-compose.yml      ← 7-container topology
├── mami_core/              ← shared library (single source of truth)
│   ├── config.py           ← portfolio, prices, vols, infra settings
│   ├── compute.py          ← GPU/CPU array backend (CuPy → NumPy)
│   ├── black_scholes.py    ← Delta / Gamma / Vega / Theta
│   ├── var_engine.py       ← Historical + Monte Carlo VaR / CVaR
│   ├── anomaly.py          ← Isolation Forest + EWMA forecaster
│   └── schemas.py          ← Kafka message contracts
├── services/
│   ├── ingestion/          ← GBM market-data producer + command consumer
│   ├── risk_engine/        ← Greeks + Monte Carlo VaR
│   ├── ml_service/         ← anomaly detection + forecasting
│   └── api_gateway/        ← FastAPI: REST + WebSocket + dashboard
├── dashboard/index.html    ← live risk dashboard (single file)
└── tests/                  ← Greeks + VaR/CVaR unit tests
```

**Tuning knobs** all live in [`mami_core/config.py`](mami_core/config.py): the
portfolio positions, initial prices, volatilities, risk-free rate, VaR confidence
level, Monte Carlo path count, tick interval, and anomaly/flash-crash thresholds.
Infrastructure settings (Kafka/Redis addresses, topic names) are environment
variables wired in `docker-compose.yml`.

---

## 9. How to run it

```bash
docker compose up --build
```

Then open **http://localhost:8000** for the dashboard, or
**http://localhost:8000/docs** for the API explorer.

Trigger the demo flash crash from the terminal:

```bash
curl -X POST "http://localhost:8000/api/v1/trigger-crash"            # random symbol
curl -X POST "http://localhost:8000/api/v1/trigger-crash?symbol=NVDA" # specific one
```

Run the unit tests (no Docker — they exercise the shared `mami_core` algorithms):

```bash
pip install -r requirements.txt
pytest        # 32 tests covering the Greeks and VaR/CVaR math
```

Stop the stack with `docker compose down` (add `-v` to wipe Kafka/Redis volumes).
