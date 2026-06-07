# MAMI — Technical Design Document

**Real-Time Multi-Asset Risk & Market Intelligence**

This is the authoritative engineering reference for MAMI: high-level design
(HLD), low-level design (LLD), architecture, data/message models, algorithms,
APIs, and operational concerns. For a narrative "what it does" introduction see
[OVERVIEW.md](OVERVIEW.md); for the distributed run guide see
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## Table of contents

1. [Purpose & scope](#1-purpose--scope)
2. [System overview](#2-system-overview)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
4. [Architecture](#4-architecture)
5. [Low-Level Design (LLD)](#5-low-level-design-lld)
6. [Data models & contracts](#6-data-models--contracts)
7. [API reference](#7-api-reference)
8. [Algorithms in depth](#8-algorithms-in-depth)
9. [Cross-cutting concerns](#9-cross-cutting-concerns)
10. [Scalability & performance](#10-scalability--performance)
11. [Failure modes & recovery](#11-failure-modes--recovery)
12. [Testing strategy](#12-testing-strategy)
13. [Build & deployment](#13-build--deployment)
14. [Sequence diagrams](#14-sequence-diagrams)
15. [Production gap & roadmap](#15-production-gap--roadmap)
16. [Glossary](#16-glossary)

---

## 1. Purpose & scope

MAMI demonstrates the data flow and quantitative core of an institutional
**derivatives risk platform**: ingest a live market feed, continuously re-price
an options portfolio, quantify potential loss, and detect abnormal market
behavior — pushing results to a live dashboard in real time.

**In scope:** the real risk/ML algorithms (Black-Scholes, VaR/CVaR, Isolation
Forest, EWMA), a distributed microservice architecture on a real message bus and
state store, and a GPU-capable compute path.

**Out of scope (simulated):** a real exchange feed (a GBM simulator stands in),
and the full production data-engineering stack (Spark/Delta/K8s) — see
[§15](#15-production-gap--roadmap).

---

## 2. System overview

### 2.1 Problem statement

An options desk holds positions whose value and risk change continuously as
underlying prices move. Three questions must be answered on every price change,
within milliseconds:

1. **Sensitivity** — how does portfolio value move with the market? → **Greeks**
2. **Loss exposure** — how much could we lose over a horizon? → **VaR / CVaR**
3. **Abnormality** — is the market behaving unusually (flash crash, vol spike)? → **anomaly detection**

### 2.2 The reference portfolio

Defined in [`mami_core/config.py`](mami_core/config.py):

| Symbol | Type | Strike | Expiry (days) | Contracts | Implied vol |
|---|---|---|---|---|---|
| AAPL | call | 185.00 | 30 | 10 | 0.28 |
| NVDA | call | 900.00 | 45 | 5 | 0.55 |
| TSLA | put | 175.00 | 20 | 8 | 0.65 |
| SPY | put | 520.00 | 60 | 15 | 0.16 |

A fifth symbol, **QQQ**, is priced/streamed but has no option position (it
exercises the "tracked but unhedged" path).

### 2.3 Goals & non-goals

| Goals | Non-goals |
|---|---|
| Correct, production-grade quant math | Real exchange connectivity |
| Genuinely distributed, fault-tolerant architecture | Sub-millisecond latency SLAs |
| Horizontal scalability of compute | Multi-region / HA Kafka |
| GPU-capable, CPU-default | Authn/authz, multi-tenant |
| Runnable on a laptop via Docker Compose | Kubernetes (deferred) |

---

## 3. High-Level Design (HLD)

### 3.1 Context diagram

```
        ┌────────────┐         HTTP / WebSocket          ┌─────────────────────┐
        │  Analyst   │◀─────────────────────────────────▶│       MAMI          │
        │ (browser)  │   dashboard, REST, live stream     │   risk platform     │
        └────────────┘                                    └─────────────────────┘
                                                                    │
                                          (production: FIX feed)    │ simulated
                                                                    ▼
                                                          ┌─────────────────────┐
                                                          │  Market data (GBM)  │
                                                          └─────────────────────┘
```

Single human actor (the analyst) and one simulated external system (the market
feed). No external dependencies beyond the bundled infrastructure.

### 3.2 Two deployment modes

MAMI ships in two topologies sharing one algorithm library (`mami_core`):

| | **Lite** | **Distributed** |
|---|---|---|
| Process model | 1 process | 7 containers |
| Bus | in-process asyncio | Apache Kafka |
| State | in-memory deques | Redis |
| Entry | `python run.py` | `docker compose up --build` |
| Use | dev, algorithm work | the real architecture |
| Code | [`src/`](src/) | [`services/`](services/) |

This document focuses on the **distributed** mode; the lite mode's
`SimulationEngine` is covered in [§5.3](#53-lite-mode-simulationengine).

### 3.3 Component inventory (distributed)

| Component | Type | Responsibility |
|---|---|---|
| **ingestion** | Python service | Generate market ticks; apply crash commands |
| **risk-engine** | Python service | Greeks + Monte Carlo VaR/CVaR |
| **ml-service** | Python service | Anomaly scoring + volatility forecasting + alerts |
| **api-gateway** | Python service (FastAPI) | Edge: REST, WebSocket, dashboard, command routing |
| **Kafka** | Infrastructure | Event bus (4 topics) |
| **Zookeeper** | Infrastructure | Kafka coordination |
| **Redis** | Infrastructure | Shared state store |
| **mami_core** | Library | Shared config, algorithms, compute backend, schemas |

### 3.4 Technology choices & rationale

| Choice | Why |
|---|---|
| **Kafka** | Durable, partitioned, replayable event bus; decouples producers/consumers; backpressure buffering |
| **Redis** | Low-latency shared state; the gateway assembles snapshots from it; survives any app-process restart |
| **FastAPI + uvicorn** | Async-native, WebSocket support, auto-generated OpenAPI docs |
| **kafka-python** (sync services) | Pure-Python, simple blocking consumer loops |
| **aiokafka** (gateway) | Async Kafka client that integrates with FastAPI's event loop |
| **NumPy / SciPy / scikit-learn** | The real quant/ML algorithms; CPU baseline |
| **CuPy** (optional) | Drop-in GPU array backend for the Monte Carlo kernel |
| **Docker Compose** | One-command local orchestration of the whole topology |

### 3.5 Quality attributes

| Attribute | How it is met |
|---|---|
| **Scalability** | Stateless-ish compute services scale by adding replicas in a Kafka consumer group; partitions rebalance automatically |
| **Fault tolerance** | State externalized to Redis; `restart: unless-stopped` on every service; Kafka buffers while a consumer is down |
| **Decoupling** | Services share no memory; communicate only via Kafka topics |
| **Performance** | Heavy VaR throttled (every N ticks) and GPU-offloadable |
| **Observability** | Structured stdout logs per service; `docker compose logs` |
| **Portability** | Runs identically on any Docker host; GPU is opt-in, never required |

---

## 4. Architecture

### 4.1 Distributed topology

```
                            ┌──────────────────────────────┐
                            │            KAFKA             │
                            │  market.ticks   risk.metrics │
                            │  ml.alerts      market.commands
                            └──────────────────────────────┘
   market.ticks ▲   │ ticks         │ ticks          ▲ commands  │ ticks
                │   ▼               ▼                │           ▼
   ┌───────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────┐
   │ ingestion │  │ risk-engine│  │ ml-service │  │ api-gateway  │
   │  GBM sim  │  │ Greeks+VaR │  │  IF + EWMA │  │ FastAPI/WS   │
   └─────┬─────┘  └─────┬──────┘  └─────┬──────┘  └──────┬───────┘
         │              │ write         │ write          │ read+write
         │              ▼               ▼                ▼
         │           ┌─────────────────────────────────────────┐
         │           │                 REDIS                    │
         │           └─────────────────────────────────────────┘
         │                                                  │
         └──────────── flash-crash command ◀──────── dashboard (browser)
```

### 4.2 Event bus design

| Topic | Producer | Consumers | Payload | Purpose |
|---|---|---|---|---|
| `market.ticks` | ingestion | risk-engine, ml-service, api-gateway | TickFrame | Per-tick market snapshot |
| `risk.metrics` | risk-engine | (gateway / future) | MetricsMsg | Computed Greeks + VaR |
| `ml.alerts` | ml-service | (gateway / future) | AlertMsg | Anomaly alerts |
| `market.commands` | api-gateway | ingestion | CommandMsg | Operator commands (crash) |

- **Delivery semantics:** at-least-once. Consumers use `auto_offset_reset=latest`
  (a live dashboard cares about *now*, not replaying history). Each logical
  consumer is its own `group_id` so all services see every tick independently.
- **Partitioning:** topics are auto-created with default partitions (1 in the
  slice). Scaling lever: increase `market.ticks` partitions and run multiple
  replicas per consumer group. (Caveat in [§10](#10-scalability--performance).)
- **Serialization:** JSON (one document per message) via
  [`mami_core/schemas.py`](mami_core/schemas.py).

### 4.3 State store design (Redis)

The gateway reassembles the canonical snapshot from these keys. Writers and
readers are decoupled — services never call each other directly.

| Key | Type | Writer | Reader | Contents |
|---|---|---|---|---|
| `mami:prices` | string(JSON) | api-gateway | api-gateway | `{sym: {price,bid,ask,spread,change_pct}}` |
| `mami:history:<SYM>` | list | api-gateway | api-gateway | capped price history (last 120) |
| `mami:greeks` | string(JSON) | risk-engine | api-gateway | `{sym: greeks block}` |
| `mami:portfolio` | string(JSON) | risk-engine | api-gateway | net delta/theta/vega |
| `mami:var` | string(JSON) | risk-engine | api-gateway | portfolio VaR/CVaR |
| `mami:scores` | hash | ml-service | api-gateway | `sym → anomaly score` |
| `mami:forecasts` | string(JSON) | ml-service | api-gateway | `{sym: forecast block}` |
| `mami:alerts` | list | ml-service | api-gateway | capped alert feed (last 50) |
| `mami:tick` | string | api-gateway | api-gateway | latest tick number |

Key names are constants in `mami_core/config.py` (`RKEY_*`) — single source of
truth shared by writers and readers.

### 4.4 Deployment view

| Container | Image | Ports (host:container) | Restart |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | — | default |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092:9092 | default |
| redis | redis:7-alpine | 6379:6379 | default |
| ingestion | mami-ingestion | — | unless-stopped |
| risk-engine | mami-risk-engine | — | unless-stopped |
| ml-service | mami-ml-service | — | unless-stopped |
| api-gateway | mami-api-gateway | 8000:8000 | unless-stopped |

**Networking:** Kafka uses dual listeners — `kafka:29092` (in-cluster, used by
services via `KAFKA_BOOTSTRAP`) and `localhost:9092` (host tools). Startup
ordering uses Compose healthchecks: services gate on `kafka`/`redis` being
`healthy`, and app code additionally retries the connection.

---

## 5. Low-Level Design (LLD)

### 5.1 `mami_core` library

The single source of truth for domain logic, imported by both modes.

```
mami_core/
├── config.py        # domain + infra settings (env-driven)
├── compute.py       # GPU/CPU array-backend abstraction
├── black_scholes.py # Greeks (CPU/SciPy)
├── var_engine.py    # VaR/CVaR (xp backend)
├── anomaly.py       # Isolation Forest + EWMA
└── schemas.py       # Kafka message contracts
```

#### 5.1.1 `config.py`

Domain constants and infrastructure settings, the latter read from environment
variables so all services agree on names. Key values:

| Constant | Value | Meaning |
|---|---|---|
| `RISK_FREE_RATE` | 0.053 | r used in pricing & GBM drift |
| `VAR_CONFIDENCE` | 0.95 | VaR confidence level |
| `MONTE_CARLO_PATHS` | 5000 (env) | VaR simulation paths |
| `SHARES_PER_CONTRACT` | 100 | Option contract multiplier |
| `TICK_INTERVAL_SECONDS` | 0.5 (env) | Simulated tick cadence |
| `WARMUP_TICKS` | 200 (env) | Silent ticks before streaming |
| `ANOMALY_THRESHOLD` | 0.60 | Score ≥ → raise alert |
| `FLASH_CRASH_PROB` | 0.003 | Spontaneous crash prob / tick |
| `HISTORY_SIZE` | 300 | Max in-memory history per symbol |

Also defines `PORTFOLIO`, `INITIAL_PRICES`, `VOLATILITIES`, all Kafka topic
names (`TOPIC_*`), and Redis key names (`RKEY_*`).

#### 5.1.2 `compute.py` — GPU/CPU abstraction

```python
xp              # CuPy if GPU+CuPy available, else NumPy
GPU_AVAILABLE   # bool
to_host(value)  # → Python float regardless of backend
backend_name()  # 'cupy (GPU)' | 'numpy (CPU)'
```

Selection logic at import:
1. If `MAMI_FORCE_CPU=1` → NumPy.
2. Else try `import cupy`; **touch a device op** (`cupy.zeros(1).sum()`) so a
   CUDA-less CuPy install doesn't masquerade as ready; on success → CuPy.
3. On any failure → NumPy.

`to_host` calls `.get()` on CuPy arrays (device→host copy) and `float()`
otherwise. Numeric code written against `xp` runs unchanged on either backend.

#### 5.1.3 `black_scholes.py`

```python
compute_greeks(S, K, T, sigma, r=0.053, option_type="call") -> dict
    # → {delta, gamma, vega, theta, price}
scale_to_position(greeks, contracts, shares_per_contract=100) -> dict
    # → adds {position_delta, position_theta, position_vega, option_price}
```

CPU/SciPy (Greeks are cheap; no GPU benefit). Handles the `T ≤ 1e-6` expiry edge
case explicitly (intrinsic value, delta ∈ {−1,0,1}, other Greeks 0). Math in
[§8.2](#82-black-scholes--the-greeks).

#### 5.1.4 `var_engine.py`

```python
historical_var(returns, position_value, confidence=0.95) -> (var, cvar)   # CPU, needs ≥20 obs
monte_carlo_var(S, sigma, r, position_delta, T=1/252, n_paths=5000, confidence=0.95) -> dict
    # → {var, cvar, best_case, worst_case, n_paths}   (xp backend)
portfolio_var(positions, confidence=0.95, n_paths=5000, r=0.053) -> dict
    # → {portfolio_var, portfolio_cvar, confidence}    (xp backend)
```

The two Monte Carlo functions run against `xp` (GPU/CPU). `portfolio_var`
aggregates per-position simulated P&L into one loss distribution (assumes
independence — conservative). Scalars converted home with `to_host`. Math in
[§8.3](#83-value-at-risk--cvar).

#### 5.1.5 `anomaly.py`

**`AnomalyDetector`** (per symbol): wraps `sklearn.ensemble.IsolationForest`
(`n_estimators=100`, `contamination=0.05`, `random_state=42`).

- Buffers feature vectors `[price_velocity, volume_zscore, spread_ratio]`.
- `warmup=100`: returns `0.0` until buffered; fits when buffer ≥ warmup.
- `refit_at=200`: re-fits periodically to adapt to regime shifts.
- Score: `−score_samples(x)` flipped, normalized `(raw − 0.30) / 0.40`, clipped
  to `[0,1]` — higher = more anomalous.

**`VolatilityForecaster`** (all symbols): EWMA variance, RiskMetrics
`lambda_=0.94`.

- Update: `var = λ·var + (1−λ)·r²`.
- Compares current vol to a 60-tick baseline → `vol_ratio`.
- Sigmoid `spike_probability` keyed on `vol_ratio` vs `spike_mul=2.5`.
- Annualizes with `ann_factor = sqrt(252·6.5·3600 / 0.5)`.
- Status: `SPIKE` (ratio > 2.5), `ELEVATED` (> 1.5), else `NORMAL`.

#### 5.1.6 `schemas.py`

Pure functions building/validating the four JSON message shapes plus
`encode`/`decode` helpers. The documented single source of the wire format — see
[§6.1](#61-kafka-message-contracts).

### 5.2 Services

All sync services follow the same skeleton: **connect-with-retry** (loop until
Kafka/Redis reachable) → **consume loop** (per-message processing) → **write
Redis + produce downstream**. Every service logs to stdout (`flush=True`).

#### 5.2.1 ingestion ([`services/ingestion/main.py`](services/ingestion/main.py))

| Aspect | Detail |
|---|---|
| Role | Market-data feed (producer) + command consumer |
| Threads | Main: produce ticks. Background daemon: consume `market.commands` |
| State | `MarketSimulator`: current prices, prev prices, per-symbol crash counters, lock |
| Per tick | `advance()` → GBM step per symbol (or crash shock) → build TickFrame → `produce(market.ticks)` → sleep `TICK_INTERVAL_SECONDS` |
| Crash | `trigger_crash(sym)` sets a 4–7 tick crash counter (thread-safe via lock); each crashed tick applies a −1.2%..−2.8% log shock |
| Warmup | Runs up to 200 silent `advance()` calls before producing |
| GBM dt | `TICK_INTERVAL_SECONDS / (252·6.5·3600)` |

#### 5.2.2 risk-engine ([`services/risk_engine/main.py`](services/risk_engine/main.py))

| Aspect | Detail |
|---|---|
| Role | Compute Greeks + portfolio VaR |
| Consumes | `market.ticks` (group `risk-engine`) |
| Per frame | `compute_metrics(prices)` → per-position Greeks + portfolio aggregate; **every `VAR_EVERY`=10 ticks** → `compute_var()` (5000-path Monte Carlo); write `mami:greeks/portfolio/var` (Redis pipeline); produce `risk.metrics` |
| Compute | Logs `backend_name()` at startup; VaR runs on `xp` (GPU/CPU) |
| Scaling | Stateless — replicas in group `risk-engine` share tick partitions |

#### 5.2.3 ml-service ([`services/ml_service/main.py`](services/ml_service/main.py))

| Aspect | Detail |
|---|---|
| Role | Anomaly scoring, volatility forecasting, alerting |
| Consumes | `market.ticks` (group `ml-service`) |
| State | `MLState`: per-symbol return + volume deques, per-symbol `AnomalyDetector`, one `VolatilityForecaster` (stateful stream processor) |
| Per frame | Feature-engineer (`price_vel`, `vol_z`, `spread_ratio`) → score → forecast → write `mami:scores/forecasts`; raise alert if score ≥ threshold |
| Alert dedup | Max 2 alerts per symbol within the most recent 10 in `mami:alerts`; list trimmed to 50 |
| Alert type | `FLASH_CRASH` (price_vel < −1.5), `VOL_SPIKE` (forecast status SPIKE), else `ANOMALY`; severity HIGH if score > 0.75 |

#### 5.2.4 api-gateway ([`services/api_gateway/main.py`](services/api_gateway/main.py))

| Aspect | Detail |
|---|---|
| Role | Edge: serve dashboard, REST, WebSocket; route commands; owns no math |
| Stack | FastAPI + uvicorn; `aiokafka` consumer/producer; `redis.asyncio` |
| Lifespan | On startup: connect-with-retry, then spawn `tick_loop()` task. On shutdown: cancel task, stop Kafka clients |
| `tick_loop` | Tail `market.ticks` → write `mami:prices`, append `mami:history:<sym>` (LTRIM 120), set `mami:tick` → assemble snapshot → broadcast to all WebSocket clients (drop dead sockets) |
| `assemble_snapshot` | Read all `RKEY_*` from Redis, merge into the canonical snapshot dict ([§6.3](#63-snapshot-schema-canonical-dashboard-contract)) |
| Commands | `POST /trigger-crash` → `send_and_wait(market.commands, …)` (round-trips to ingestion) |
| Clients | In-memory `set[WebSocket]`; snapshot sent on connect, then every tick |

### 5.3 Lite mode: `SimulationEngine`

[`src/engine.py`](src/engine.py) collapses all four services into one class for
no-Docker development. `step()` performs, in-process and in order:
`_advance_prices()` → `_run_ml()` → `_compute_greeks()` → `_compute_var()`
(every 10th tick) → `snapshot()`. [`src/api.py`](src/api.py) wraps it in FastAPI
with a 0.5s broadcast loop. It imports the **same** `mami_core` algorithms, so
behavior matches the distributed services.

---

## 6. Data models & contracts

### 6.1 Kafka message contracts

Built by [`mami_core/schemas.py`](mami_core/schemas.py). All JSON.

**TickFrame** → `market.ticks`
```json
{
  "tick": 128,
  "ts": "2026-06-07T12:00:00+00:00",
  "prices":  { "AAPL": {"price":185.5,"bid":185.46,"ask":185.54,"spread":0.08,"change_pct":0.01}, "...": {} },
  "returns": { "AAPL": 0.00012, "...": 0.0 }
}
```

**MetricsMsg** → `risk.metrics`
```json
{ "tick": 128, "greeks": {"AAPL": {...}}, "portfolio": {...}, "var": {...} }
```

**AlertMsg** → `ml.alerts`
```json
{ "tick": 128, "scores": {"NVDA": 0.86}, "forecasts": {"NVDA": {...}}, "alerts": [ {...} ] }
```

**CommandMsg** → `market.commands`
```json
{ "command": "trigger_crash", "symbol": "NVDA" }
```

### 6.2 Redis key catalog

See [§4.3](#43-state-store-design-redis).

### 6.3 Snapshot schema (canonical dashboard contract)

The single most important contract: the dashboard and all `/api/v1` readers
depend on this exact shape. Identical in lite and distributed modes.

```json
{
  "timestamp": "ISO-8601",
  "tick": 128,
  "prices": {
    "AAPL": {
      "price": 185.5, "bid": 185.46, "ask": 185.54, "spread": 0.08,
      "change_pct": 0.01, "history": [/* last 120 prices */],
      "anomaly_score": 0.0, "forecast": { "status": "NORMAL", "spike_probability": 0.0, "...": 0 }
    }
  },
  "greeks": {
    "AAPL": { "delta": .., "gamma": .., "vega": .., "theta": ..,
              "position_delta": .., "position_theta": .., "position_vega": ..,
              "option_type": "call", "strike": 185.0, "contracts": 10, "stock_price": 185.5 }
  },
  "portfolio": { "net_delta": .., "total_theta": .., "total_vega": .. },
  "var": { "portfolio_var": .., "portfolio_cvar": .., "confidence": 0.95 },
  "alerts": [ { "id","symbol","timestamp","type","severity","score","change_pct","spike_prob" } ]
}
```

---

## 7. API reference

Served by the api-gateway (distributed) or `src/api.py` (lite). Base path
`/api/v1`.

| Method | Endpoint | Description | Returns |
|---|---|---|---|
| GET | `/` | Dashboard HTML | text/html |
| GET | `/docs` | OpenAPI / Swagger UI | text/html |
| WS | `/ws/live` | Snapshot on connect, then a snapshot per tick (~0.5s) | Snapshot JSON |
| GET | `/api/v1/snapshot` | Full current state | Snapshot JSON |
| GET | `/api/v1/greeks/{symbol}` | Greeks for one position | Greeks block / 404 |
| GET | `/api/v1/var` | Portfolio aggregate + VaR/CVaR | `{net_delta,…,portfolio_var,portfolio_cvar,confidence}` |
| GET | `/api/v1/alerts` | Recent anomaly alerts | `[AlertMsg item]` |
| GET | `/api/v1/history/{symbol}` | Price history | `{symbol,prices,count}` |
| POST | `/api/v1/trigger-crash?symbol=SYM` | Inject a flash crash (omit `symbol` for random) | `{message,symbol}` |

---

## 8. Algorithms in depth

### 8.1 Geometric Brownian Motion (price simulation)

Each tick advances each price `S` by one GBM step over `dt` (one tick in
trading-year units):

```
shock = (r − ½σ²)·dt + σ·√dt·Z          where Z ~ N(0,1)
S_new = max(S · e^shock, 1.0)
```

A flash crash replaces the diffusion term with a large negative log shock
(`log(1 − U)`, U ∈ [1.2%, 2.8%] forced, or [1.8%, 4.0%] spontaneous) for 4–7
consecutive ticks.

### 8.2 Black-Scholes & the Greeks

With `d1 = [ln(S/K) + (r + ½σ²)T] / (σ√T)`, `d2 = d1 − σ√T`:

| Greek | Call | Put |
|---|---|---|
| **Delta** | `N(d1)` | `N(d1) − 1` |
| **Gamma** | `φ(d1) / (Sσ√T)` | same |
| **Vega** | `S·φ(d1)·√T / 100` (per 1% vol) | same |
| **Theta** | `−[S·φ(d1)·σ/(2√T) + rK·e^(−rT)·N(d2)] / 365` | put variant |
| **Price** | `S·N(d1) − K·e^(−rT)·N(d2)` | `K·e^(−rT)·N(−d2) − S·N(−d1)` |

Position Greeks scale by `contracts × 100` (delta/theta) or `contracts` (vega).

### 8.3 Value at Risk & CVaR

- **VaR(c):** the loss at the `(1−c)` percentile of the P&L distribution.
- **CVaR(c):** the mean of losses *beyond* VaR (Expected Shortfall, Basel IV).

**Monte Carlo (single position):** simulate `n_paths` GBM log-returns over
horizon `T=1/252`; convert to price changes; P&L via delta-linear
`PnL ≈ Δ_position × ΔS`; VaR = percentile, CVaR = tail mean.

**Portfolio:** sum simulated per-position P&L into `total_pnl` (independence
assumption), then percentile/tail mean. Positions with ~zero delta are skipped.

### 8.4 Isolation Forest (anomaly detection)

Unsupervised: isolates outliers via random partitioning — anomalies need fewer
splits to isolate. Features per tick: price velocity, volume z-score, spread
ratio. Raw `score_samples` is sign-flipped and normalized to `[0,1]`. Re-fit
every 200 ticks to track regime change. No labeled crash data required.

### 8.5 EWMA volatility forecasting

`var_t = λ·var_{t−1} + (1−λ)·r_t²` (λ=0.94). Spike probability is a logistic
function of the ratio of current vol to a 60-tick baseline. In production this
role is an LSTM; EWMA captures the same "is volatility regime-shifting" signal
cheaply.

---

## 9. Cross-cutting concerns

### 9.1 Configuration

All tunables are environment variables (defaults in `mami_core/config.py`,
overridden in `docker-compose.yml`): `KAFKA_BOOTSTRAP`, `REDIS_URL`,
`MONTE_CARLO_PATHS`, `VAR_EVERY`, `TICK_INTERVAL_SECONDS`, `WARMUP_TICKS`,
`MAMI_FORCE_CPU`.

### 9.2 Concurrency model

| Service | Model |
|---|---|
| ingestion | Main thread produces; daemon thread consumes commands; shared state guarded by a `threading.Lock` |
| risk-engine / ml-service | Single blocking consumer loop (one partition consumer per process) |
| api-gateway | Single asyncio event loop: `tick_loop` task + request handlers + WebSocket fan-out coexist cooperatively |

### 9.3 Consistency model

**Eventual consistency.** Risk and ML write their slices to Redis asynchronously;
the gateway's snapshot reflects the latest value of each slice, which may be from
slightly different ticks. Acceptable for a live risk dashboard (sub-second skew).
Strict cross-slice consistency would require a coordinating tick barrier — a
deliberate non-goal.

### 9.4 Compute backend selection

See [§5.1.2](#512-computepy--gpucpu-abstraction). The risk-engine logs the live
backend on startup. GPU is opt-in (add CuPy + NVIDIA runtime); CPU is the
default and always works.

---

## 10. Scalability & performance

| Lever | Mechanism |
|---|---|
| **More compute throughput** | `docker compose up --scale risk-engine=N`; Kafka rebalances `market.ticks` partitions across the group |
| **GPU acceleration** | Swap NumPy→CuPy for the Monte Carlo kernel (no code change) |
| **VaR cost control** | `VAR_EVERY` throttles the expensive step; `MONTE_CARLO_PATHS` trades accuracy for cost |
| **Ingest volume** | Kafka buffers; add partitions for parallel consumption |

**Known limitation (slice):** `market.ticks` carries *whole-market frames* and
topics default to 1 partition, so scaling consumers past 1 partition needs a
partition increase first. Portfolio VaR needs all symbols together, so naive
per-symbol partitioning would split the portfolio — the documented next step is
per-symbol partitioning **plus** a portfolio aggregator ([§15](#15-production-gap--roadmap)).

**Bottlenecks, in order:** (1) Monte Carlo VaR (CPU) → GPU offload; (2) ml-service
stateful per-symbol models → partition + externalize state; (3) gateway WebSocket
fan-out → pub/sub layer for many consumers.

---

## 11. Failure modes & recovery

| Failure | Behavior | Recovery |
|---|---|---|
| A compute service crashes | Kafka retains offsets; state remains in Redis | `restart: unless-stopped` relaunches; consumer resumes from last committed offset |
| Kafka not ready at startup | Services log "waiting for Kafka" | App-level retry loop + Compose `condition: service_healthy` |
| Redis transient unavailability | Writes/reads error | redis-py reconnects; next tick overwrites state (idempotent writes) |
| Gateway restarts | WebSocket clients disconnect | Browser dashboard auto-reconnects to `/ws/live`; snapshot re-sent on connect |
| A consumer falls behind | Kafka buffers; consumer lag grows | Catches up (uses `latest` so it may skip to head on rejoin) |
| Slow/dead WebSocket client | Detected on send failure | Socket dropped from the client set |

State is **externalized and overwritten every tick**, so no single process holds
unrecoverable state — the core property the lite mode lacks.

---

## 12. Testing strategy

| Layer | What | Where |
|---|---|---|
| **Unit** | Black-Scholes Greeks invariants (delta bounds, parity, gamma peak, theta sign, expiry edges) | [`tests/test_greeks.py`](tests/test_greeks.py) |
| **Unit** | VaR/CVaR properties (negative VaR, CVaR ≤ VaR, monotonic in vol/size, seed reproducibility, portfolio) | [`tests/test_var.py`](tests/test_var.py) |
| **Integration (local)** | Full per-tick pipeline wired without infra (simulator → risk → ml → snapshot), crash mechanic | smoke script (manual) |
| **System (e2e)** | `docker compose up`, verify REST/WS/snapshot shape, crash command round-trip, alert generation | manual via curl + ws client |

Run units: `pip install pytest && pytest` (32 tests). They import the shared
`mami_core` algorithms, so they cover both modes.

---

## 13. Build & deployment

```bash
# Distributed
docker compose up --build           # build images, start 7 containers
docker compose ps                   # health/status
docker compose logs -f risk-engine  # follow one service
docker compose up -d --scale risk-engine=3   # scale a compute service
docker compose down                 # stop
docker compose down -v              # stop + wipe Kafka/Redis volumes

# Lite
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python run.py                       # http://localhost:8000
```

**Image design:** each service has its own Dockerfile (build context = repo
root) that installs only its dependency set and copies `mami_core` + its
`main.py`. The gateway image is intentionally light (no NumPy/SciPy). Imports
resolve via the working directory (`/app`) being on `sys.path`.

---

## 14. Sequence diagrams

### 14.1 Tick lifecycle

```
ingestion        Kafka(market.ticks)     risk-engine   ml-service   api-gateway     Redis    WS client
   │  advance()        │                      │             │            │            │          │
   ├─ TickFrame ──────▶│                      │             │            │            │          │
   │                   ├───────────────────▶ consume        │            │            │          │
   │                   │                   compute_metrics   │            │            │          │
   │                   │                   (+VaR every 10)   │            │            │          │
   │                   │                      ├── write greeks/portfolio/var ───────▶ │          │
   │                   ├──────────────────────────────────▶ consume      │            │          │
   │                   │                                  score/forecast/alert        │          │
   │                   │                                     ├── write scores/forecasts/alerts ─▶ │
   │                   ├─────────────────────────────────────────────▶ consume        │          │
   │                   │                                                  write prices/history ─▶ │
   │                   │                                                  assemble_snapshot ◀──── │
   │                   │                                                       ├── send snapshot ─────▶│
```

### 14.2 Flash-crash command round-trip

```
WS client     api-gateway     Kafka(market.commands)    ingestion        (then normal tick lifecycle)
   │ POST /trigger-crash?NVDA │                              │
   ├────────────────────────▶ │                              │
   │                          ├── CommandMsg ──────────────▶ │
   │                          │                          trigger_crash(NVDA)
   │  202 {message} ◀───────  │                          (sets 4–7 tick crash)
   │                          │                              │ next advances apply −shock to NVDA
   │                          │                              ▼ ml-service scores ↑ → FLASH_CRASH alert
```

### 14.3 Service startup

```
service                       Kafka / Redis
  │ from_url(redis)               │
  │ loop: KafkaConsumer(...)      │
  ├─ try connect ───────────────▶ │  (not ready → exception)
  │   sleep 3, retry              │
  ├─ try connect ───────────────▶ │  (healthy → connected)
  │ log "ready"                   │
  └─ enter consume loop           │
```

---

## 15. Production gap & roadmap

What this slice intentionally simplifies vs. a production platform, and the path
to close each gap (priority order):

| # | Gap | Production target |
|---|---|---|
| 1 | In-memory ML state; whole-market frames | **Spark/Flink** managed state + **per-symbol partitioning** + portfolio aggregator |
| 2 | Raw JSON on the bus | **Schema Registry** + Avro/Protobuf (versioned contracts) |
| 3 | No metrics/tracing | **Prometheus + Grafana**, consumer-lag alerts, OpenTelemetry traces |
| 4 | Compose orchestration | **Kubernetes** + Helm, HPA autoscaling, liveness/readiness probes |
| 5 | Ephemeral state only | **Delta Lake / Parquet** sink for tick + risk history, audit trail |
| 6 | CPU NumPy default | **RAPIDS/cuDF on GPU** with full per-path option repricing |
| 7 | GBM simulator | **FIX / market-data** connectors |
| 8 | No auth | API gateway authn/authz, mTLS between services |

---

## 16. Glossary

| Term | Meaning |
|---|---|
| **Greeks** | Sensitivities of option value (Delta, Gamma, Vega, Theta) |
| **VaR** | Value at Risk — loss threshold at a confidence level |
| **CVaR / ES** | Conditional VaR / Expected Shortfall — mean loss beyond VaR |
| **GBM** | Geometric Brownian Motion — standard stochastic price model |
| **EWMA** | Exponentially Weighted Moving Average |
| **Isolation Forest** | Unsupervised tree-based anomaly detector |
| **TickFrame** | Per-tick market snapshot message on `market.ticks` |
| **Snapshot** | The canonical full-state dict served to the dashboard |
| **Consumer group** | Kafka clients sharing partitions of a topic (scaling unit) |
| **xp** | The active array backend (CuPy on GPU, NumPy on CPU) |
| **Lite / Distributed** | Single-process vs. microservice deployment modes |
