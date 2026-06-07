# MAMI — Real-Time Multi-Asset Risk & Market Intelligence

A real-time derivatives **risk platform**: it streams a live market feed,
continuously re-prices an options portfolio, quantifies potential loss, and
flags abnormal market behavior — all pushed to a live dashboard.

It runs as a **distributed microservice system** (Kafka + Redis + 4 services) on
Docker Compose — fault-tolerant, horizontally scalable, and GPU-capable — yet
boots on a laptop with one command. The quant/ML algorithms are the real ones.

## Quick start

```bash
docker compose up --build
# open http://localhost:8000        → live dashboard
# open http://localhost:8000/docs   → API docs
```

```bash
docker compose down        # stop
docker compose down -v     # stop + wipe Kafka/Redis volumes
```

## Documentation

| Doc | Covers |
|---|---|
| [OVERVIEW.md](OVERVIEW.md) | Narrative "what it does" — mental model, components, the algorithms |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Run guide — topology, services, GPU enablement, scaling, config |
| **[DESIGN.md](DESIGN.md)** | **Full technical design — HLD, LLD, data/message models, algorithms, APIs, failure & scaling analysis, sequence diagrams** |

## Architecture

```
ingestion ──market.ticks──▶ risk-engine ─┐
    ▲                       ml-service  ─┼─▶ Redis ◀── api-gateway ──▶ dashboard
    └──market.commands── api-gateway ────┘         (Kafka is the bus)
```

**7 containers:** Zookeeper + Kafka (event bus), Redis (shared state), and four
independent Python microservices:

| Service | Role |
|---|---|
| **ingestion** | GBM market-data feed → publishes ticks; applies crash commands |
| **risk-engine** | Black-Scholes Greeks + Monte Carlo VaR/CVaR (GPU-capable) |
| **ml-service** | Isolation Forest anomaly detection + EWMA volatility forecasting |
| **api-gateway** | FastAPI edge: REST + WebSocket + dashboard |

All services share one algorithm library, [`mami_core`](mami_core/) — a single
source of truth for the math.

## The algorithms (the real part)

- **Black-Scholes** Greeks — exact formula (SciPy)
- **Monte Carlo VaR / CVaR** — 5,000 GBM paths, GPU-capable (CuPy → NumPy fallback)
- **Isolation Forest** anomaly detection (scikit-learn)
- **EWMA** volatility forecasting (production uses an LSTM)

## What maps to what (vs. a production platform)

| Production component | In this project |
|---|---|
| FIX / WebSocket market feed | GBM price simulator (ingestion service) |
| Apache Kafka | **Apache Kafka** (real) |
| Spark Structured Streaming | Stateful stream consumers (ml-service) |
| Delta Lake / Databricks | Redis shared state |
| RAPIDS / cuDF (GPU) | CuPy when a GPU is present, else NumPy |
| Kubernetes | Docker Compose (K8s is the documented next step) |

## REST API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Dashboard |
| GET | `/docs` | Auto-generated API docs |
| WS  | `/ws/live` | Real-time JSON stream (~500 ms) |
| GET | `/api/v1/snapshot` | Full current state |
| GET | `/api/v1/greeks/{symbol}` | Greeks for one position |
| GET | `/api/v1/var` | Portfolio VaR / CVaR |
| GET | `/api/v1/alerts` | Recent anomaly alerts |
| GET | `/api/v1/history/{symbol}` | Price history |
| POST | `/api/v1/trigger-crash` | Inject a flash crash (demo) |

## Demo: trigger a flash crash

**From the dashboard:** click the ⚡ button in the Risk panel.

**From the terminal** (routed through the bus to the ingestion service):
```bash
curl -X POST "http://localhost:8000/api/v1/trigger-crash"            # random symbol
curl -X POST "http://localhost:8000/api/v1/trigger-crash?symbol=NVDA" # specific one
```

Watch the Anomaly Alerts panel light up and the Isolation Forest score spike.

## Project structure

```
mami/
├── docker-compose.yml      ← 7-container topology
├── mami_core/              ← shared library (single source of truth)
│   ├── config.py           ← portfolio + infra settings
│   ├── compute.py          ← GPU/CPU array backend (CuPy → NumPy)
│   ├── black_scholes.py    ← Delta, Gamma, Vega, Theta
│   ├── var_engine.py       ← Historical + Monte Carlo VaR / CVaR
│   ├── anomaly.py          ← Isolation Forest + EWMA forecaster
│   └── schemas.py          ← Kafka message contracts
├── services/
│   ├── ingestion/          ← GBM market-data producer
│   ├── risk_engine/        ← Greeks + VaR
│   ├── ml_service/         ← anomaly + forecasting
│   └── api_gateway/        ← FastAPI: REST + WebSocket + dashboard
├── dashboard/index.html    ← live risk dashboard (single file)
└── tests/                  ← Greeks + VaR/CVaR unit tests
```

## Scale a compute service

Kafka spreads tick partitions across replicas in the same consumer group:

```bash
docker compose up -d --scale risk-engine=3
```

## Run the tests

The unit tests exercise the shared `mami_core` algorithms — no Docker needed:

```bash
pip install -r requirements.txt
pytest          # 32 tests — Black-Scholes Greeks + VaR/CVaR
```

## Customise the portfolio

Edit [`mami_core/config.py`](mami_core/config.py) to change positions, strikes,
expiry, or tracked symbols, then rebuild: `docker compose up --build`.

## Push to GitHub

```bash
git init
git add .
git commit -m "MAMI — distributed real-time risk platform"
gh repo create mami --public --push
```
