# MAMI — Real-Time Multi-Asset Risk & Market Intelligence

Local demo of the MAMI platform described in the project breakdown.  
Runs fully offline — no cloud services needed.

## Two ways to run

| Mode | Command | What it is |
|---|---|---|
| **Lite** (single process) | `python run.py` | Everything in one Python process. Fastest to start, no Docker. Documented below + in [OVERVIEW.md](OVERVIEW.md). |
| **Distributed** (microservices) | `docker compose up --build` | Industry-grade architecture: Kafka + Redis + 4 containerized microservices. Survives failures, scales horizontally, GPU-capable. See **[ARCHITECTURE.md](ARCHITECTURE.md)**. |

Both modes share the same risk/ML algorithms via the `mami_core` library.
The rest of this README covers the **Lite** mode.

## Documentation

| Doc | Covers |
|---|---|
| [OVERVIEW.md](OVERVIEW.md) | Narrative "what it does" — the mental model, components, local-vs-production mapping |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Distributed run guide — topology, services, GPU enablement, config |
| **[DESIGN.md](DESIGN.md)** | **Full technical design — HLD, LLD, data/message models, algorithms, APIs, failure & scaling analysis, sequence diagrams** |

## What this demos

| Production component | Local equivalent |
|---|---|
| FIX / WebSocket ingest | Geometric Brownian Motion price simulator |
| Apache Kafka | In-process asyncio event loop |
| Spark Structured Streaming | In-process tick aggregation |
| Delta Lake / Databricks | In-memory deques per symbol |
| RAPIDS / cuDF (GPU) | NumPy (CPU) |
| Kubernetes microservices | Single FastAPI process |

The **algorithms** are the real ones:
- **Black-Scholes** Greeks (exact formula, scipy)
- **Monte Carlo VaR / CVaR** (5,000 GBM paths, numpy)
- **Isolation Forest** anomaly detection (sklearn)
- **EWMA** volatility forecasting (production uses LSTM)

---

## Quick start

```bash
# 1. Install Python 3.10+
# 2. Install dependencies
pip install -r requirements.txt

# 3. Run
python run.py

# 4. Open dashboard
open http://localhost:8000
```

---

## REST API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Dashboard |
| GET | `/docs` | Auto-generated API docs |
| WS  | `/ws/live` | Real-time JSON stream (500 ms) |
| GET | `/api/v1/snapshot` | Full current state |
| GET | `/api/v1/greeks/{symbol}` | Greeks for one position |
| GET | `/api/v1/var` | Portfolio VaR / CVaR |
| GET | `/api/v1/alerts` | Recent anomaly alerts |
| GET | `/api/v1/history/{symbol}` | Price + return history |
| POST | `/api/v1/trigger-crash` | Inject a flash crash (demo) |

---

## Project structure

```
mami/
├── run.py                  ← Entry point
├── requirements.txt
├── src/
│   ├── config.py           ← Portfolio + simulation settings
│   ├── black_scholes.py    ← Delta, Gamma, Vega, Theta
│   ├── var_engine.py       ← Historical + Monte Carlo VaR / CVaR
│   ├── anomaly.py          ← Isolation Forest + EWMA forecaster
│   ├── engine.py           ← Simulation orchestrator
│   └── api.py              ← FastAPI app + WebSocket
└── dashboard/
    └── index.html          ← Live risk dashboard (single file)
```

---

## Demo: trigger a flash crash

**From the dashboard:** click the ⚡ button in the Risk panel.

**From the terminal:**
```bash
curl -X POST "http://localhost:8000/api/v1/trigger-crash"
# Or for a specific stock:
curl -X POST "http://localhost:8000/api/v1/trigger-crash?symbol=NVDA"
```

Watch the Anomaly Alerts panel light up and the Isolation Forest score spike.

---

## Run the tests

```bash
pip install pytest
pytest          # 32 tests — Black-Scholes Greeks + VaR/CVaR
```

---

## Customise the portfolio

Edit `src/config.py` to change positions, strikes, expiry, or the list
of tracked symbols. Changes take effect on next `python run.py`.

---

## Push to GitHub

```bash
git init
git add .
git commit -m "MAMI local demo"
gh repo create mami --public --push
```
