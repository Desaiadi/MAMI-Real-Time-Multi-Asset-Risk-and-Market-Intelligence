# MAMI — Distributed Architecture

This document is the **run guide** for MAMI: independent microservices
communicating over a real message bus (Kafka) with externalized state (Redis).
This is the "span many machines, survive failures, ingest real volume"
architecture, runnable on a laptop via Docker Compose.

All services share the same math via the [`mami_core`](mami_core/) library —
one source of truth for Black-Scholes, VaR, and the ML models.

```bash
docker compose up --build     # build + start all 7 containers
# open http://localhost:8000   (dashboard)  ·  /docs (API)
```

---

## 1. Topology

```
                            ┌──────────────────────────────┐
                            │           KAFKA              │  (the event bus)
                            │  topics: market.ticks        │
                            │          risk.metrics        │
                            │          ml.alerts           │
                            │          market.commands     │
                            └──────────────────────────────┘
        market.ticks ▲  │ market.ticks        │ market.ticks   ▲ market.commands
                     │  ▼                     ▼                │
   ┌───────────┐   ┌────────────┐      ┌────────────┐    ┌──────────────┐
   │ ingestion │──▶│ risk-engine│      │ ml-service │    │ api-gateway  │
   │  (GBM)    │   │ Greeks+VaR │      │ IF + EWMA  │    │ FastAPI/WS   │
   └───────────┘   └─────┬──────┘      └─────┬──────┘    └──────┬───────┘
         ▲               │ writes            │ writes           │ reads/writes
         │               ▼                   ▼                  ▼
         │            ┌────────────────────────────────────────────┐
         │            │                  REDIS                      │
         │            │   shared state: prices, greeks, portfolio,  │
         │            │   var, scores, forecasts, alerts, history   │
         │            └────────────────────────────────────────────┘
         │                                                   │
         └──────────────── flash-crash command ◀────── dashboard (browser)
```

**7 containers:** `zookeeper`, `kafka`, `redis`, and the four app services.

---

## 2. The services

| Service | Role | Consumes | Produces / Writes | Deps |
|---|---|---|---|---|
| **ingestion** | Market-data feed. GBM price simulator. Applies flash-crash commands. | `market.commands` | → `market.ticks` | numpy, kafka |
| **risk-engine** | Black-Scholes Greeks + Monte Carlo VaR/CVaR. GPU-capable. | `market.ticks` | → Redis (greeks, portfolio, var), `risk.metrics` | numpy, scipy, kafka, redis |
| **ml-service** | Isolation Forest anomaly scores + EWMA forecasts + alerts. Stateful stream processor. | `market.ticks` | → Redis (scores, forecasts, alerts), `ml.alerts` | numpy, sklearn, kafka, redis |
| **api-gateway** | Edge. Assembles snapshots from Redis, serves dashboard + WebSocket + REST, routes crash commands. Owns no math. | `market.ticks` | → Redis (prices, history), `market.commands`, WebSocket push | fastapi, aiokafka, redis |

Each service is an independent process with its own Dockerfile and dependency
set. The gateway is deliberately lightweight (no numpy/scipy) since it does no
computation.

---

## 3. Why this architecture (vs. a naive single-process design)

| Property | Naive single process | This (distributed) |
|---|---|---|
| **Survives a crash** | No — process death loses all state | Yes — state is in Redis; any service restarts (`restart: unless-stopped`) and resumes |
| **Scales horizontally** | No — one GIL-bound process | Yes — add replicas of risk-engine/ml-service in the same Kafka consumer group; partitions spread across them |
| **Decoupled components** | No — one function-call chain | Yes — services communicate only via Kafka topics; deploy/restart independently |
| **Backpressure / buffering** | No | Yes — Kafka buffers ticks if a consumer falls behind |
| **Real ingest path** | In-process loop | A bus any number of consumers can read independently |

---

## 4. GPU compute (grade) with CPU fallback

The heavy kernel — Monte Carlo VaR — runs against the `xp` array namespace in
[`mami_core/compute.py`](mami_core/compute.py):

- If an **NVIDIA GPU + CuPy** are present, `xp` is **CuPy** and the simulation
  runs on the GPU.
- Otherwise `xp` is **NumPy** and it runs on CPU. **Nothing breaks without a
  GPU** — this is the default.

The risk-engine logs which backend is live on startup
(`compute backend = numpy (CPU)` or `cupy (GPU)`).

**To enable the GPU path:**
1. Add a matching CuPy build to `services/risk_engine/requirements.txt`, e.g.
   `cupy-cuda12x` (match your CUDA version).
2. Give the `risk-engine` service access to the GPU via the NVIDIA Container
   Toolkit (a `deploy.resources.reservations.devices` / `gpus: all` stanza in
   `docker-compose.yml`).
3. Rebuild. The code is unchanged — only the backend swaps.

Set `MAMI_FORCE_CPU=1` to force NumPy even on a GPU box (useful for
reproducible benchmarks).

---

## 5. Running it

```bash
docker compose up --build        # build + start all 7 containers
# open http://localhost:8000      → dashboard
# open http://localhost:8000/docs → API docs

docker compose logs -f risk-engine   # follow one service
docker compose ps                    # health/status
docker compose down                  # stop everything
docker compose down -v               # stop + wipe Redis/Kafka volumes
```

Trigger a flash crash (routed through the bus to the ingestion service):

```bash
curl -X POST "http://localhost:8000/api/v1/trigger-crash?symbol=NVDA"
```

**Scale a compute service** (Kafka spreads tick partitions across replicas):

```bash
docker compose up -d --scale risk-engine=3
```

---

## 6. Configuration

All knobs are environment variables read in
[`mami_core/config.py`](mami_core/config.py) and wired in `docker-compose.yml`:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Broker address (services use the in-cluster listener) |
| `REDIS_URL` | `redis://redis:6379/0` | Shared state store |
| `MONTE_CARLO_PATHS` | `5000` | VaR simulation paths (raise for accuracy / to load a GPU) |
| `VAR_EVERY` | `10` | Recompute VaR every N ticks (risk-engine) |
| `TICK_INTERVAL_SECONDS` | `0.5` | Simulated market tick rate |
| `MAMI_FORCE_CPU` | `0` | Force NumPy even if a GPU is present |

---

## 7. What's deliberately deferred (next steps)

This is the **foundational slice** — a genuinely distributed, fault-tolerant
system, but intentionally not the whole production stack yet. Natural next
layers, in rough priority order:

1. **Spark / Flink** for the tick aggregation + ML state (replacing the
   in-memory buffers in `ml-service` with managed, partitioned state).
2. **Schema Registry + Avro/Protobuf** for the bus instead of raw JSON.
3. **Per-symbol partitioning** of `market.ticks` + a portfolio aggregator, so
   anomaly detection scales per-symbol across many ml-service replicas.
4. **Observability** — Prometheus metrics, Grafana dashboards, structured logs,
   consumer-lag alerts.
5. **Kubernetes** — Helm charts, HPA autoscaling, liveness/readiness probes
   (lifting these same containers off Compose onto a cluster).
6. **Durable storage** — Delta Lake / Parquet sink for tick + risk history and
   regulatory audit trails.
