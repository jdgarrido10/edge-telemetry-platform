# Edge IIoT Gateway — Async Telemetry Pipeline

> Fault-tolerant async gateway that ingests industrial sensor data from OPC UA subscriptions and motorsport telemetry over UDP, normalises both into a single typed pipeline, and delivers to MQTT with SQLite-backed store-and-forward, differentiated QoS, and statistical anomaly detection — all running on a single Python asyncio event loop.

---

## Overview

The gateway addresses the core problem of an industrial edge node: getting data from heterogeneous sources into a time-series store reliably, without a managed broker, cloud infrastructure, or silent failure modes.

Two sources feed a single `asyncio.Queue`:

- **OPC UA** — event-driven subscriptions from an industrial protocol server, with `SourceTimestamp` preservation and `StatusCode → QualityStatus` translation
- **UDP datagrams** — threshold-triggered telemetry from an Assetto Corsa shared-memory emitter, with per-signal range validation and lap event detection

Both streams converge into a unified `SensorReading` Pydantic model validated at the ingestion boundary. The worker is source-agnostic. When MQTT is unavailable, `CRITICAL` signals persist to SQLite and replay FIFO on reconnection. `BEST_EFFORT` signals are explicitly discarded to stay within the measured write ceiling. An in-process anomaly detector runs sliding-window statistics per sensor; persistent exceedances publish alerts to a dedicated MQTT topic. Three HTTP endpoints (`/health`, `/metrics`, `/alerts`) expose internal state with no live I/O on the read path.

This is an engineering exercise with measurable behaviour, explicit failure handling, and documented tradeoffs — not deployed infrastructure.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                                │
│                                                                         │
│  OPC UA Server (event-driven)          AC Simulator (UDP :9000)         │
│    SourceTimestamp preserved             ~50 Hz polling, JSON datagrams │
│    StatusCode → QualityStatus            per-signal range validation    │
│    node_id → sensor_id (sensors.yaml)    LapEvent detection             │
│    SensorReading.from_opc_notification() SensorReading.from_ac_packet() │
│         │  put_nowait()                           │  put_nowait()       │
└─────────┼───────────────────────────────────────-─┼─────────────────────┘
          └──────────────────┬──────────────────────┘
                             ▼
               asyncio.Queue (maxsize=2500)
                 overflow → discard + WARNING log
                             │
                             ▼
                          worker()
                             │
               ┌─────────────┴───────────────┐
               │  pending > 0?               │
               │  forward_pending()  ◄──── StoreAndForward (SQLite FIFO)
               └─────────────┬───────────────┘  save_batch()  on False
                             │                  delete_batch() after confirm
               queue.get(timeout=5s)
                             │
               measure latency: received_at − timestamp
                             │
               destination.send(payload)
                             │
               ┌─────────────┴──────────────────┐
               │  True                           │  False
               │  metrics.cont_total++           │  CRITICAL  → SQLite
               │  AnomalyDetector.analyze()      │  BEST_EFFORT → discard
               │    sliding window mean ± k·σ    │
               │    N consecutive → AlertData    │
               │    → alert_destination.send()   │
               └─────────────────────────────────┘
                             │
               MQTTDestination (aiomqtt, persistent)
                 recreate client on MqttError
                             │
               Mosquitto broker
                             │
               Telegraf  (timestamp: SensorReading.timestamp, not now())
                             │
               InfluxDB 2.7
                 measurement: sensor_readings
                 tags:        sensor_id, quality, unit
                 field:       value (float)
                             │
               Grafana dashboard ($sensor_id variable)

asyncio.TaskGroup (main.py)
  ├─ connect_with_retry()   OPC UA, exponential backoff 2s → 60s
  ├─ worker()               single consumer
  ├─ ac_adapter()           UDP datagram endpoint :9000
  └─ http_server()          /health  /metrics  /alerts  (stdlib only)
```

---

## Key Engineering Features

**Dual-source ingestion, source-agnostic worker**
OPC UA `DataChangeHandler` and UDP `ACProtocol` are synchronous by contract. Both call `queue.put_nowait()` and construct `SensorReading` independently. The worker never inspects source identity — `opc_node_id=None` is semantic, not an error. Adding a third source requires only a new adapter.

**Bounded queue with explicit backpressure**
`asyncio.Queue(maxsize=2500)` with `put_nowait()` in all sync handlers. On overflow: discard + WARNING log. A logged drop is observable; a frozen event loop is not. No silent failures.

**SQLite store-and-forward (at-least-once)**
`delete_batch()` executes only after `destination.send()` returns `True`. A crash between a successful publish and the delete causes re-delivery on restart — at-least-once by design, never silent loss. `get_batch()` orders by `id ASC`; `forward_pending()` stops on the first failed send, preserving FIFO order during partial outage recovery.

**Differentiated QoS — CRITICAL vs BEST_EFFORT**
Per-signal `criticality` configured in `ac_signals.yaml`. On broker failure: `CRITICAL` signals (RPM, brake, throttle, gear, speed) → `save_batch()` to SQLite; `BEST_EFFORT` signals (tyre temps, wheel slip, accelerometers, suspension, fuel) → discard + log. OPC UA signals always follow the `CRITICAL` path. 21 AC signals are configured across both criticality tiers. The classification is the primary architectural mitigation — it reduces persistence volume before SQLite is ever stressed under high-frequency load.

**Validation at the ingestion boundary**
`SensorReading` rejects NaN, ±Inf, timezone-naive timestamps, and `value=None` with `quality≠BAD` at construction — before the message touches the queue. The worker operates under the invariant that any dequeued message has already passed the contract. NaN propagated to InfluxDB corrupts every window-function query that touches that interval.

**Online anomaly detection**
Per-sensor sliding window (configurable `window_size`, `k_factor`, `min_samples`) computes mean ± k·σ dynamically. Single-sample outliers are discarded; N consecutive samples outside the band emit an `AlertData` to `gateway/alerts` and surface on `/alerts`. Quality-BAD samples trigger a separate alert type.

**Three-tier observability**
`/health` classifies the system as `healthy` / `degraded` / `unhealthy` based on queue fill, MQTT state, and OPC UA connection. `/metrics` exposes p50/p95/p99 latency over a rolling 1,000-sample window. `/alerts` returns live anomaly state. All endpoints use `asyncio.start_server()` from stdlib; no framework. No live I/O on the read path.

**OPC UA reconnection with exponential backoff**
`connect_with_retry()` loops with configurable initial delay (2 s), factor (×2), and ceiling (60 s). `status_change_notification()` signals reconnection via `asyncio.Event`. `sensors.yaml` is reloaded on reconnect — configuration changes during downtime take effect automatically.

**Graceful shutdown**
Ingest tasks cancel first. `queue.join()` with 10 s timeout drains the live queue. On timeout, remaining items drain to SQLite manually. Worker cancels last.

---

## Design Decisions & Tradeoffs

**`put_nowait()` over `await queue.put()` in sync handlers**
OPC UA callbacks and `DatagramProtocol` are synchronous by contract — they cannot `await`. Blocking the event loop until space is available would freeze all coroutines. The alternative is explicit data loss under sustained overload: a logged discard is observable; a frozen event loop is neither. This is the correct tradeoff for a real-time ingestion system.

**Single worker, not a worker pool**
Current load is ~95 msg/s against a measured worker ceiling of ~6,300–8,300 msg/s with a real broker — a margin of ~66–87×. A pool would add coordination overhead: shared SQLite access requires lock arbitration, counter increments need synchronisation, and FIFO ordering across workers requires explicit sequencing. None of that complexity is justified before hitting a bottleneck. Scale when the benchmark says to.

**SQLite for store-and-forward, not Redis or Kafka**
SQLite requires no additional process, no configuration, and is durable across restarts. `executemany` within a single transaction (PRAGMA WAL) amortises commit overhead to >150 k msg/s at batch size 500. The per-message commit figure (~442 msg/s) reflects the worst case of individual transactions — not the operational path. For the projected high-frequency scenario (~1,200 msg/s with MQTT down), batch throughput has sufficient headroom; the `BEST_EFFORT` classification reduces persistence volume further as the first line of defence. Redis would add a dependency and operational surface without solving a problem that exists at this scale; Kafka would be premature until horizontal partitioning is actually needed.

**Drop on queue overflow, not block**
The alternative to `put_nowait()` + discard in synchronous handlers is blocking the event loop — which would stall all in-flight coroutines including the worker, the HTTP server, and the OPC UA reconnect loop. Queue overflow at 2,500 in-flight messages already indicates a sustained MQTT or worker failure; adding producer backpressure at that point would amplify the failure, not mitigate it.

**In-memory anomaly state, not persisted**
Persisting per-sensor sliding windows adds write overhead and recovery complexity for a feature whose value is low-latency online detection, not historical completeness. After restart, detection resumes after `min_samples` new readings. Historical anomaly context already lives in InfluxDB via the raw series. The cold-start window is documented, not hidden.

**MQTT QoS 1, not QoS 2**
QoS 2 requires a 4-way handshake per message. At ~100 msg/s that is ~400 additional MQTT round-trips per second. The application layer already provides at-least-once semantics via `delete_batch()` post-confirm. InfluxDB deduplicates on `(measurement, tags, timestamp)`, so re-delivery of an identical point is harmless. QoS 2 overhead is real; the benefit is redundant.

**No HTTP framework**
Three read-only JSON endpoints do not justify an ASGI dependency. `asyncio.start_server()` from stdlib answers health checks in microseconds. HTTP/1.0-style connection closing is fine for low-frequency polling; it would be inappropriate for sustained query load, which is not a use case here.

**Scalability limits**
At current load (~95 msg/s), the architecture has ~66× headroom to the worker MQTT ceiling and ~5× headroom to the SQLite per-message ceiling. The documented stress scenario: 20 AC signals including high-frequency channels (wheel slip, accelerometer, suspension at ~60 Hz effective) under simultaneous MQTT outage yields ~1,200 msg/s ingress, exceeding the SQLite write ceiling. The `BEST_EFFORT` classification for those signals is the designed mitigation. A worker pool, Redis, or partitioned Kafka topic would be the next scaling levers — when the benchmark justifies it.

---

## Data Model

### `SensorReading` (Pydantic)

The canonical message through every pipeline stage. Constructed at source; never mutated by the worker.

| Field | Type | Constraint |
|---|---|---|
| `sensor_id` | `str` | min length 3 |
| `timestamp` | `datetime` | tz-aware required; naive → `ValidationError` |
| `value` | `float \| None` | `None` only when `quality=BAD`; NaN → `ValidationError`; ±Inf → `ValidationError` |
| `quality` | `QualityStatus` | enum: `Good`, `Bad`, `Uncertain` |
| `unit` | `str` | non-empty |
| `opc_node_id` | `str \| None` | `None` for UDP/AC sources — not an error field |
| `received_at` | `datetime` | tz-aware; set at gateway ingestion |
| `raw_value` | `float \| None` | mirrors `value`; reserved seam for future per-sensor calibration transforms |

**Invariants (model_validator):**
- `value=None` + `quality=Good` → `ValidationError`
- `value=None` + `quality=Uncertain` → `ValidationError`
- `value=None` + `quality=BAD` → accepted (OPC UA `Variant(Null)` on bad reads)

**Semantic constructors:**
- `from_opc_notification()` — translates asyncua native types; isolates the asyncua API surface from the rest of the pipeline
- `from_ac_packet()` — parses UDP JSON dict; applies per-signal range bounds from `ac_signals.yaml` to set `quality`

### `LapEvent` (Pydantic)

Discrete event emitted on AC lap completion. Routed to `gateway/laps` MQTT topic. Always `BEST_EFFORT` — not persisted to SQLite.

| Field | Type |
|---|---|
| `session_id` | `str` |
| `lap_number` | `int` |
| `lap_time_ms` | `int` |
| `timestamp` | `datetime` (tz-aware) |

### InfluxDB schema

```
measurement: sensor_readings
tags:         sensor_id, quality, unit
fields:       value (float)
timestamp:    SensorReading.timestamp  ← source timestamp, not ingestion time
```

Tags are indexed; `sensor_id` queries are O(series). Timestamp comes from the source, preserving the ability to compute true pipeline latency from stored data.

### Alert payload (`gateway/alerts`)

```json
{
  "sensor_id": "planta1.maquina1.temperatura",
  "timestamp": "2026-05-10T09:15:00+00:00",
  "type": "statistical",
  "value": 94.3,
  "mean": 67.1,
  "std": 2.4,
  "threshold": 71.9,
  "duration_samples": 8
}
```

`type` is `"quality"` for `quality=BAD` events, `"statistical"` for sliding-window exceedances.

---

## Reliability Model

| Failure | Mechanism | Guarantee |
|---|---|---|
| Queue overflow | `put_nowait()` + WARNING log | Observable discard; event loop never blocked |
| MQTT broker down | `save_batch()` on `send()` → False | At-least-once for CRITICAL; explicit discard for BEST_EFFORT |
| MQTT reconnection | `MQTTDestination` recreates client on `MqttError` | First failure always persists or discards; subsequent messages use restored connection |
| SQLite crash between send and delete | `delete_batch()` only after confirmed send | At-least-once: possible re-delivery, never silent loss |
| OPC UA disconnection | Exponential backoff 2 s → 60 s via `asyncio.Event` | Subscriptions restored without manual intervention |
| Data corruption at ingestion | Pydantic validators reject NaN, ±Inf, naive timestamps, `value=None` + `quality≠BAD` | Corrupt readings never enter the queue |
| Process shutdown | `queue.join()` with 10 s timeout; overflow drained to SQLite | In-flight messages land in SQLite, not lost |
| SQLite ceiling exceeded (high-freq MQTT outage) | BEST_EFFORT classification reduces persistence volume | No protection beyond QoS classification; documented, not hidden |

---

## Observability

### `/health`

```json
{
  "status": "healthy",
  "queue_size": 0,
  "sql_pending": 0,
  "mqtt_connected": true,
  "opc_ua_connected": true
}
```

| Status | Condition |
|---|---|
| `healthy` | MQTT connected + OPC UA connected + queue < 75% of maxsize |
| `degraded` | queue ≥ 75% full (≥ 1,875 of 2,500) |
| `unhealthy` | MQTT disconnected or OPC UA subscription dropped |

`mqtt_connected` is a cached flag updated by `send()` side effects — no live I/O on the health check path.

### `/metrics`

```json
{
  "pending": 0,
  "queueSize": 12,
  "cont_total": 4821,
  "cont_fails": 3,
  "latency_p50": 0.0012,
  "latency_p95": 0.0031,
  "latency_p99": 0.0087
}
```

Latency percentiles computed over a rolling 1,000-sample `collections.deque`. Suppressed until ≥ 100 samples. Latency is `received_at − timestamp`: pipeline latency from `SourceTimestamp` for OPC UA; UDP transit + polling delta for AC.

### `/alerts`

Returns active anomaly alerts from in-memory `AnomalyDetector` state. Historical data is not stored in the gateway — InfluxDB retains the raw series for retrospective analysis.

### Grafana + InfluxDB

Telegraf subscribes to `gateway/data` and writes each message with `SensorReading.timestamp` (not `now()`), preserving source-time semantics. Grafana dashboards use a `$sensor_id` variable for dynamic filtering across sensors. The full monitoring stack starts automatically via Docker Compose.

---

## Tech Stack

| Component | Role |
|---|---|
| Python 3.12 / asyncio | Runtime, event loop, concurrency model |
| asyncua | OPC UA client, subscription handler |
| aiomqtt | Async MQTT client (persistent connection) |
| Pydantic v2 | Data model, validation, serialisation |
| SQLite (stdlib) | Store-and-forward buffer |
| python-dotenv | Environment configuration |
| Mosquitto | MQTT broker |
| Telegraf | MQTT → InfluxDB bridge |
| InfluxDB 2.7 | Time-series storage |
| Grafana | Dashboard and visualisation |
| Docker Compose | Reproducible full-stack startup |
| pytest + pytest-asyncio | Unit and integration tests |

---

## Performance & Benchmarks

All measurements from local development hardware. Not representative of production or network environments.

**Worker throughput** (`benchmark_worker.py`, 10,000 pre-loaded messages):

| Destination | Throughput |
|---|---|
| `MockDestination` (pure in-memory, no I/O) | > 31,000 msg/s |
| `MQTTDestination` (local Docker broker) | > 6,300 msg/s |

Bottleneck with real MQTT: publish round-trip latency and socket I/O, not Python or asyncio processing. `MockDestination` is fully integrated in the benchmark, enabling CPU and anomaly detector throughput measurement without external infrastructure.

**SQLite write throughput** (`benchmark_sqlite.py`, `save_batch()` in isolation):

| Batch size | Throughput |
|---|---|
| 1 (per-message commit, worst case) | ~442 msg/s |
| 500 | ~156,000 msg/s |
| 1,000 | ~232,000 msg/s |

`save_batch()` uses `executemany` within a single transaction (PRAGMA WAL) to amortise commit overhead. The previous ~490 msg/s ceiling was caused by individual per-message transactions saturating disk I/O — transactional batching eliminates that bottleneck. Under real combined load (worker + `forward_pending()` contention on the shared `asyncio.Lock`), throughput will be lower than the isolated figure.

**Operational load vs. measured limits:**

| Metric | Value |
|---|---|
| Current load (5 OPC UA + 21 AC signals) | ~95 msg/s |
| Worker ceiling (MQTT) | > 6,300 msg/s |
| SQLite effective ceiling (batch 500) | > 150,000 msg/s |
| Margin to worker ceiling | ~66× |
| Margin to SQLite ceiling | > 1,000× |
| Projected stress scenario (21 AC signals, ~60 Hz effective) | ~1,200 msg/s |

At ~1,200 msg/s with MQTT down, SQLite batch throughput has sufficient headroom. The `BEST_EFFORT` QoS classification for high-frequency signals remains the first line of defence — it reduces persistence volume before SQLite is ever stressed.

---

## Known Limitations

**BEST_EFFORT signals are explicitly dropped under broker failure.**
Tyre temps, wheel slip, accelerometers, suspension, and fuel (16 of 21 AC signals) are discarded when MQTT is unavailable. This is a design choice — `CRITICAL` signals (5) are persisted; `BEST_EFFORT` signals reduce SQLite write volume by design. With transactional batching, SQLite throughput is no longer the constraint; the classification remains correct for storage economics and signal priority, not as a capacity workaround.

**AC timestamp is gateway wall clock, not simulator cycle time.** `SensorReading.timestamp` for AC sources is `datetime.now(UTC)` at UDP datagram receipt, not the simulator's internal game clock. Latency measurement for AC reflects UDP transit + polling delta, not true sensor-to-gateway latency. OPC UA sources use `SourceTimestamp` from the server, which is precise.

**Anomaly detector loses state on restart.** The per-sensor sliding window is in-process memory. After restart, detection resumes after `min_samples` new readings. The cold-start window is logged but not externally signalled.

**Health check uses last-known MQTT state, not a live probe.** `is_available()` returns a flag updated by `send()`. If the broker silently drops the connection without a `MqttError`, the health endpoint may report `healthy` until the next `send()` fails.

**MQTT reconnection loses one message per disconnect.** The message that encounters a `MqttError` triggers client recreation but is not retried in the same cycle — it routes to SQLite or is discarded. The next message uses the restored connection. At-least-once is preserved for `CRITICAL` signals; one-cycle latency on reconnection is inherent.

**No authentication in the demo stack.** Mosquitto, InfluxDB, and the OPC UA simulator are configured without TLS or token auth for local development ease. Not suitable for network-exposed deployments.

**Single-node architecture.** One worker, one SQLite file, one MQTT connection. Horizontal scaling would require partitioning the queue, sharding SQLite or replacing it, and coordinating worker state — none of which is warranted at current load.

---

## Quick Start

**Prerequisites:** Docker ≥ 24, Docker Compose v2.

```bash
git clone <repo-url>
cd edge-iiot-gateway

cp .env.example .env
# Edit .env: INFLUXDB_TOKEN, INFLUXDB_PASSWORD, GRAFANA_PASSWORD

docker compose up --build
```

Allow ~15 seconds for full stack initialisation.

| Endpoint | Address |
|---|---|
| Gateway health | http://localhost:8080/health |
| Gateway metrics | http://localhost:8080/metrics |
| Gateway alerts | http://localhost:8080/alerts |
| Grafana | http://localhost:3000 |
| InfluxDB | http://localhost:8086 |
| MQTT broker | localhost:1883 |

**Run tests (no Docker required):**

```bash
pip install -r requirements.txt
pytest tests/test_models.py tests/test_pipeline.py -v
```

Test coverage: 14 Pydantic validation cases + 9 pipeline integration tests (`MockDestination`) covering valid delivery, MQTT failure → SQLite persistence, FIFO replay order, CRITICAL vs BEST_EFFORT divergence, `LapEvent` routing, naive timestamp rejection, and `quality=BAD` + `value=None` acceptance.

**Run benchmarks:**

```bash
# SQLite throughput (no external dependencies)
python -m tests.benchmark_sqlite

# Worker throughput with MockDestination (no external dependencies)
python -m tests.benchmark_worker

# Worker throughput with real MQTT (requires running broker)
# Edit benchmark_worker.py: use_real_mqtt=True
python -m tests.benchmark_worker
```

---

## Engineering Principles

**Validation at the boundary.** `SensorReading` validates at construction, before the queue. The pipeline interior operates under the invariant that any message it handles has passed the contract. The cost of validation at entry is negligible; the cost of NaN propagated to InfluxDB is corruption of every window-function query that touches that interval.

**Source-agnostic worker.** The worker never branches on source identity. `opc_node_id=None` is semantic, not an error. `LapEvent` is handled by type check, not payload inspection. A third source requires only a new adapter producing `SensorReading` objects — the worker is unchanged.

**Explicit over implicit failure handling.** Every failure mode at every stage has a documented outcome: queue full → discard + log; `send()` False + CRITICAL → SQLite; `send()` False + BEST_EFFORT → discard + log; OPC UA drop → backoff + reconnect; shutdown with queue items → drain to SQLite. No silent failures.

**No I/O on the health check path.** `is_available()` returns a cached flag; `opc_ua_connected` reads an `asyncio.Event`. Health endpoints answer in microseconds without network calls.

**Scale when the benchmark says to.** Single worker, single SQLite file, one MQTT connection. Current load is ~95 msg/s against a worker ceiling of >6,300 msg/s and a SQLite batch ceiling of >150,000 msg/s. The benchmark that would justify a pool, Redis, or Kafka does not yet exist.

---

## Project Structure

```
.
├── src/
│   ├── main.py                  # TaskGroup orchestration, shutdown logic
│   ├── worker.py                # Core consumer loop, QoS routing, latency measurement
│   ├── models.py                # SensorReading, LapEvent, QualityStatus (Pydantic)
│   ├── store_forward.py         # SQLite persistence (save_batch, get_batch, delete_batch)
│   ├── anomaly_detector.py      # Sliding window mean ± k·σ, persistent alert detection
│   ├── opcua_client.py          # DataChangeHandler, connect_with_retry
│   ├── ac_adapter.py            # UDP datagram endpoint, ACProtocol
│   ├── metrics.py               # GatewayMetrics (counters, latency deque, health state)
│   ├── server.py                # HTTP server: /health, /metrics, /alerts
│   ├── settings.py              # Centralised config (env vars + .env + defaults)
│   ├── config.py                # YAML loaders: sensors, ac_signals, analysis
│   └── destinations/
│       ├── base.py              # BaseDestination ABC
│       └── mqtt_destination.py  # MQTTDestination with reconnection on MqttError
├── config/
│   ├── sensors.yaml             # OPC UA node → sensor_id + unit mapping
│   ├── ac_signals.yaml          # AC signal → sensor_id, unit, range, criticality
│   └── analysis.yaml            # Anomaly detector: window_size, k_factor, min_samples
├── simulator/
│   └── opcua_server.py          # Containerised OPC UA server with simulated sensor data
├── tests/
│   ├── test_models.py           # Unit tests: SensorReading validators (14 cases)
│   ├── test_pipeline.py         # Integration tests: end-to-end pipeline (9 cases)
│   ├── benchmark_worker.py      # Worker throughput benchmark
│   └── benchmark_sqlite.py      # SQLite write throughput benchmark
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.sim
├── Dockerfile.ac_sim
├── telegraf.conf
├── mosquitto.conf
├── .env.example
└── requirements.txt
```

---

## Portfolio Notes

### Demo

<!-- Add GIF or screen recording of the running pipeline here -->
<!-- Suggested: terminal split showing gateway logs + curl /metrics cycling -->
<!-- Grafana dashboard screenshot showing live sensor_readings series -->

> Video walkthrough: *(link to be added)* — covers architecture, failure injection (MQTT kill + recovery), and anomaly alert triggering.

### Why This Project Matters

IIoT edge nodes face a specific class of problem that differs from web backends: data arrives from protocols that predate async I/O (OPC UA, Modbus, CAN bus), quality metadata must travel with the value (not inferred later), and local persistence must bridge intermittent connectivity without a managed broker. This project works through those constraints concretely — backpressure without blocking a synchronous callback, at-least-once without a distributed log, QoS differentiation without a full message bus — and documents where each decision breaks down.

### What I Would Scale Next

| Current | Next lever | When |
|---|---|---|
| Single `asyncio.Queue` | Partitioned queues by criticality | Queue saturation on mixed high-frequency loads |
| SQLite store-and-forward | Apache Kafka (or Redpanda) | Need replication, replay from arbitrary offset, or multi-consumer fan-out |
| Single worker | Worker pool with shared `asyncio.Lock` on SQLite | Worker CPU becomes the bottleneck (not yet) |
| In-memory anomaly state | Redis-backed sliding window | Multi-gateway deployment needing shared detection state |
| Single-node gateway | Horizontal partitioning by `sensor_id` range | Throughput exceeds single-node MQTT or SQLite ceiling |
| Per-message QoS classification | Dynamic criticality based on alert state | Adaptive persistence during active anomaly windows |