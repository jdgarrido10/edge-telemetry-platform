# Edge IIoT Gateway — Async OPC UA & UDP Telemetry Pipeline

Fault-tolerant async gateway that ingests industrial sensor data from OPC UA subscriptions and motorsport telemetry over UDP, normalises both streams into a single typed data model, and delivers to MQTT with SQLite-backed store-and-forward for at-least-once delivery.

---

## Overview

The gateway bridges two physically separate data sources — an OPC UA server exposing industrial process variables, and an Assetto Corsa shared-memory emitter sending vehicle telemetry over UDP — into a unified asyncio pipeline backed by MQTT, InfluxDB, and Grafana.

All data entering the pipeline passes through a single Pydantic model (`SensorReading`) that enforces timezone-awareness, rejects NaN/Inf values, and validates the `value=None` / `quality=BAD` invariant at the point of ingestion, before the message ever touches the queue. The worker never needs to defend against corrupt data.

When MQTT is unavailable, undelivered messages are persisted to a local SQLite buffer and replayed FIFO on reconnection. Only after a successful `publish` is the record deleted — the system guarantees at-least-once delivery for signals marked `CRITICAL`, and explicitly discards `BEST_EFFORT` signals under broker outage to stay within the measured SQLite write ceiling.

An in-process anomaly detector runs a sliding-window statistical analysis (mean ± k·σ) over configurable signals. Persistent exceedances — N consecutive samples outside the band — trigger an `AlertData` published to a dedicated MQTT topic and surfaced on `/alerts`. The gateway also exposes `/health` and `/metrics` over a stdlib-only HTTP server with no external dependencies.

The full stack (Mosquitto, InfluxDB, Telegraf, Grafana, OPC UA simulator, AC simulator, gateway) is containerised via Docker Compose and starts with a single command.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                                    │
│                                                                             │
│  OPC UA Server                          AC Shared Memory (Windows host)     │
│    event-driven subscriptions             ~50 Hz polling, threshold-driven  │
│    SourceTimestamp preserved              JSON over UDP :9000               │
│         │                                         │                         │
│  DataChangeHandler (sync)               ACProtocol.datagram_received (sync) │
│    StatusCode → QualityStatus             SensorReading.from_ac_packet()    │
│    node_id → sensor_id (sensors.yaml)     range validation per signal       │
│    SensorReading.from_opc_notification()  LapEvent detection                │
│         │  put_nowait()                           │  put_nowait()           │
└─────────┼───────────────────────────────────────-─┼─────────────────────────┘
          └──────────────────┬──────────────────────┘
                             ▼
                 asyncio.Queue (maxsize=2500)
                   backpressure via put_nowait() + log
                             │
                             ▼
                          worker()
                             │
                   ┌─────────┴──────────┐
                   │  pending > 0?      │
                   │  forward_pending() │◄── StoreAndForward (SQLite FIFO)
                   │                    │      save_batch()  on send() = False
                   └─────────┬──────────┘      delete_batch() after confirm
                             │
                   queue.get(timeout=5s)
                             │
                      measure latency
                   received_at − timestamp
                             │
                   destination.send(payload)
                             │
                   ┌─────────┴─────────────┐
                   │  True                 │  False
                   │  metrics.cont_total++ │  CRITICAL → SQLite
                   │                       │  BEST_EFFORT → discard + log
                   │  anomaly_detector     │
                   │  .analyze(reading)    │
                   │  if alert:            │
                   │    alert_destination  │
                   │    .send(alert)       │
                   └───────────────────────┘
                             │
                      MQTTDestination
                        persistent connection (aiomqtt)
                        recreate client on MqttError
                             │
                      Mosquitto broker
                             │
                      Telegraf (MQTT subscriber)
                        timestamp: SensorReading.timestamp
                             │
                      InfluxDB 2.7
                        measurement: sensor_readings
                        tags: sensor_id, quality, unit
                        field: value (float)
                             │
                      Grafana dashboard
                        variable: $sensor_id (dynamic)

asyncio.TaskGroup (main.py)
  ├─ connect_with_retry()   OPC UA, exponential backoff 2s → 60s
  ├─ worker()               single consumer
  ├─ ac_adapter()           UDP datagram endpoint :9000
  └─ http_server()          /health  /metrics  /alerts  (stdlib only)
```

### Data flow — normal operation

1. OPC UA `DataChangeHandler` or `ACProtocol` constructs a `SensorReading` and calls `queue.put_nowait()`. If the queue is full the datum is discarded with a WARNING log.
2. `worker()` checks SQLite for any pending messages from a previous outage and replays them FIFO before consuming from the live queue.
3. It calls `destination.send(payload)`. On success it increments counters and runs the anomaly detector. On failure it routes based on the signal's configured criticality.
4. After `send()` returns, `AnomalyDetector.analyze()` updates the per-sensor sliding buffer and emits an `AlertData` to a separate MQTT topic if N consecutive samples breach the statistical threshold.
5. Telegraf subscribes to `gateway/data`, stamps each message with `SensorReading.timestamp` (not `now()`), and writes to InfluxDB.

### Data flow — MQTT outage

- `send()` returns `False`; `MQTTDestination` immediately attempts to recreate the client.
- `CRITICAL` signals are persisted to SQLite via `save_batch()`.
- `BEST_EFFORT` signals (configurable in `ac_signals.yaml`) are discarded.
- On next worker cycle, `forward_pending()` replays SQLite rows FIFO. `delete_batch()` is called only after the broker confirms receipt.

---

## Core Features

- **Dual-source ingestion:** OPC UA event-driven subscriptions and UDP datagrams unified into one typed pipeline; the worker is source-agnostic.
- **Pydantic data contract:** `SensorReading` validates timezone-awareness, rejects NaN/Inf, enforces `value=None` only with `quality=BAD`, and maps OPC UA `StatusCode` to `QualityStatus`.
- **SQLite store-and-forward:** at-least-once delivery for `CRITICAL` signals; FIFO replay on reconnection; `delete_batch()` called only after confirmed delivery.
- **Differentiated QoS:** per-signal `criticality` flag (`CRITICAL` / `BEST_EFFORT`) in `ac_signals.yaml` controls SQLite persistence vs. discard under broker outage.
- **Exponential backoff reconnection:** OPC UA reconnects with configurable initial delay, backoff factor, and ceiling (defaults: 2 s → 60 s, ×2).
- **MQTT persistent connection:** `MQTTDestination` keeps one connection open; recreates the `aiomqtt.Client` on `MqttError` within the same `send()` call.
- **Sliding-window anomaly detection:** per-sensor buffer, mean ± k·σ threshold, quality-BAD detection; alerts published to `gateway/alerts` topic only after N consecutive anomalous samples.
- **Lap event detection:** `LapEvent` messages from the AC adapter are routed to a dedicated `gateway/laps` MQTT topic; treated as `BEST_EFFORT` (not persisted to SQLite).
- **Bounded backpressure:** `asyncio.Queue(maxsize=2500)` with `put_nowait()` in all synchronous handlers; no blocking of the event loop under producer pressure.
- **Latency measurement:** `received_at − timestamp` computed per message in the worker; accumulated in a fixed-length deque for p50/p95/p99 reporting in `/metrics`.
- **HTTP observability:** `/health`, `/metrics`, `/alerts` served with `asyncio.start_server()`; no framework dependency.
- **External YAML configuration:** sensor-to-node mapping (`sensors.yaml`), AC signal config (`ac_signals.yaml`), and anomaly analysis parameters (`analysis.yaml`) loaded at startup and on reconnection.
- **Docker Compose stack:** Mosquitto, InfluxDB 2.7, Telegraf, Grafana, OPC UA simulator, AC simulator, and gateway; single-command startup.
- **Integration test suite:** `MockDestination`-based pipeline tests covering happy path, validation rejection, SQLite fallback, and FIFO replay.

---

## Data Model / Contracts

### `SensorReading` (Pydantic)

The canonical message that flows through every stage of the pipeline.

| Field | Type | Constraint |
|---|---|---|
| `sensor_id` | `str` | min length 3 |
| `timestamp` | `datetime` | tz-aware required; naive → `ValidationError` |
| `value` | `float \| None` | `None` only when `quality=BAD`; NaN → `ValidationError`; ±Inf → `ValidationError` |
| `quality` | `QualityStatus` | enum: `Good`, `Bad`, `Uncertain` |
| `unit` | `str` | non-empty |
| `opc_node_id` | `str \| None` | `None` for UDP/AC sources — not an error field |
| `received_at` | `datetime` | tz-aware; set by the gateway at ingestion |
| `raw_value` | `float \| None` | mirrors `value`; reserved for future calibration |

**Invariants enforced by `model_validator`:**
- `value=None` + `quality=Good` → `ValidationError`
- `value=None` + `quality=Uncertain` → `ValidationError`
- `value=None` + `quality=Bad` → accepted (OPC UA `Variant(Null)` on bad reads)

**Semantic constructors:**
- `SensorReading.from_opc_notification()` — translates `asyncua` native types; isolates the asyncua API surface.
- `SensorReading.from_ac_packet()` — parses UDP JSON dict; applies per-signal range validation to set `quality`.

### `LapEvent` (Pydantic)

Discrete event emitted by the AC adapter when a lap completes.

| Field | Type |
|---|---|
| `session_id` | `str` |
| `lap_number` | `int` |
| `lap_time_ms` | `int` |
| `timestamp` | `datetime` (tz-aware) |

### MQTT payload (both models)

`model.to_mqtt_payload()` calls `model_dump(mode='json')` — all fields serialised, timestamps as ISO-8601 strings.

### InfluxDB schema

```
measurement: sensor_readings
tags:         sensor_id, quality, unit
fields:       value (float)
timestamp:    SensorReading.timestamp
```

Tags are indexed; `sensor_id` queries are O(series). Timestamp comes from the source, not from Telegraf ingestion time, preserving the ability to compute pipeline latency from stored data.

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

## Reliability & Failure Handling

### Backpressure

Both `DataChangeHandler` (OPC UA) and `ACProtocol` (UDP) are synchronous and cannot `await`. They call `queue.put_nowait()`. When the queue is at capacity (`maxsize=2500`) the datum is silently discarded with a `WARNING` log — the only alternative that does not block the event loop. The choice is explicit: a logged drop is observable; a frozen event loop is not.

### MQTT reconnection

`MQTTDestination` maintains a persistent `aiomqtt.Client`. On `MqttError` in `send()`, it immediately attempts to recreate and reconnect the client before returning `False`. The message that triggered the failure is not retried in the same cycle — it is either persisted to SQLite (if `CRITICAL`) or discarded (if `BEST_EFFORT`). The next message uses the restored connection.

### Store-and-forward (SQLite)

`StoreAndForward` uses a single persistent connection opened in `__init__`, protected by `asyncio.Lock` across all coroutines. `executemany` is used for batch inserts to reduce transaction overhead.

**At-least-once guarantee:**
- Messages are written to SQLite before `send()` is retried.
- `delete_batch()` is called only after `destination.send()` returns `True` for each row.
- If the process crashes between a successful `publish` and `delete_batch()`, the message will be replayed on restart. This is a known consequence of at-least-once semantics.

**FIFO replay:** `get_batch()` orders by `id ASC`. `forward_pending()` iterates the batch sequentially and stops on the first failed send, so partial delivery during an ongoing outage does not reorder messages.

### Differentiated QoS (CRITICAL / BEST_EFFORT)

Each AC signal has a `criticality` field in `ac_signals.yaml`. The worker consults this after a failed `send()`:
- `CRITICAL` → `store.save_batch([payload])`
- `BEST_EFFORT` → discard + WARNING log

OPC UA signals always follow the `CRITICAL` path (no `ac_signals` entry defaults to `CRITICAL`).

**Known capacity limit:** `save_batch()` throughput is ~490 msg/s sustained in isolation. With 20 AC signals at high effective frequency under MQTT outage, ingress can exceed this rate. The `BEST_EFFORT` classification for high-frequency signals (e.g. wheel slip, suspension travel) is the architectural mitigation for this constraint.

### OPC UA reconnection

`connect_with_retry()` wraps the client in an exponential backoff loop (initial: 2 s, factor: ×2, ceiling: 60 s). When the subscription drops, `status_change_notification()` sets an `asyncio.Event` that the subscription loop monitors. On reconnection, `sensors.yaml` is reloaded — configuration changes during downtime take effect automatically.

### Graceful shutdown

`shutdown()` cancels ingest tasks (`t_ingesta`, `t_ac`, `t_http`) first, then calls `queue.join()` with a 10-second timeout to let the worker drain the live queue. On timeout, remaining queue items are manually drained to SQLite. The worker task is cancelled last.

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
| `degraded` | queue ≥ 75% of maxsize (≥ 1875 of 2500) |
| `unhealthy` | MQTT disconnected or OPC UA subscription dropped |

`mqtt_connected` reflects `_connected`, a flag updated by `send()` — no live I/O on the health check path. `opc_ua_connected` reflects the `asyncio.Event` state.

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

Latency percentiles (p50/p95/p99) are computed over a rolling window of 1000 samples from a `collections.deque`. Percentiles are suppressed until at least 100 samples are accumulated.

Latency is `received_at − timestamp`. For OPC UA, this is pipeline latency from `SourceTimestamp` to gateway ingestion. For AC UDP, it is UDP transit time plus the threshold-triggered emission delta.

### `/alerts`

Returns the current set of active anomaly alerts as computed by `AnomalyDetector`. Historical alert data is not stored in the gateway — InfluxDB retains the raw series from which historical analysis can be reconstructed.

### Logging

Structured log lines at DEBUG/INFO/WARNING/ERROR via Python `logging`. Notable events: queue full (WARNING), validation rejection (ERROR), forward start/completion (INFO), SQLite save/delete counts (INFO), latency per message (DEBUG).

---

## Tech Stack

**Runtime**
- Python 3.12 / asyncio
- [asyncua](https://github.com/FreeOpcUa/opcua-asyncio) — async OPC UA client
- [aiomqtt](https://github.com/sbtinstruments/aiomqtt) — async MQTT client
- [Pydantic v2](https://docs.pydantic.dev/) — data validation and serialisation
- SQLite (stdlib `sqlite3`) — store-and-forward buffer
- python-dotenv — environment configuration

**Infrastructure**
- Eclipse Mosquitto 2 — MQTT broker
- InfluxDB 2.7 — time-series storage
- Telegraf 1.29 — MQTT → InfluxDB bridge
- Grafana — dashboard and visualisation

**Containerisation**
- Docker / Docker Compose — full stack, single-command startup

**Testing**
- pytest + pytest-asyncio
- `MockDestination` (in-process, no broker required) for pipeline integration tests

---

## Project Structure

```
.
├── src/
│   ├── main.py                # asyncio.TaskGroup entrypoint; shutdown logic
│   ├── settings.py            # centralised configuration (env vars + .env)
│   ├── models.py              # SensorReading, LapEvent, QualityStatus
│   ├── opcua_client.py        # DataChangeHandler, subscribe(), connect_with_retry()
│   ├── ac_adapter.py          # ACProtocol (UDP datagram endpoint), ac_adapter()
│   ├── worker.py              # worker(), forward_pending()
│   ├── store_forward.py       # StoreAndForward (SQLite, asyncio.Lock, FIFO)
│   ├── anomaly_detector.py    # AnomalyDetector, AlertData
│   ├── metrics.py             # GatewayMetrics (latency deque, health/snapshot)
│   ├── server.py              # HTTP server: /health /metrics /alerts (stdlib)
│   ├── config.py              # YAML loaders: sensors, ac_signals, analysis
│   └── destinations/
│       ├── base.py            # BaseDestination ABC
│       └── mqtt_destination.py # MQTTDestination (aiomqtt, persistent connection)
├── config/
│   ├── sensors.yaml           # OPC UA node → sensor_id + unit mapping
│   ├── ac_signals.yaml        # AC signal config: sensor_id, unit, range, criticality
│   └── analysis.yaml          # Anomaly detector: window_size, min_samples, k_factor
├── tests/
│   ├── test_models.py         # Unit tests: Pydantic validation invariants (14 tests)
│   ├── test_pipeline.py       # Integration tests: MockDestination-based E2E
│   ├── benchmark_sqlite.py    # SQLite write throughput benchmark
│   ├── benchmark_worker.py    # Worker throughput benchmark (MockDestination + MQTT)
│   └── pytest.ini
├── docker-compose.yml         # Full stack: broker, DB, telegraf, grafana, sims, gateway
├── Dockerfile                 # Gateway image
├── Dockerfile.sim             # OPC UA simulator image
├── Dockerfile.ac_sim          # AC telemetry simulator image
├── telegraf.conf              # MQTT input → InfluxDB output with timestamp passthrough
├── mosquitto.conf
└── requirements.txt
```

---

## Performance / Benchmarks

Measured on development hardware. Numbers reflect isolated component throughput, not combined-load throughput.

**Worker throughput (`benchmark_worker.py`, 10 000 pre-loaded messages)**

| Destination | Throughput |
|---|---|
| `MockDestination` (no I/O) | ~66 000 msg/s |
| `MQTTDestination` (Docker broker) | ~8 300 msg/s |

Bottleneck with real MQTT: publish round-trip latency, not Python processing.

**SQLite write throughput (`benchmark_sqlite.py`, `save_batch()` in isolation)**

| Batch size | Throughput |
|---|---|
| 1 (one INSERT per call) | ~few hundred msg/s |
| 500 | higher |
| 1 000 | ~490 msg/s sustained ceiling |

`executemany` within a single transaction is used for all batch writes. Throughput degrades under concurrent `forward_pending()` contention — the isolation benchmark represents an upper bound.

**Operational load (current configuration)**

| Source | Typical rate |
|---|---|
| OPC UA (5 sensors, ~1 Hz) | ~5 msg/s |
| AC UDP (6 signals, threshold-driven) | ~90 msg/s peak |
| Combined | ~95 msg/s |

Current load is ~5× below the SQLite write ceiling and ~87× below the worker MQTT ceiling.

---

## Known Limitations

**SQLite throughput under high-frequency AC load.** If the full set of high-rate AC signals (wheel slip, accelerometer, suspension travel at ~60 Hz effective) is enabled and MQTT fails simultaneously, ingress can exceed ~490 msg/s. The `BEST_EFFORT` criticality classification for those signals is the current mitigation — they will be discarded rather than queued. There is no backpressure mechanism beyond this.

**AC timestamp accuracy.** `SensorReading.timestamp` for AC signals is set to `datetime.now(UTC)` at the Windows emitter at the time of shared-memory read, not the simulator's internal cycle timestamp. This introduces a polling jitter of up to one cycle (~20 ms at 50 Hz). OPC UA signals use `SourceTimestamp` from the server, which is more precise.

**Anomaly detector state is not persisted.** The per-sensor sliding buffer is in-process memory. If the gateway restarts, the buffer resets and the first `window_size` samples after restart will not produce statistical alerts.

**MQTT reconnection loses one message.** The message that encounters a `MqttError` is not delivered in the same `send()` call — the client is recreated and the message is routed to SQLite (if `CRITICAL`) or discarded (if `BEST_EFFORT`). The next message uses the restored connection. This is by design for at-least-once semantics but introduces one-cycle latency on reconnection.

**No authentication in the demo stack.** Mosquitto, InfluxDB, and the OPC UA simulator are configured without TLS or token auth for ease of local development. Not suitable for network-exposed deployments without additional hardening.

**`benchmark_worker.py` with `MQTTDestination` requires a live broker.** The benchmark numbers above for MQTT throughput are not reproducible without Docker infrastructure. `MockDestination` numbers are reproducible without any external dependencies.

---

## Quick Start

**Prerequisites:** Docker ≥ 24, Docker Compose v2.

```bash
git clone <repo-url>
cd edge-iiot-gateway

# Copy and configure environment
cp .env.example .env
# Edit .env: set INFLUXDB_TOKEN, INFLUXDB_PASSWORD, GRAFANA_PASSWORD

# Start the full stack
docker compose up --build

# Gateway health check (allow ~15s for startup)
curl http://localhost:8080/health

# Metrics
curl http://localhost:8080/metrics

# Active anomaly alerts
curl http://localhost:8080/alerts
```

Grafana is available at `http://localhost:3000` (default port; configurable via `GRAFANA_PORT` in `.env`).

**Running tests (no Docker required):**

```bash
pip install -r requirements.txt
pytest tests/test_models.py tests/test_pipeline.py -v
```

**Running benchmarks:**

```bash
# SQLite throughput (no external dependencies)
python -m tests.benchmark_sqlite

# Worker throughput with MockDestination (no external dependencies)
python -m tests.benchmark_worker
```

---

## Engineering Principles

**Validation at the boundary.** `SensorReading` validates on construction, before the queue. The interior of the pipeline operates under the invariant that any message it handles has already passed the contract. NaN rejected at entry costs nothing; NaN propagated to InfluxDB corrupts every window-function query that touches that interval.

**Source-agnostic worker.** The worker does not branch on `opc_node_id` or any source identifier. `opc_node_id=None` is a semantic signal (non-OPC UA source), not an error. A `LapEvent` in the queue is handled by type check, not by inspecting payload fields. Adding a third source requires only a new adapter that produces `SensorReading` objects — the worker is unchanged.

**No I/O on the health check path.** `is_available()` returns a cached `_connected` flag updated by `send()`. `opc_ua_connected` returns the state of an `asyncio.Event`. Health endpoints answer in microseconds without network calls.

**Scale when the benchmark says to.** The current architecture uses a single worker, a single SQLite file, and one MQTT connection. The worker handles ~8 300 msg/s with a real broker; current load is ~95 msg/s. No additional concurrency, message bus, or distributed buffer is warranted at this scale.

**Explicit over implicit failure handling.** Every failure mode at every stage has a documented outcome: queue full → discard + log; `send()` False + CRITICAL → SQLite; `send()` False + BEST_EFFORT → discard + log; OPC UA subscription drop → reconnect event + backoff; shutdown with queue items → drain to SQLite or manual persist on timeout. There are no silent failures.
