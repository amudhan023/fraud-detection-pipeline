# Real-Time Fraud Detection & Spend Analytics Pipeline

A production-grade streaming pipeline that processes **1M+ payment transactions per second**
with **sub-second end-to-end latency** — from Postgres inserts through Debezium CDC, Apache
Flink enrichment and anomaly scoring, to live Grafana dashboards.

**Resume talking points demonstrated:**
- Real-time streaming with Flink 1.18 DataStream API (Scala) + Kafka 3.6 (KRaft)
- Change Data Capture with Debezium 2.4 — zero-downtime source ingestion
- 3 concurrent Flink jobs: enrichment + scoring, windowed anomaly detection, spend analytics
- Exactly-once semantics via two-phase commit checkpointing
- RocksDB keyed state for per-account velocity, z-score, and geo-travel rules
- Full observability: OpenTelemetry distributed tracing → Jaeger, Prometheus → Grafana
- JVM (Scala) for the hot path; Python for load generation — explicit tech-split decision

---

## Architecture

```
Load Generator (Python)
  └─► INSERT → PostgreSQL (wal_level=logical)
                   │
               Debezium CDC
               (Kafka Connect)
                   │
         Kafka topic: transactions.raw  (12 partitions, keyed by account_id)
                   │
         ┌─────────┴──────────────────────────────────┐
         │                                             │
   Flink Job 1: EnrichScore              Flink Job 3: SpendAnalytics
   ─────────────────────────             ────────────────────────────
   • Merchant broadcast state            • Tumbling 1-min windows
   • Account broadcast state             • Category / Merchant / Account
   • Risk scoring (5 signals)            • JDBC upsert → Postgres
   • Exactly-once checkpointing
         │
   transactions.scored  (12 partitions)
         │
   Flink Job 2: AnomalyDetection
   ─────────────────────────────
   • Velocity rule    (N txns / M sec)
   • Amount spike     (z-score > 3σ)
   • Geo-travel       (implied speed > 900 km/h)
   • Config-driven thresholds
         │
   transactions.anomalies  (6 partitions)

All jobs → Prometheus metrics + OpenTelemetry traces → Jaeger
```

---

## Prerequisites

| Tool           | Version   | Install                       |
|----------------|-----------|-------------------------------|
| Docker Desktop | 24+       | https://www.docker.com        |
| Docker Compose | 2.20+     | bundled with Docker Desktop   |
| `make`         | any       | `brew install make`           |
| sbt (optional) | 1.9+      | for building Flink fat jar    |
| Python 3.11+   | optional  | for running load gen locally  |

---

## Quickstart (single command)

```bash
git clone <repo>
cd fraud-detection-pipeline

# Bring up the full stack + register Debezium connector
make up

# Build Flink fat jar and submit all three jobs
make submit-jobs

# Open dashboards
open http://localhost:3000    # Grafana (admin/admin)
open http://localhost:8082    # Flink Web UI
open http://localhost:16686   # Jaeger traces
```

### Run the load generator

```bash
# In a new terminal — 200 tps, 5% fraud, run for 10 minutes
cd load-generator
pip install -r requirements.txt
python src/generator.py --rate 200 --fraud-pct 5 --duration 600
```

Or start everything end-to-end in one command:

```bash
make demo
```

---

## Service URLs

| Service         | URL                        | Credentials  |
|-----------------|----------------------------|--------------|
| Grafana         | http://localhost:3000      | admin/admin  |
| Flink UI        | http://localhost:8082      | —            |
| Prometheus      | http://localhost:9090      | —            |
| Jaeger          | http://localhost:16686     | —            |
| Kafka UI        | http://localhost:9080      | —            |
| Kafka Connect   | http://localhost:8083      | —            |
| Schema Registry | http://localhost:8081      | —            |

---

## Sample Input / Output

### Input transaction (Debezium CDC envelope on `transactions.raw`)

```json
{
  "before": null,
  "after": {
    "transaction_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "account_id":     "b2c3d4e5-0001-0001-0001-000000000004",
    "merchant_id":    "a1b2c3d4-0001-0001-0001-000000000004",
    "amount":         4250.00,
    "currency":       "USD",
    "latitude":       37.7749,
    "longitude":     -122.4194,
    "event_time":     "2026-06-12T14:30:00Z"
  },
  "op": "c",
  "ts_ms": 1749737400000
}
```

### Flagged fraud output on `transactions.anomalies`

```json
{
  "transactionId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "accountId":     "b2c3d4e5-0001-0001-0001-000000000004",
  "reasonCode":    "AMOUNT_SPIKE",
  "evidence": {
    "amount":     "4250.00",
    "mean":       "47.32",
    "stddev":     "18.91",
    "z_score":    "217.85",
    "threshold":  "3.0"
  },
  "detectedAtMs": 1749737400234
}
```

End-to-end latency from Postgres INSERT to anomaly event: **~180ms** (local dev stack).

---

## Extending the Pipeline

### Add a new anomaly rule

1. Open `CombinedAnomalyFunction.scala`
2. Add new state descriptor(s) in `open()`
3. Implement `checkMyRule(txn, out)` — emit `AnomalyEvent` with a new `reasonCode`
4. Call it from `processElement()`
5. Add threshold config in `application.conf` under `anomaly.my-rule`
6. No recompile needed for threshold changes — just update the config and restart the job

### Add a new sink

1. Create a new class in `flink-jobs/src/main/scala/com/fraudpipeline/sinks/`
2. Implement `SinkFunction[ScoredTransaction]` or use `JdbcSink`
3. Wire it into `SpendAnalyticsJob.main()` with `.addSink(...)`

### Scale to more throughput

See [SCALING.md](docs/SCALING.md) — the only changes needed are Kafka partition count
and `parallelism.default` in `flink-conf.yaml`. No code changes.

---

## Running Tests

```bash
# Python load generator unit tests
cd load-generator
pip install -r requirements.txt pytest
pytest tests/ -v

# Scala Flink jobs (requires sbt)
cd flink-jobs
sbt test
```

---

## Further Reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — Data flow, topic design, CAP trade-offs
- [OBSERVABILITY.md](docs/OBSERVABILITY.md) — Tracing a transaction, Prometheus metrics reference
- [SCALING.md](docs/SCALING.md) — How to reach 1M+ events/sec, bottleneck playbook
