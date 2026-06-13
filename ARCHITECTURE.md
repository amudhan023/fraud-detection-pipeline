# Architecture: Real-Time Fraud Detection & Spend Analytics Pipeline

## Overview

A production-grade streaming pipeline that ingests payment transactions via Change Data Capture,
enriches and scores them in real time with Apache Flink, detects fraud patterns through windowed
anomaly rules, and surfaces results through live Grafana dashboards — all with sub-second
end-to-end latency at 1M+ events/sec.

---

## End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DATA INGESTION LAYER                                   │
│                                                                                 │
│   Load Generator (Python)                                                       │
│   └─► INSERT/UPDATE ──► PostgreSQL (accounts, transactions, merchants)          │
│                              │  wal_level=logical                               │
│                              │  replication slot                                │
│                              ▼                                                  │
│                         Debezium CDC                                            │
│                         (Kafka Connect)                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            KAFKA MESSAGE BUS                                    │
│                                                                                 │
│  Topic: transactions.raw          (partitioned by account_id, 12 partitions)   │
│  Topic: transactions.scored       (partitioned by account_id, 12 partitions)   │
│  Topic: transactions.anomalies    (partitioned by account_id, 6 partitions)    │
│  Topic: analytics.spend           (partitioned by category, 6 partitions)      │
│  Topic: enrichment.merchants      (compacted, broadcast state source)          │
│  Topic: enrichment.accounts       (compacted, broadcast state source)          │
└─────────────────────────────────────────────────────────────────────────────────┘
          │                    │                         │
          ▼                    ▼                         ▼
┌──────────────────┐  ┌─────────────────┐    ┌───────────────────────┐
│  FLINK JOB 1     │  │  FLINK JOB 2    │    │  FLINK JOB 3          │
│  EnrichScore     │  │  AnomalyDetect  │    │  SpendAnalytics       │
│                  │  │                 │    │                       │
│  • Broadcast     │  │  • Velocity     │    │  • Tumbling windows   │
│    state lookup  │  │    (N/M rule)   │    │    (1m, 5m, 1h)       │
│  • Risk scoring  │  │  • Amount spike │    │  • Spend by category  │
│  • Watermarks    │  │    (z-score)    │    │  • Spend by merchant  │
│  • Checkpoints   │  │  • Geo-travel   │    │  • Per-account totals │
└──────────────────┘  └─────────────────┘    └───────────────────────┘
          │                    │                         │
          ▼                    ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SINK LAYER                                         │
│                                                                                 │
│  PostgreSQL (operational queries, fraud case management)                        │
│  Apache Iceberg (long-term analytics, time-travel queries)                      │
│  Prometheus (metrics scrape endpoint exposed by Flink + custom exporters)       │
└─────────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          OBSERVABILITY LAYER                                    │
│                                                                                 │
│  Grafana ◄── Prometheus ◄── Flink Metrics + Custom Exporters                   │
│  Jaeger  ◄── OpenTelemetry Collector ◄── OTel SDK (Flink + Load Gen)           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Versions

| Component            | Version   | Rationale                                              |
|----------------------|-----------|--------------------------------------------------------|
| Apache Flink         | 1.18.1    | Latest stable; Scala 2.12 binaries available           |
| Scala                | 2.12.17   | Flink 1.18 ships Scala 2.12 artifacts                  |
| Apache Kafka         | 3.6.x     | KRaft mode (no ZooKeeper), improved partition perf     |
| Debezium             | 2.4.x     | Stable CDC; PostgreSQL 15+ logical replication support |
| Kafka Connect        | 3.6.x     | Bundled with Debezium connector                        |
| PostgreSQL           | 15        | Logical replication, `wal_level=logical` required      |
| Apache Iceberg       | 1.4.x     | Flink connector available; time-travel support         |
| Prometheus           | 2.48.x    | Flink JMX → Prometheus reporter                        |
| Grafana              | 10.2.x    | Provisioned dashboards as code                         |
| OpenTelemetry        | 1.32.x    | Java agent + Collector; Kafka header propagation       |
| Jaeger               | 1.52.x    | OTLP receiver; distributed trace UI                    |
| Python               | 3.11      | Load generator and tooling                             |

---

## Kafka Topic Design

### Partitioning Strategy

All transaction topics partition by `account_id` using consistent hashing. This guarantees:
- All events for a given account arrive in order at the same Flink subtask (no cross-partition joins for per-account state)
- Keyed state in Flink (velocity counters, rolling averages) is co-located with the incoming record
- Consumer lag can be monitored per-partition to detect hot partitions

### Topic Specifications

```
transactions.raw
  Partitions : 12
  Replication: 3
  Retention  : 7 days
  Key        : account_id (string)
  Value      : Debezium envelope JSON → Avro (schema registry)
  Compaction : none (append-only event log)

transactions.scored
  Partitions : 12
  Replication: 3
  Retention  : 7 days
  Key        : transaction_id (string)
  Value      : enriched + scored transaction (Avro)

transactions.anomalies
  Partitions : 6
  Replication: 3
  Retention  : 30 days
  Key        : account_id (string)
  Value      : anomaly event with reason code + evidence (Avro)

analytics.spend
  Partitions : 6
  Replication: 3
  Retention  : 14 days
  Key        : window_start|dimension|dimension_value
  Value      : aggregated spend record (Avro)

enrichment.merchants   (compacted)
  Partitions : 3
  Compaction : enabled (latest merchant state per merchant_id)
  Use        : Flink broadcast state; replayed on job restart

enrichment.accounts    (compacted)
  Partitions : 3
  Compaction : enabled (latest account state per account_id)
  Use        : Flink broadcast state; replayed on job restart
```

### Schema Evolution Policy

- Avro schemas stored in Confluent Schema Registry (Schema Registry container in compose)
- All schema changes must be backward-compatible (add optional fields with defaults)
- Debezium envelope includes `before`/`after`/`op`/`ts_ms` fields; consumers read `after` for inserts/updates

---

## Flink Job Architecture

### Job 1: EnrichScore (`EnrichScoreJob`)

```
KafkaSource[RawTransaction]
  └─► assignTimestampsAndWatermarks (BoundedOutOfOrdernessWatermarks, 5s tolerance)
  └─► keyBy(accountId)
  └─► connect(BroadcastStream[MerchantLookup])
  └─► EnrichmentFunction (BroadcastProcessFunction)
        ├─ merchant risk category lookup
        ├─ account tier lookup
        └─ null-safe defaults when lookup missing
  └─► RiskScoringFunction (KeyedProcessFunction)
        ├─ base score from transaction amount
        ├─ merchant risk multiplier
        ├─ account history modifier (ValueState[AccountProfile])
        └─ time-of-day / geographic risk factor
  └─► KafkaSink[ScoredTransaction] → transactions.scored
```

**Checkpointing**: RocksDB state backend, checkpoints every 30s, exactly-once semantics.
**Watermark strategy**: `BoundedOutOfOrdernessWatermarks` with 5-second tolerance. Late events
(up to 10s) are routed to a side output for separate handling; events older than 10s are dropped
with a metric increment.

### Job 2: AnomalyDetect (`AnomalyDetectionJob`)

```
KafkaSource[ScoredTransaction]
  └─► keyBy(accountId)
  └─► AnomalyDetectionFunction (KeyedProcessFunction)
        ├─ VelocityRule: ListState[Timestamp], sliding count in window
        ├─ AmountSpikeRule: ValueState[RunningStats], z-score
        └─ GeoTravelRule: ValueState[LastLocation + Timestamp], speed check
  └─► filter(_.isAnomaly)
  └─► KafkaSink[AnomalyEvent] → transactions.anomalies
```

### Job 3: SpendAnalytics (`SpendAnalyticsJob`)

```
KafkaSource[ScoredTransaction]
  └─► keyBy(category)
  └─► window(TumblingEventTimeWindows.of(Time.minutes(1)))
  └─► SpendAggregateFunction (AggregateFunction)
  └─► PostgresSink + IcebergSink → analytics.spend
```

---

## CAP Theorem Trade-offs

This system operates in a **Partition-tolerant + Available (AP)** mode for the hot path, with
CP guarantees available for specific operations:

### Hot Path (real-time scoring): AP

| Property      | Choice         | Reasoning                                                  |
|---------------|----------------|------------------------------------------------------------|
| Consistency   | Eventual       | Enrichment broadcast state may lag by seconds; acceptable  |
| Availability  | High           | Flink continues processing even if enrichment topic lags   |
| Partition     | Tolerated      | Kafka replicas provide redundancy; Flink restarts from ckpt|

**Trade-off accepted**: A transaction scored during a merchant-lookup broadcast lag will use the
previous merchant risk category. The error window is bounded by checkpoint interval (30s max).
This is acceptable because fraud rules have high recall tolerance — a slightly stale merchant
risk factor does not miss critical velocity or amount-spike signals.

### Checkpointing / State (exactly-once): CP

Flink + Kafka exactly-once delivery requires coordinator agreement (two-phase commit).
During a checkpoint barrier, throughput momentarily dips. We accept this latency spike
every 30 seconds in exchange for no duplicate or missing events in the sink.

### Analytics Sinks: AP → CP upgrade path

The Iceberg sink provides ACID guarantees at the table level (snapshot isolation). Postgres
sink uses upsert-on-conflict for idempotency. Both tolerate temporary network partitions via
retry with exponential backoff.

---

## Ordering Guarantees

Debezium captures WAL events in commit order. Each `(account_id, sequence_number)` tuple
is monotonically increasing within a Kafka partition. Flink respects per-partition ordering.
Cross-partition ordering is intentionally not guaranteed — no business rule requires
global ordering across accounts.

---

## Schema Evolution Handling

1. Debezium emits a schema field in every envelope — consumers can detect changes
2. Avro Schema Registry enforces backward compatibility before a connector restart
3. Flink jobs register Avro schema at startup; new optional fields are ignored by older job
   versions until redeployed
4. Breaking changes (field renames, type changes) require a blue-green job deployment with
   a shadow topic during cutover

---

## Directory Structure

```
fraud-detection-pipeline/
├── ARCHITECTURE.md               ← this file
├── Makefile                      ← make up / make down / make demo
├── flink-jobs/                   ← Scala (Flink DataStream API)
│   ├── build.sbt
│   ├── project/
│   └── src/
│       ├── main/
│       │   ├── scala/com/fraudpipeline/
│       │   │   ├── enrichment/   ← EnrichScoreJob, EnrichmentFunction
│       │   │   ├── scoring/      ← RiskScoringFunction
│       │   │   ├── anomaly/      ← AnomalyDetectionJob, rules
│       │   │   ├── sinks/        ← SpendAnalyticsJob, Postgres/Iceberg sinks
│       │   │   └── utils/        ← Avro serialization, config, metrics
│       │   └── resources/
│       │       ├── application.conf
│       │       └── log4j2.xml
│       └── test/scala/com/fraudpipeline/
├── load-generator/               ← Python
│   ├── src/
│   │   ├── generator.py          ← main entrypoint
│   │   ├── fraud_patterns.py     ← fraudulent scenario injection
│   │   └── db.py                 ← Postgres connection + INSERT logic
│   ├── config/
│   │   └── generator.yaml
│   ├── tests/
│   └── requirements.txt
├── infra/
│   ├── docker-compose.yml        ← full stack
│   ├── kafka/
│   │   ├── create-topics.sh
│   │   └── connector-config.json ← Debezium connector registration
│   ├── postgres/
│   │   ├── init.sql              ← schema + seed data
│   │   └── postgresql.conf       ← wal_level=logical
│   ├── flink/
│   │   └── flink-conf.yaml
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── otel/
│   │   └── otel-collector.yaml
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources/
│   │   │   └── dashboards/
│   │   └── dashboards/
│   └── jaeger/
├── dashboards/                   ← Grafana JSON (exported + versioned)
│   ├── realtime-traffic.json
│   ├── fraud-monitoring.json
│   └── spend-analytics.json
└── docs/
    ├── OBSERVABILITY.md
    └── SCALING.md
```
