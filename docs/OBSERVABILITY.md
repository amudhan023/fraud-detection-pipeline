# Observability Guide

## Overview

The pipeline has three observability layers:

| Layer    | Tool         | What it covers                               |
|----------|--------------|----------------------------------------------|
| Metrics  | Prometheus + Grafana | Throughput, latency, consumer lag, fraud rate |
| Tracing  | OpenTelemetry → Jaeger | End-to-end transaction trace across all jobs |
| Logging  | Log4j2 → stdout | Per-event DEBUG logs, anomaly reason codes  |

---

## 1. Tracing a Single Transaction End-to-End

Every transaction carries a trace ID from the moment it is written to Postgres through to
the final anomaly or spend-aggregate output.

### How trace propagation works

```
Load Generator
  └─► INSERT into Postgres
       └─► Debezium CDC → Kafka header: traceparent=00-<traceId>-<spanId>-01
            └─► EnrichScoreJob reads the header, continues the trace
                 └─► RiskScoringFunction adds span: "score-transaction"
                      └─► Emits to transactions.scored with header forwarded
                           └─► AnomalyDetectionJob reads header, adds span: "detect-anomaly"
                                └─► If flagged → AnomalyEvent with span: "emit-anomaly"
```

### Finding a trace in Jaeger

1. Open Jaeger UI at http://localhost:16686
2. Service: `fraud-pipeline`  →  Operation: `score-transaction`
3. Search by `transactionId` tag: paste any UUID from the Postgres `transactions` table
4. Click the trace → expand spans to see each stage with timing

### Key spans

| Span name             | Job                   | Attributes                              |
|-----------------------|-----------------------|-----------------------------------------|
| `enrich-transaction`  | EnrichScoreJob        | `account_id`, `merchant_id`, `op`       |
| `score-transaction`   | RiskScoringFunction   | `risk_score`, `merchant_risk`           |
| `detect-anomaly`      | AnomalyDetectionJob   | `rule`, `account_id`                    |
| `emit-anomaly`        | CombinedAnomalyFunction | `reason_code`, `evidence`             |
| `aggregate-spend`     | SpendAnalyticsJob     | `dimension`, `window_start`             |

---

## 2. Prometheus Metrics Reference

All metrics are exported by the Flink metrics reporter on port `:9249`.
Kafka and Postgres metrics are scraped from their exporters.

### Custom fraud pipeline metrics

| Metric                                          | Type    | Description                              |
|-------------------------------------------------|---------|------------------------------------------|
| `fraud_pipeline_events_processed_total`         | Counter | Total events processed by a job          |
| `fraud_pipeline_late_events_dropped_total`      | Counter | Events dropped for being too late         |
| `fraud_pipeline_anomalies_detected_total`       | Counter | Total anomaly events emitted             |
| `fraud_pipeline_enrichment_misses_total`        | Counter | Merchant or account lookup misses        |
| `fraud_pipeline_velocity_flags_total`           | Counter | Velocity rule triggers                   |
| `fraud_pipeline_amount_spike_flags_total`       | Counter | Amount spike rule triggers               |
| `fraud_pipeline_geo_travel_flags_total`         | Counter | Geo-travel rule triggers                 |

### Key Flink metrics (auto-exported)

| Metric                                          | What to watch                             |
|-------------------------------------------------|-------------------------------------------|
| `flink_jobmanager_job_uptime`                   | Job uptime — alerts if it resets          |
| `flink_taskmanager_job_task_busyTimeMsPerSecond` | > 800 = TaskManager overloaded           |
| `flink_taskmanager_job_task_numRecordsInPerSecond` | Throughput                             |
| `flink_jobmanager_job_lastCheckpointDuration`   | Checkpoint latency — target < 5s         |
| `flink_jobmanager_job_numberOfInProgressCheckpoints` | > 1 = checkpoint stall alert        |

### Kafka consumer lag

```promql
# Consumer lag for EnrichScore job
kafka_consumergroup_lag{consumergroup="flink-enrich-score-cg"}

# Lag across all fraud pipeline groups
kafka_consumergroup_lag{consumergroup=~"flink-.*-cg"}
```

### Alert queries (paste into Grafana)

```promql
# Fraud rate over last 5 minutes
rate(fraud_pipeline_anomalies_detected_total[5m])

# Enrichment miss rate (indicates broadcast state lag)
rate(fraud_pipeline_enrichment_misses_total[1m])
  / rate(fraud_pipeline_events_processed_total[1m])

# Checkpoint taking > 10s
flink_jobmanager_job_lastCheckpointDuration > 10000
```

---

## 3. Grafana Dashboard Navigation

Open Grafana at http://localhost:3000 (admin/admin).

### Real-Time Traffic dashboard
- **Throughput panel**: `rate(flink_taskmanager_job_task_numRecordsInPerSecond[1m])`
- **Latency p99**: derived from checkpoint duration as a proxy
- **Kafka lag heatmap**: per consumer group × partition

### Fraud Monitoring dashboard
- **Live anomaly feed**: query `anomaly_events` Postgres table (last 100 rows)
- **Reason code breakdown**: pie chart of `reason_code` counts
- **Fraud rate over time**: `rate(fraud_pipeline_anomalies_detected_total[5m])`

### Spend Analytics dashboard
- Pulls from the `spend_aggregates` Postgres table
- Category breakdown: bar chart by `dimension_value` where `dimension = 'CATEGORY'`
- Top accounts by spend: ranked table

---

## 4. Diagnosing Common Issues

### "Consumer lag keeps growing"
1. `make topic-list` — verify topics exist
2. `make connector-status` — verify Debezium is RUNNING
3. Check Flink Web UI (http://localhost:8082) — is the job RUNNING?
4. Look at TaskManager busy ratio — if > 900ms/s, add TaskManagers via `--scale taskmanager=N`

### "No anomalies appearing"
1. Confirm load generator is injecting fraud: `--fraud-pct` > 0
2. Check `fraud_pipeline_velocity_flags_total` counter in Prometheus
3. Verify `transactions.scored` topic has messages: `make topic-list`
4. Lower thresholds in `application.conf` (`anomaly.velocity.max-transactions`)

### "Checkpoints failing"
1. Check `flink_jobmanager_job_lastCheckpointDuration` — if high, increase checkpoint interval
2. Verify RocksDB volume has space: `docker exec jobmanager df -h /flink-checkpoints`
3. Check for backpressure: Flink UI → job graph → backpressure indicator on operators
