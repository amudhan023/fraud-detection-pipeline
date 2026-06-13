# Scaling Guide — Reaching 1M+ Events/Second

## Baseline Architecture (as shipped)

| Component       | Local dev config          | Target at 1M tps            |
|-----------------|---------------------------|-----------------------------|
| Kafka           | 1 broker, 12 partitions   | 3+ brokers, 48+ partitions  |
| Flink           | 2 TaskManagers, 4 slots   | 10+ TaskManagers, 8 slots   |
| Postgres sink   | Single JDBC sink          | Connection pool + sharding  |
| State backend   | RocksDB (incremental ckpt)| RocksDB (same, tuned)       |

---

## Kafka Partition Strategy

Partitions are the unit of parallelism in Kafka and the ceiling on Flink operator parallelism.

```
partitions_needed = ceil(target_tps / throughput_per_partition)

# Rule of thumb: 1 partition ≈ 20–30 MB/s write throughput on commodity hardware.
# At 1M events/s with avg payload 500 bytes:
#   throughput = 1M × 500B = ~500 MB/s
#   partitions = ceil(500 / 25) = 20 per topic
# Add 2× headroom → 48 partitions for transactions.raw / transactions.scored
```

**How to change partition count without restarting jobs:**

```bash
# Increase partitions (Kafka only allows increases, not decreases)
kafka-topics.sh --bootstrap-server kafka:29092 \
  --alter --topic transactions.raw --partitions 48
```

Flink will automatically rebalance across new partitions after a job restart or
on the next Kafka consumer group rebalance.

---

## Flink Parallelism

### Key settings

```yaml
# flink-conf.yaml — no code change needed
parallelism.default: 16           # was 4
taskmanager.numberOfTaskSlots: 8  # was 4
```

### Scaling TaskManagers (docker compose)

```bash
docker compose -f infra/docker-compose.yml up -d --scale taskmanager=10
```

This adds 10 TaskManagers × 8 slots = 80 execution slots.
A job with parallelism=40 will use 40 of those slots.

### Parallelism heuristic

```
operator_parallelism = kafka_topic_partitions
                     = 48 (at 1M tps target)
```

Always keep `operator_parallelism ≤ kafka_topic_partitions` to avoid idle subtasks.

---

## RocksDB State Backend Tuning

The anomaly detection job is the most state-heavy. Each account maintains:
- `ListState[Long]` (velocity timestamps)
- `ValueState[RunningStats]` (amount history)
- `ValueState[(Double,Double,Long)]` (geo location)

### Key RocksDB options (set via `application.conf` or env vars)

```
# In flink-conf.yaml
state.backend.rocksdb.block.cache-size: 256mb
state.backend.rocksdb.writebuffer.size: 64mb
state.backend.rocksdb.writebuffer.count: 3
state.backend.rocksdb.compaction.style: LEVEL
state.backend.rocksdb.use-bloom-filter: true
```

### Incremental checkpoints

```yaml
state.backend.incremental: true
```

At 1M tps with 1M accounts in state, full checkpoint can be 50–200 GB.
Incremental checkpoints only upload the delta (typically < 1% per interval).
Target checkpoint size < 2 GB/interval.

---

## Checkpoint Interval Trade-offs

| Interval | Recovery time | Overhead during steady state |
|----------|---------------|------------------------------|
| 5s       | Low (~5s)     | High — barriers every 5s    |
| 30s      | Moderate      | Low — default                |
| 5min     | High          | Minimal                      |

**Recommended production setting**: 30s interval, 60s timeout.
If `lastCheckpointDuration > 10s`, reduce frequency of external writes or increase
checkpoint timeout. Never let `minPauseBetweenCheckpoints` fall below 5s.

---

## Network Buffer Tuning

At high throughput, network buffer exhaustion causes backpressure cascades.

```yaml
# flink-conf.yaml
taskmanager.network.memory.fraction: 0.15       # was 0.1
taskmanager.network.memory.min: 256mb
taskmanager.network.memory.max: 2gb
taskmanager.network.numberOfBuffers: 4096        # was 2048
```

---

## What to Watch (Bottleneck Playbook)

### Symptom 1: Kafka consumer lag growing

```
Check: kafka_consumergroup_lag{consumergroup="flink-enrich-score-cg"}
Fix:   Increase Flink parallelism AND Kafka partition count
```

### Symptom 2: TaskManager busy ratio > 90%

```
Check: flink_taskmanager_job_task_busyTimeMsPerSecond > 900
Fix:   --scale taskmanager=N (add more TaskManagers)
       or increase taskmanager.numberOfTaskSlots
```

### Symptom 3: Checkpoint duration > 10s

```
Check: flink_jobmanager_job_lastCheckpointDuration > 10000
Root causes:
  a) State is too large → tune RocksDB write buffers
  b) Kafka sink is slow → switch to AT_LEAST_ONCE delivery
  c) Network congestion → tune network buffers (see above)
```

### Symptom 4: GC pauses > 200ms

```
Fix: Increase JVM heap in docker-compose FLINK_PROPERTIES:
     taskmanager.memory.process.size: 4096m
     Set -XX:+UseG1GC -XX:MaxGCPauseMillis=200 in env.java.opts.taskmanager
```

### Symptom 5: Postgres sink is the bottleneck

```
Fix:
  - Increase JdbcExecutionOptions.batchSize to 500+
  - Use connection pooling (PgBouncer sidecar)
  - Partition the spend_aggregates table by window_start (monthly range)
  - For >1M tps: replace Postgres sink with Iceberg-only and query via Trino
```

---

## Kubernetes Path (beyond compose)

```yaml
# Flink Kubernetes Operator deployment excerpt
spec:
  job:
    parallelism: 48
  taskManager:
    resource:
      memory: "4096m"
      cpu: 4
    replicas: 12   # 12 × 4 slots = 48
```

Each `taskmanager.numberOfTaskSlots` should match available CPU cores per pod.
Use pod anti-affinity to spread TaskManagers across nodes to avoid hot-spot failures.
