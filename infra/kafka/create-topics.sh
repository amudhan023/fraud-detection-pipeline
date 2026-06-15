#!/usr/bin/env bash
# Creates all required Kafka topics. Run once after Kafka is healthy.
set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
# Confluent CP images expose kafka-topics (no .sh); plain Apache uses kafka-topics.sh
KAFKA_TOPICS_CMD=$(command -v kafka-topics 2>/dev/null || command -v kafka-topics.sh 2>/dev/null || echo "kafka-topics")

create_topic() {
  local name=$1 partitions=$2 retention_ms=$3 cleanup=$4
  $KAFKA_TOPICS_CMD \
    --bootstrap-server "$KAFKA_BROKER" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$retention_ms" \
    --config cleanup.policy="$cleanup"
  echo "  [ok] $name ($partitions partitions)"
}

echo "Creating Kafka topics..."
#                  name                         parts  retention_ms     cleanup
create_topic "transactions.raw"                   12   604800000        "delete"
create_topic "transactions.scored"                12   604800000        "delete"
create_topic "transactions.anomalies"              6   2592000000       "delete"
create_topic "analytics.spend"                     6   1209600000       "delete"
create_topic "enrichment.merchants"                3   -1               "compact"
create_topic "enrichment.accounts"                 3   -1               "compact"
echo "All topics ready."
