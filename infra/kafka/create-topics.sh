#!/usr/bin/env bash
# Creates all required Kafka topics. Run once after Kafka is healthy.
set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"

create_topic() {
  local name=$1 partitions=$2 retention_ms=$3 cleanup=$4
  kafka-topics.sh \
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
