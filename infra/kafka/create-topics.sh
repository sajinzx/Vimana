#!/bin/bash
# Create all Vimana topics. Idempotent (--if-not-exists).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

H=3600000
D=$((24 * H))

# name:partitions:retention_ms
TOPICS=(
  "adsb.raw:6:$((1 * D))"
  "weather.metar:1:$((7 * D))"
  "adsb.clean:6:$((1 * D))"
  "flight.events:3:$((7 * D))"
  "airport.metrics:3:$((3 * D))"
  "predictions:3:$((7 * D))"
  "alerts:1:$((3 * D))"
  "dlq:1:$((7 * D))"
  "pipeline.metrics:1:$((1 * D))"
  "sim.control:1:$((1 * D))"
)

for spec in "${TOPICS[@]}"; do
  IFS=: read -r name parts retention <<<"$spec"
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor 1 \
    --config "retention.ms=$retention"
done

echo "Topics:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
