#!/usr/bin/env bash
set -euo pipefail
RESET="${1:-}"
BOOTSTRAP="localhost:29092"
APP="${APPLICATION_ID:-ventas-streams-dsy1107}"
TOPICS=(
  "ventas-raw"
  "ventas-agregadas"
  "ventas-raw-dlq"
  # Kafka Streams no puede crearlos con auto.create.topics.enable=false: se aprovisionan aquí.
  "$APP-por-tienda-repartition"
  "$APP-ventanas-store-repartition"
  "$APP-dedup-store-changelog"
  "$APP-eventtime-store-changelog"
  "$APP-ventanas-store-changelog"
)

if [[ "$RESET" == "--reset" ]]; then
  for t in "${TOPICS[@]}"; do
    docker compose exec -T kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --if-exists --topic "$t"
  done
  sleep 3
fi

for t in "${TOPICS[@]}"; do
  extra=(--config min.insync.replicas=1 --config retention.ms=604800000)
  if [[ "$t" == *-changelog ]]; then
    extra=(--config min.insync.replicas=1 --config cleanup.policy=compact)
  fi
  docker compose exec -T kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$t" --partitions 4 --replication-factor 2 "${extra[@]}"
done

echo "--- Descripción de tópicos ---"
docker compose exec -T kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --describe
