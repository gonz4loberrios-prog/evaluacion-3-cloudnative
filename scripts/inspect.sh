#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP="localhost:29092"
GRUPO="ventas-streams-dsy1107"

echo "===== TOPICS / ISR ====="
docker compose exec -T kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --describe
echo "===== CONSUMER GROUPS ====="
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --list
echo "===== LAG: $GRUPO ====="
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GRUPO"
echo "===== ENDPOINT /metrics ====="
curl -fsS http://localhost:18081/metrics || true
