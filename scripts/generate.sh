#!/usr/bin/env bash
set -euo pipefail
SCENARIO="${1:-all}"
BOOTSTRAP="${2:-kafka-1:29092,kafka-2:29092}"
BASE_TIME="${3:-2026-01-01T10:00:00Z}"
TOPIC="${4:-ventas-raw}"

docker compose exec -T streams-app java -cp "/app/ventas-streams.jar:/app/lib/*" \
  cl.dsy1107.ventas.generator.EventGenerator \
  --bootstrap "$BOOTSTRAP" --topic "$TOPIC" --scenario "$SCENARIO" --base-time "$BASE_TIME"
