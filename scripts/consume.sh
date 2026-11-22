#!/usr/bin/env bash
set -euo pipefail
TOPIC="${1:-ventas-agregadas}"
MAX="${2:-50}"

# kafka-console-consumer devuelve error al agotar --timeout-ms sin mensajes
# nuevos. Si ya imprimió registros, se trata como fin normal de la consulta.
if output=$(docker compose exec -T kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:29092 --topic "$TOPIC" --from-beginning \
  --timeout-ms 5000 --max-messages "$MAX" \
  --property print.key=true --property key.separator=" | " 2>&1); then
  codigo=0
else
  codigo=$?
fi

printf '%s\n' "$output"

if [ "$codigo" -ne 0 ]; then
  if printf '%s' "$output" | grep -q "TimeoutException" && printf '%s' "$output" | grep -q " | "; then
    echo "(consulta finalizada por --timeout-ms después de entregar mensajes)"
    exit 0
  fi
  exit "$codigo"
fi
