#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP="localhost:29092"
TOPIC="ventas-raw"
GROUP="ventas-streams-dsy1107"

mostrar_isr() {
  echo "===== ISR $1 ====="
  docker compose exec -T kafka-1 kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
}

mostrar_lag() {
  echo "===== LAG $1 ====="
  docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" \
    --describe --group "$GROUP"
}

mostrar_isr "ANTES (2 brokers, RF=2)"

echo "Deteniendo kafka-2..."
docker compose stop kafka-2
sleep 6
mostrar_isr "DURANTE (kafka-2 detenido)"

echo "Produciendo con acks=all contra el broker que queda..."
echo "(acks=-1 es el valor interno equivalente a 'all'; enable.idempotence=true)"
docker compose exec -T streams-app java -cp "/app/ventas-streams.jar:/app/lib/*" \
  cl.dsy1107.ventas.generator.EventGenerator \
  --bootstrap kafka-1:29092 --topic ventas-raw --scenario valid \
  --base-time 2026-01-03T10:00:00Z 2>&1 |
  grep -E "acks =|enable.idempotence|idempotent producer|ProducerId set|^\[valid\]" | tail -12

echo "Cerrando las ventanas del escenario de failover..."
docker compose exec -T streams-app java -cp "/app/ventas-streams.jar:/app/lib/*" \
  cl.dsy1107.ventas.generator.EventGenerator \
  --bootstrap kafka-1:29092 --topic ventas-raw --scenario flush \
  --base-time 2026-01-03T10:00:00Z 2>&1 |
  grep -E "^\[flush\]" | tail -3

sleep 5
mostrar_lag "DURANTE LA FALLA"

echo "Reiniciando kafka-2..."
docker compose start kafka-2
sleep 15
mostrar_isr "DESPUES (kafka-2 reiniciado)"
mostrar_lag "TRAS RECUPERAR EL BROKER"

echo "===== METRICAS ANTES DEL REINICIO ====="
curl -s http://localhost:18081/metrics | grep -E "^# TYPE ventas_|^ventas_" || true

echo "Reiniciando streams-app para demostrar la restauración desde changelogs..."
docker compose restart streams-app
sleep 55

echo "===== RESTAURACIÓN / CONTINUIDAD ====="
docker compose logs --since 90s streams-app 2>&1 |
  grep -E "Finished restoring changelog|Restored and ready to run|Restoration took|Estado Kafka Streams|Processed|ERROR" | tail -40

mostrar_lag "TRAS RESTAURAR EL ESTADO"
echo "Prueba de failover finalizada."
