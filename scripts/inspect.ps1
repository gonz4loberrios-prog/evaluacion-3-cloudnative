$ErrorActionPreference = "Stop"
$bootstrap = "localhost:29092"
$grupo = "ventas-streams-dsy1107"

Write-Host "===== TOPICS / ISR ====="
docker compose exec -T kafka-1 kafka-topics --bootstrap-server $bootstrap --describe

Write-Host "`n===== CONSUMER GROUPS ====="
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server $bootstrap --list

Write-Host "`n===== LAG: $grupo ====="
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server $bootstrap --describe --group $grupo

Write-Host "`n===== ENDPOINT /metrics ====="
try { (Invoke-WebRequest -UseBasicParsing "http://localhost:18081/metrics").Content } catch { Write-Host "No disponible: $_" }
