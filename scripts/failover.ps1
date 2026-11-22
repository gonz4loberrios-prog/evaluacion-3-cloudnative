$ErrorActionPreference = "Continue"
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $OutputEncoding = [System.Text.Encoding]::UTF8
} catch { }

$bootstrap = "localhost:29092"
$tema = "ventas-raw"
$grupo = "ventas-streams-dsy1107"

function Mostrar-ISR([string]$etiqueta) {
    Write-Host "`n===== ISR $etiqueta ====="
    docker compose exec -T kafka-1 kafka-topics --bootstrap-server $bootstrap --describe --topic $tema
}

function Mostrar-Lag([string]$etiqueta) {
    Write-Host "`n===== LAG $etiqueta ====="
    docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server $bootstrap --describe --group $grupo
}

Mostrar-ISR "ANTES (2 brokers, RF=2)"

Write-Host "`nDeteniendo kafka-2..."
docker compose stop kafka-2 2>$null
Start-Sleep -Seconds 6
Mostrar-ISR "DURANTE (kafka-2 detenido)"

Write-Host "`nProduciendo con acks=all contra el broker que queda..."
Write-Host "(acks=-1 es el valor interno equivalente a 'all'; enable.idempotence=true)"
docker compose exec -T streams-app java -cp "/app/ventas-streams.jar:/app/lib/*" `
    cl.dsy1107.ventas.generator.EventGenerator `
    --bootstrap kafka-1:29092 --topic ventas-raw --scenario valid `
    --base-time 2026-01-03T10:00:00Z 2>&1 |
    Select-String -Pattern "acks =|enable.idempotence|idempotent producer|ProducerId set|^\[valid\]" |
    Select-Object -Last 12

Write-Host "`nCerrando las ventanas del escenario de failover..."
docker compose exec -T streams-app java -cp "/app/ventas-streams.jar:/app/lib/*" `
    cl.dsy1107.ventas.generator.EventGenerator `
    --bootstrap kafka-1:29092 --topic ventas-raw --scenario flush `
    --base-time 2026-01-03T10:00:00Z 2>&1 |
    Select-String -Pattern "^\[flush\]" |
    Select-Object -Last 3

Start-Sleep -Seconds 5
Mostrar-Lag "DURANTE LA FALLA"

Write-Host "`nReiniciando kafka-2..."
docker compose start kafka-2 2>$null
Start-Sleep -Seconds 15
Mostrar-ISR "DESPUES (kafka-2 reiniciado)"
Mostrar-Lag "TRAS RECUPERAR EL BROKER"

Write-Host "`n===== METRICAS ANTES DEL REINICIO ====="
try {
    $metricas = (Invoke-WebRequest -UseBasicParsing "http://localhost:18081/metrics").Content -split "`n"
    $metricas | Where-Object { $_ -match "^# TYPE ventas_" -or $_ -match "^ventas_" }
} catch {
    Write-Host "No disponible: $_"
}

Write-Host "`nReiniciando streams-app para demostrar la restauracion desde changelogs..."
docker compose restart streams-app 2>$null
Start-Sleep -Seconds 55

Write-Host "`n===== RESTAURACION / CONTINUIDAD ====="
docker compose logs --since 90s streams-app 2>&1 |
    Select-String -Pattern "Finished restoring changelog|Restored and ready to run|Restoration took|Estado Kafka Streams|Processed|ERROR" |
    Select-Object -Last 40

Mostrar-Lag "TRAS RESTAURAR EL ESTADO"
Write-Host "`nPrueba de failover finalizada."
exit 0
