param(
    [string]$Topic = "ventas-agregadas",
    [int]$MaxMessages = 50
)

# kafka-console-consumer termina con TimeoutException cuando se agota --timeout-ms
# sin mensajes nuevos. Eso es esperable después de leer todo el tópico; si ya
# imprimió registros, el script termina con éxito para no ensuciar la evidencia.
$ErrorActionPreference = "Continue"

$salida = docker compose exec -T kafka-1 kafka-console-consumer `
    --bootstrap-server localhost:29092 --topic $Topic --from-beginning `
    --timeout-ms 5000 --max-messages $MaxMessages `
    --property print.key=true --property key.separator=" | " 2>$null | Out-String
$codigo = $LASTEXITCODE

Write-Output $salida

if ($codigo -ne 0) {
    # Con mensajes impresos, el único error esperable es el_TIMEOUT del consumer.
    $terminoPorInactividad = $salida -match "\|"
    if ($terminoPorInactividad) {
        Write-Host "(consulta finalizada por --timeout-ms después de entregar mensajes)"
        exit 0
    }
    exit $codigo
}
