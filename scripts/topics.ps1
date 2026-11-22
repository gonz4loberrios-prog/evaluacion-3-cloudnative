param([switch]$Reset)

$ErrorActionPreference = "Stop"
$bootstrap = "localhost:29092"
$app = if ($env:APPLICATION_ID) { $env:APPLICATION_ID } else { "ventas-streams-dsy1107" }
$temas = @(
    @{ Nombre = "ventas-raw";      Particiones = 4; Config = @("min.insync.replicas=1", "retention.ms=604800000") },
    @{ Nombre = "ventas-agregadas";Particiones = 4; Config = @("min.insync.replicas=1", "retention.ms=604800000") },
    @{ Nombre = "ventas-raw-dlq";  Particiones = 4; Config = @("min.insync.replicas=1", "retention.ms=604800000") },
    # Kafka Streams no puede crearlos con auto.create.topics.enable=false: se aprovisionan aquí.
    @{ Nombre = "$app-por-tienda-repartition";                    Particiones = 4; Config = @("min.insync.replicas=1", "retention.ms=604800000") },
    @{ Nombre = "$app-ventanas-store-repartition";                 Particiones = 4; Config = @("min.insync.replicas=1", "retention.ms=604800000") },
    @{ Nombre = "$app-dedup-store-changelog";                      Particiones = 4; Config = @("min.insync.replicas=1", "cleanup.policy=compact") },
    @{ Nombre = "$app-eventtime-store-changelog";                  Particiones = 4; Config = @("min.insync.replicas=1", "cleanup.policy=compact") },
    @{ Nombre = "$app-ventanas-store-changelog";                   Particiones = 4; Config = @("min.insync.replicas=1", "cleanup.policy=compact") }
)

if ($Reset) {
    foreach ($tema in $temas) {
        docker compose exec -T kafka-1 kafka-topics --bootstrap-server $bootstrap --delete --if-exists --topic $tema.Nombre
    }
    Start-Sleep -Seconds 3
}

foreach ($tema in $temas) {
    $args = @("compose", "exec", "-T", "kafka-1", "kafka-topics", "--bootstrap-server", $bootstrap,
              "--create", "--if-not-exists", "--topic", $tema.Nombre,
              "--partitions", $tema.Particiones, "--replication-factor", 2)
    foreach ($cfg in $tema.Config) { $args += @("--config", $cfg) }
    & docker @args
}

Write-Host "`n--- Descripción de tópicos ---"
docker compose exec -T kafka-1 kafka-topics --bootstrap-server $bootstrap --describe
