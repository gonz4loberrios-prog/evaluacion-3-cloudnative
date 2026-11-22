param(
    [string]$Scenario = "all",
    [string]$Bootstrap = "kafka-1:29092,kafka-2:29092",
    [string]$BaseTime = "2026-01-01T10:00:00Z",
    [string]$Topic = "ventas-raw"
)

$ErrorActionPreference = "Stop"
docker compose exec -T streams-app java -cp "/app/ventas-streams.jar:/app/lib/*" `
    cl.dsy1107.ventas.generator.EventGenerator `
    --bootstrap $Bootstrap --topic $Topic --scenario $Scenario --base-time $BaseTime
