@echo off
chcp 65001 >nul
cd /d "%~dp0.."

echo.
echo ===== 1. STACK ACTIVO =====
docker compose ps

echo.
echo ===== 2. REPO =====
git status
git log --oneline --decorate -1

echo.
echo ===== 3. TOPICOS E ISR =====
docker compose exec -T kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe --topic ventas-raw

echo.
echo ===== 4. METRICAS =====
curl.exe -s http://localhost:18081/metrics | findstr /B "ventas_"

echo.
echo ===== 5. SALIDA AGREGADA =====
call scripts\consume.bat -Topic ventas-agregadas -MaxMessages 200 | findstr "1768212"

echo.
echo ===== 6. DLQ =====
call scripts\consume.bat -Topic ventas-raw-dlq -MaxMessages 200 | findstr /C:"2026-11-23T16:57"

echo.
echo ===== 7. LAG =====
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group ventas-streams-dsy1107 2>&1 | findstr "ventas-raw "

echo.
echo ===== 8. RESTAURACION DESDE CHANGELOGS =====
docker compose logs --since 24h streams-app 2>&1 | findstr /C:"Finished restoring changelog" /C:"Restored and ready to run" /C:"Restoration took" /C:"Estado Kafka Streams"

echo.
echo ===== VERIFICACION TERMINADA =====
