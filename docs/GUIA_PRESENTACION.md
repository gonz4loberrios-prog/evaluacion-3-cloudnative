# Guía de capturas para la presentación

Esta guía te lleva paso a paso para sacar pantallazos reales del laboratorio. El stack está **actualmente levantado** (7/7 contenedores `Up`) con datos generados con base `2026-01-12T10:00:00Z` (`1768212000000`). Las métricas vigentes son `26 / 3 / 1 / 1 / 19` (válidos / inválidos / tardíos / duplicados / agregados). El hash del commit se toma directamente del paso 1 (cambia cada vez que se actualiza la documentación).

**Antes de empezar:** abre PowerShell en `C:\Users\gonza\OneDrive\Desktop\wwww` y maximiza la ventana. Usa el mismo orden de pasos; así las capturas cuentan una historia continua.

> Si PowerShell bloquea los scripts, ejecuta:
> `powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\<script>.ps1"`.

---

## Resumen de capturas

| # | Captura | Comando / lugar | Qué resaltar |
|---:|---|---|---|
| 0 | Stack activo | `docker compose ps` | 7 contenedores `Up` |
| 1 | Repo y commit | `git status` + `git log --oneline -1` | `main`, `HEAD -> main`, árbol limpio |
| 2 | Arquitectura | `README.md` o `docs/ARQUITECTURA.md` | Flujo productor → Streams → salidas + DLQ |
| 3 | Tópicos e ISR | `docker compose exec -T kafka-1 kafka-topics ... --describe` | 4 particiones, RF=2, `Isr: 1,2` |
| 4 | Generación | `.\scripts\generate.ps1 -Scenario all -BaseTime 2026-01-12T10:00:00Z` | Entrada real con errores y duplicado |
| 5 | Métricas | `http://localhost:18081/metrics` | `26 / 3 / 1 / 1 / 19` |
| 6 | Agregados | `.\scripts\consume.ps1 -Topic ventas-agregadas` | `tienda-1` con `4330.5` y `2165.25` |
| 7 | DLQ | `.\scripts\consume.ps1 -Topic ventas-raw-dlq` | 5 categorías de rechazo |
| 8 | Lag | `kafka-consumer-groups ... --describe --group ...` | `LAG = 0` en todas las particiones |
| 9 | Failover | `docs/EVIDENCIA.md` §5 o ejecutar `failover.ps1` | ISR `1,2 → 1 → 1,2` |
| 10 | Restauración | `docker compose logs ... streams-app` | `Restored and ready to run`, `Restoration took` |
| 11 | Grafana | `http://localhost:3000` | Panel **Streaming resiliente de ventas - DSY1107** |

**Nombres sugeridos para los archivos:** `00-stack.png`, `01-repo.png`, `02-arquitectura.png`, `03-topics-isr.png`, `04-generacion.png`, `05-metricas.png`, `06-agregados.png`, `07-dlq.png`, `08-lag.png`, `09-failover.png`, `10-restauracion.png`, `11-grafana.png`. Guárdalos en `docs/capturas/`.

---

## Paso 0 — Stack levantado

Ejecuta:

```powershell
docker compose ps
```

Captura cuando veas los 7 servicios en `Up`: `dsy-zookeeper`, `dsy-kafka-1`, `dsy-kafka-2`, `dsy-streams-app`, `dsy-kafka-exporter`, `dsy-prometheus` y `dsy-grafana`.

**Qué decir:** “El laboratorio completo está operativo: dos brokers con ZooKeeper, la aplicación Streams, el exportador y el stack de monitoreo.”

---

## Paso 1 — Repo listo

Ejecuta:

```powershell
git status
git log --oneline --decorate -1
```

Captura cuando veas `HEAD -> main` y que `git status` no muestra cambios pendientes. El mensaje del último commit debe ser `Actualizar guía de capturas al estado actual del stack` (o el que corresponda si se hicieron más ajustes después).

```text
<hash> (HEAD -> main) Actualizar guía de capturas al estado actual del stack
```

**Qué decir:** “El entregable está versionado en la rama `main`, con el árbol de trabajo limpio y la evidencia documentada en el último commit.”

---

## Paso 2 — Arquitectura

Abre `README.md` en el editor y captura el bloque:

```text
productor ──▶ ventas-raw ──▶ Kafka Streams ──▶ ventas-agregadas
                  │              │
                  │              └─▶ ventas-raw-dlq
```

Si quieres más detalle, abre `docs/ARQUITECTURA.md` y captura el diagrama Mermaid.

**Qué decir:** “Un productor publica en `ventas-raw`; la topología valida, deduplica, repartiza por tienda, agrega en ventanas de 1 minuto con gracia de 30 s y `suppress`, y publica el resultado. Los rechazos van a DLQ sin detener el flujo.”

---

## Paso 3 — Tópicos e ISR

Ejecuta:

```powershell
docker compose exec -T kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe --topic ventas-raw
```

Captura las 4 líneas de partición. Deberías ver:

```text
Topic: ventas-raw	TopicId: DEb3fCUlRJCSURK26Mwy6g	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-raw	Partition: 0	Leader: 2	Replicas: 2,1	Isr: 1,2
	Topic: ventas-raw	Partition: 1	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-raw	Partition: 2	Leader: 2	Replicas: 2,1	Isr: 1,2
	Topic: ventas-raw	Partition: 3	Leader: 1	Replicas: 1,2	Isr: 1,2
```

**Qué decir:** “Cuatro particiones, factor de replicación 2 y ambos brokers en ISR. `min.insync.replicas=1` permite seguir produciendo con `acks=all` cuando cae un broker.”

---

## Paso 4 — Generar eventos (si quieres repetirlo)

El estado actual ya tiene los datos. Si quieres ver la generación en vivo, ejecuta:

```powershell
.\scripts\generate.ps1 -Scenario all -BaseTime 2026-01-12T10:00:00Z
```

> **Importante:** si repites la generación, usa una base de tiempo **más de 24 h posterior** a la última usada (por ejemplo, `2026-01-14T10:00:00Z`). La dedup usa `<= 24 h`, así que una base exactamente 24 h después se cuenta como duplicado. Otra opción es reiniciar `streams-app` para limpiar los contadores.

Captura las primeras líneas:

```text
[all] key=val-1 value={"eventId":"val-1",...,"eventTime":1768212001000,...,"total":3980.0,...}
[all] key=bad-json-1 value={"eventId": "roto", "tiendaId":
[all] key=dup-evento-1 value={"eventId":"dup-evento-1",...}
```

**Qué decir:** “El generador es reproducible y mezcla casos válidos, JSON roto, campos ausentes, total inconsistente, duplicados y tardíos.”

---

## Paso 5 — Métricas

Abre en el navegador:

```text
http://localhost:18081/metrics
```

Busca `Ctrl+F` por `ventas_` y captura:

```text
# TYPE ventas_validas_total counter
ventas_validas_total 26
# TYPE ventas_invalidas_total counter
ventas_invalidas_total 3
# TYPE ventas_tardias_total counter
ventas_tardias_total 1
# TYPE ventas_duplicadas_total counter
ventas_duplicadas_total 1
# TYPE ventas_agregadas_total counter
ventas_agregadas_total 19
```

Alternativa en PowerShell si prefieres capturar la terminal:

```powershell
$txt = (Invoke-WebRequest -UseBasicParsing "http://localhost:18081/metrics").Content -split "`n"
$txt | Where-Object { $_ -match "^ventas_" }
```

**Qué decir:** “Los contadores de negocio confirman 26 válidos, 3 inválidos, 1 tardío, 1 duplicado y 19 agregados emitidos. Los contadores se reinician al reiniciar `streams-app`; el estado de negocio se recupera de los changelogs.”

---

## Paso 6 — Salida agregada

Ejecuta:

```powershell
.\scripts\consume.ps1 -Topic ventas-agregadas -MaxMessages 200
```

Captura especialmente esta línea:

```text
tienda-1|1768212000000 | {"tiendaId":"tienda-1","windowStart":1768212000000,"windowEnd":1768212060000,"ventas":2,"unidades":3,"montoTotal":4330.5,"ticketPromedio":2165.25,"actualizadoEn":"..."}
```

**Qué decir:** “Las ventas válidas de `tienda-1` se agrupan en la ventana `[1768212000000, 1768212060000)` y la salida aparece recién al cerrarse gracias a `suppress`: en esta ejecución quedaron 2 ventas, 3 unidades, `4330.5` y ticket promedio `2165.25`. El ejemplo clásico de 3 ventas / `4630.5` / `1543.5` corresponde a la ejecución con base `2026-01-10` y está en `docs/EVIDENCIA.md`.”

---

## Paso 7 — DLQ

Ejecuta:

```powershell
.\scripts\consume.ps1 -Topic ventas-raw-dlq -MaxMessages 200
```

Captura las líneas con estos motivos:

```text
bad-json-1      → JSON_MALFORMADO        (INVALIDO)      partición 1, offset 55
missing-total-1 → TOTAL_AUSENTE          (INVALIDO)      partición 1, offset 56, eventTime 1768212012000
bad-total-1     → TOTAL_NO_COINCIDE      (INVALIDO)      partición 1, offset 57, eventTime 1768212013000
dup-evento-1    → EVENTID_DUPLICADO      (DUPLICADO)     partición 2, offset 60, eventTime 1768212020000
late-bad-2      → LATE_BEYOND_GRACE      (TARDIO)        partición 3, offset 32, eventTime 1768212005000
```

> La DLQ acumula ejecuciones anteriores. Para la captura, busca las líneas cuyo `eventTime` empiece con `1768212` (base `2026-01-12`) o usa `Ctrl+F` por `late-bad-2` y `dup-evento-1`.

**Qué decir:** “La DLQ conserva tópico, partición, offset, clave y valor original. El tardío se clasifica `TARDIO`, nunca `INVALIDO`: su JSON es válido, lo que pasó es que su ventana ya cerró.”

---

## Paso 8 — Lag

Ejecuta:

```powershell
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group ventas-streams-dsy1107
```

Captura las filas de `ventas-raw`; todas deben terminar en `LAG 0`:

```text
GROUP                  TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
ventas-streams-dsy1107 ventas-raw  0          44              44              0
ventas-streams-dsy1107 ventas-raw  1          63              63              0
ventas-streams-dsy1107 ventas-raw  2          67              67              0
ventas-streams-dsy1107 ventas-raw  3          36              36              0
```

**Qué decir:** “El grupo consume al mismo ritmo en que se publica; no hay retraso acumulado.”

---

## Paso 9 — Failover

Tienes dos opciones:

### Opción A (recomendada para la presentación): capturar la evidencia ya guardada

Abre `docs/EVIDENCIA.md`, sección **5. Caída de broker, ISR y lag**, y captura los tres bloques ISR:

```text
===== ISR ANTES =====      Isr: 2,1 / 1,2 / 2,1 / 1,2
===== ISR DURANTE =====     Isr: 1   (kafka-2 detenido)
===== ISR DESPUES =====     Isr: 1,2 (kafka-2 reiniciado)
```

Ventaja: no altera el estado actual.

### Opción B (demo en vivo)

Ejecuta:

```powershell
.\scripts\failover.ps1
```

Tarda aproximadamente 2 minutos. Captura:

- `acks = -1`, `enable.idempotence = true`, `Instantiated an idempotent producer`.
- ISR `1,2 → 1 → 1,2`.
- Lag 0 durante y después.
- Los mensajes de restauración (paso 10).

> Si haces la opción B, los contadores vuelven a 0 porque el script reinicia `streams-app`. Eso está explicado en la evidencia; vuelve a generar con una base nueva (por ejemplo `2026-01-14T10:00:00Z`) si quieres repetir el paso 5.

---

## Paso 10 — Restauración desde changelogs

Ejecuta:

```powershell
docker compose logs --since 10m streams-app 2>&1 |
    Select-String -Pattern "Finished restoring changelog|Restored and ready to run|Restoration took|Estado Kafka Streams" |
    Select-Object -Last 25
```

Captura líneas como:

```text
Finished restoring changelog ventas-streams-dsy1107-ventanas-store-changelog-2 to store ventanas-store with a total number of 45 records
Finished restoring changelog ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-0000000031-changelog-2 ... with a total number of 78 records
task [2_3] Restored and ready to run
Restoration took 599 ms for all active tasks
Estado Kafka Streams: REBALANCING -> RUNNING
```

**Qué decir:** “Al reiniciar, Kafka Streams reconstruye `dedup-store`, `eventtime-store` y el store de `suppress` desde sus changelog topics; por eso no se pierde el estado de negocio.”

---

## Paso 11 — Grafana

Abre:

```text
http://localhost:3000
```

Credenciales demo: las definidas como defaults en `docker-compose.yml` (no están en `.env.example`).

Abre el dashboard **Streaming resiliente de ventas - DSY1107** (uid `ventas-dsy1107`) y captura los paneles de:

- `kafka_topic_partition_in_sync_replica` (2 en estado sano).
- `kafka_consumergroup_lag` (0).
- Contadores `ventas_*_total`.

**Qué decir:** “Además del endpoint crudo, Prometheus y Grafana exponen la misma señal para monitoreo continuo.”

---

## Anexo — Misma secuencia desde CMD (`cmd.exe`)

Los scripts del repositorio son `.ps1`, pero se incluyó un wrapper `.bat` por cada uno en `scripts\` para poder invocarlos desde CMD sin escribir `powershell -File`. Si escribies `.\scripts\consume.ps1` directamente en CMD, Windows **mostraría el código del archivo** en vez de ejecutarlo; usa siempre `scripts\consume.bat` (o el comando `powershell -File` completo). El resto son comandos nativos de CMD (`findstr` en lugar de `Select-String`, `curl.exe` en lugar de `Invoke-WebRequest` y `start` para abrir el navegador).

```cmd
cd /d C:\Users\gonza\OneDrive\Desktop\wwww

docker compose ps

git status
git log --oneline --decorate -1

docker compose exec -T kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe --topic ventas-raw

scripts\consume.bat -Topic ventas-agregadas -MaxMessages 200 | findstr "1768212"
scripts\consume.bat -Topic ventas-raw-dlq -MaxMessages 200 | findstr /C:"1768212" /C:"JSON_MALFORMADO" /C:"TOTAL_AUSENTE" /C:"TOTAL_NO_COINCIDE" /C:"EVENTID_DUPLICADO" /C:"LATE_BEYOND_GRACE"

curl.exe -s http://localhost:18081/metrics | findstr /B "ventas_"

docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group ventas-streams-dsy1107 | findstr "ventas-raw"

start "" http://localhost:9090/targets
start "" http://localhost:3000
```

Solo si necesitas regenerar datos (usa una base nueva de +2 días):

```cmd
scripts\generate.bat -Scenario all -BaseTime 2026-01-14T10:00:00Z
```

Failover en vivo y logs de restauración:

```cmd
scripts\failover.bat

docker compose logs --since 30m streams-app 2>&1 | findstr /C:"Restored and ready to run" /C:"Restoration took" /C:"Estado Kafka Streams"
```

Para abrir la evidencia del failover sin alterar el estado:

```cmd
notepad docs\EVIDENCIA.md
```

> `curl.exe` viene incluido en Windows 10/11. Si no está, abre `http://localhost:18081/metrics` directamente en el navegador y captura desde ahí.
> Si `findstr` oculta el `bad-json-1` (no lleva `eventTime`), ejecuta el consumo DLQ sin filtro y usa `Ctrl+F` en la ventana de CMD.

---

## Guion sugerido (7 diapositivas)

1. **Problema y arquitectura** — pasos 1 y 2.
2. **Contrato de datos y configuración del cluster** — paso 3.
3. **Validación y DLQ** — pasos 4 y 7.
4. **Agregación por ventana y dedup** — pasos 5 y 6.
5. **Resiliencia ante caída de broker** — pasos 8 y 9.
6. **Recuperación de estado** — paso 10.
7. **Observabilidad y cierre** — paso 11 + commit `40d0b90`.

---

## Si algo salió mal

| Síntoma | Acción |
|---|---|
| `consume.ps1` termina con timeout pero ya imprimió mensajes | Es normal: el timeout por inactividad se trata como fin correcto y sale con código 0 |
| Métricas en 0 | Reinició `streams-app`; vuelve a generar con una base de tiempo nueva (`+2 días`) |
| Todo aparece como duplicado | La base elegida está a ≤ 24 h de una anterior; usa una base ≥ 2 días posterior o reinicia la app |
| `UNKNOWN_TOPIC_OR_PARTITION` | Ejecuta `.\scripts\topics.ps1` y espera a que `streams-app` muestre `RUNNING` |
| Script bloqueado por PowerShell | `powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\...ps1"` |
| Gráficos vacíos o target `ventas-streams` caído | Abre `http://localhost:9090/targets`; los 3 targets deben estar `UP`. Las etiquetas JMX se sanitizan en `MetricsServer` para que Prometheus las acepte |
