# Guía de capturas para la presentación (CMD)

Esta guía te lleva paso a paso para sacar pantallazos reales del laboratorio **usando solo `cmd.exe`**. El stack está actualmente levantado (7/7 contenedores `Up`) con datos generados con base `2026-01-12T10:00:00Z` (`1768212000000`). Las métricas vigentes son `26 / 3 / 1 / 1 / 19` (válidos / inválidos / tardíos / duplicados / agregados).

**Antes de empezar:** abre CMD en `C:\Users\gonza\OneDrive\Desktop\wwww`, maximízalo y ejecuta:

```cmd
cd /d C:\Users\gonza\OneDrive\Desktop\wwww
chcp 65001 >nul
```

`chcp 65001` es para que se vean bien los acentos (`idéntica`, `tardío`). Los scripts `.ps1` se llaman a través de los wrappers `.bat` de `scripts\`; **no escribas `consume.ps1` directamente en CMD**, porque Windows te mostraría el código en Notepad.

**Atajo para el día de la presentación:** `scripts\presentar.bat` corre toda la verificación (stack, repo, tópicos, métricas, agregados, DLQ, lag y restauración) en una sola ventana.

---

## Resumen de capturas

| # | Captura | Comando / lugar | Qué resaltar |
|---:|---|---|---|
| 0 | Stack activo | `docker compose ps` | 7 contenedores `Up` |
| 1 | Repo y commit | `git status` + `git log --oneline -1` | `main`, `HEAD -> main`, árbol limpio |
| 2 | Arquitectura | `notepad README.md` o `docs\ARQUITECTURA.md` | Flujo productor → Streams → salidas + DLQ |
| 3 | Tópicos e ISR | `docker compose exec -T kafka-1 kafka-topics ... --describe` | 4 particiones, RF=2, `Isr: 1,2` |
| 4 | Generación | `scripts\generate.bat -Scenario all -BaseTime 2026-01-14T10:00:00Z` | Entrada real con errores y duplicado |
| 5 | Métricas | `curl.exe -s http://localhost:18081/metrics \| findstr /B "ventas_"` | `26 / 3 / 1 / 1 / 19` |
| 6 | Agregados | `scripts\consume.bat -Topic ventas-agregadas` | `tienda-1` con `4330.5` y `2165.25` |
| 7 | DLQ | `scripts\consume.bat -Topic ventas-raw-dlq` | 5 categorías de rechazo |
| 8 | Lag | `kafka-consumer-groups ... --describe --group ...` | `LAG = 0` en todas las particiones |
| 9 | Failover | `notepad docs\EVIDENCIA.md` o `scripts\failover.bat` | ISR `1,2 → 1 → 1,2` |
| 10 | Restauración | `docker compose logs ... streams-app \| findstr` | `Restored and ready to run`, `Restoration took` |
| 11 | Grafana | `start "" http://localhost:3000` | Dashboard **Streaming resiliente de ventas - DSY1107** |

**Nombres sugeridos:** `00-stack.png`, `01-repo.png`, `02-arquitectura.png`, `03-topics-isr.png`, `04-generacion.png`, `05-metricas.png`, `06-agregados.png`, `07-dlq.png`, `08-lag.png`, `09-failover.png`, `10-restauracion.png`, `11-grafana.png`. Guárdalos en `docs\capturas\`.

---

## Paso 0 — Stack levantado

```cmd
docker compose ps
```

Captura cuando veas los 7 servicios en `Up`: `dsy-zookeeper`, `dsy-kafka-1`, `dsy-kafka-2`, `dsy-streams-app`, `dsy-kafka-exporter`, `dsy-prometheus` y `dsy-grafana`.

**Qué decir:** “El laboratorio completo está operativo: dos brokers con ZooKeeper, la aplicación Streams, el exportador y el stack de monitoreo.”

---

## Paso 1 — Repo listo

```cmd
git status
git log --oneline --decorate -1
```

Captura cuando veas `HEAD -> main` y que `git status` no muestra cambios pendientes.

**Qué decir:** “El entregable está versionado en la rama `main`, con el árbol de trabajo limpio y la evidencia documentada en el último commit.”

---

## Paso 2 — Arquitectura

```cmd
notepad README.md
```

Captura el bloque del flujo:

```text
productor ──▶ ventas-raw ──▶ Kafka Streams ──▶ ventas-agregadas
                  │              │
                  │              └─▶ ventas-raw-dlq
```

Si quieres más detalle: `notepad docs\ARQUITECTURA.md` y captura el diagrama Mermaid.

**Qué decir:** “Un productor publica en `ventas-raw`; la topología valida, deduplica, repartiza por tienda, agrega en ventanas de 1 minuto con gracia de 30 s y `suppress`, y publica el resultado. Los rechazos van a DLQ sin detener el flujo.”

---

## Paso 3 — Tópicos e ISR

```cmd
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

## Paso 4 — Generar eventos (solo si quieres repetirlo)

Los datos de la ejecución actual ya están generados. Si quieres ver la generación en vivo, usa una base nueva de al menos 2 días posterior:

```cmd
scripts\generate.bat -Scenario all -BaseTime 2026-01-14T10:00:00Z
```

Captura las primeras líneas:

```text
[all] key=val-1 value={"eventId":"val-1",...,"eventTime":1768384801000,...,"total":3980.0,...}
[all] key=bad-json-1 value={"eventId": "roto", "tiendaId":
[all] key=dup-evento-1 value={"eventId":"dup-evento-1",...}
```

> La base ya usada es `2026-01-12`; repetirla haría que todo se conte como duplicado (dedup de 24 h). Si generás datos nuevos, las capturas 5 a 8 reflejarán los nuevos contadores y fechas.

**Qué decir:** “El generador es reproducible y mezcla casos válidos, JSON roto, campos ausentes, total inconsistente, duplicados y tardíos.”

---

## Paso 5 — Métricas

Abre en el navegador:

```cmd
start "" http://localhost:18081/metrics
```

O captura la terminal:

```cmd
curl.exe -s http://localhost:18081/metrics | findstr /B "ventas_"
```

Debe salir:

```text
ventas_validas_total 26
ventas_invalidas_total 3
ventas_tardias_total 1
ventas_duplicadas_total 1
ventas_agregadas_total 19
```

**Qué decir:** “Los contadores de negocio confirman 26 válidos, 3 inválidos, 1 tardío, 1 duplicado y 19 agregados emitidos. Los contadores se reinician al reiniciar `streams-app`; el estado de negocio se recupera de los changelogs.”

---

## Paso 6 — Salida agregada

```cmd
scripts\consume.bat -Topic ventas-agregadas -MaxMessages 200 | findstr "1768212"
```

Captura especialmente esta línea:

```text
tienda-1|1768212000000 | {"tiendaId":"tienda-1","windowStart":1768212000000,"windowEnd":1768212060000,"ventas":2,"unidades":3,"montoTotal":4330.5,"ticketPromedio":2165.25,"actualizadoEn":"..."}
```

**Qué decir:** “Las ventas válidas de `tienda-1` se agrupan en la ventana `[1768212000000, 1768212060000)` y la salida aparece recién al cerrarse gracias a `suppress`: en esta ejecución quedaron 2 ventas, 3 unidades, `4330.5` y ticket promedio `2165.25`.”

---

## Paso 7 — DLQ

```cmd
scripts\consume.bat -Topic ventas-raw-dlq -MaxMessages 200 | findstr /C:"2026-11-23T16:57"
```

El bloque de abajo es un **resumen para verificar** la captura, no el texto literal (la salida real son líneas JSON largas como `late-bad-2 | {"categoria":"TARDIO",...}`):

```text
bad-json-1      → JSON_MALFORMADO        (INVALIDO)      partición 1, offset 55
missing-total-1 → TOTAL_AUSENTE          (INVALIDO)      partición 1, offset 56, eventTime 1768212012000
bad-total-1     → TOTAL_NO_COINCIDE      (INVALIDO)      partición 1, offset 57, eventTime 1768212013000
dup-evento-1    → EVENTID_DUPLICADO      (DUPLICADO)     partición 2, offset 60, eventTime 1768212020000
late-bad-2      → LATE_BEYOND_GRACE      (TARDIO)        partición 3, offset 32, eventTime 1768212005000
```

Para una sola línea de ejemplo:

```cmd
scripts\consume.bat -Topic ventas-raw-dlq -MaxMessages 200 | findstr "LATE_BEYOND_GRACE" | findstr "1768212005000"
```

**Qué decir:** “La DLQ conserva tópico, partición, offset, clave y valor original. El tardío se clasifica `TARDIO`, nunca `INVALIDO`: su JSON es válido, lo que pasó es que su ventana ya cerró.”

---

## Paso 8 — Lag

```cmd
docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group ventas-streams-dsy1107 | findstr "ventas-raw"
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

### Opción A (recomendada): capturar la evidencia ya guardada

```cmd
notepad docs\EVIDENCIA.md
```

Abre la sección **5. Caída de broker, ISR y lag** y captura los tres bloques:

```text
===== ISR ANTES =====      Isr: 2,1 / 1,2 / 2,1 / 1,2
===== ISR DURANTE =====     Isr: 1   (kafka-2 detenido)
===== ISR DESPUES =====     Isr: 1,2 (kafka-2 reiniciado)
```

Ventaja: no altera el estado actual.

### Opción B (demo en vivo)

```cmd
scripts\failover.bat
```

Tarda aproximadamente 2 minutos. Captura:

- `acks = -1`, `enable.idempotence = true`, `Instantiated an idempotent producer`.
- ISR `1,2 → 1 → 1,2`.
- Lag 0 durante y después.
- Los mensajes de restauración (paso 10).

> Si haces la opción B, los contadores vuelven a 0 porque el script reinicia `streams-app`; vuelve a generar con una base nueva si quieres repetir el paso 5.

---

## Paso 10 — Restauración desde changelogs

```cmd
docker compose logs --since 24h streams-app 2>&1 | findstr /C:"Finished restoring changelog" /C:"Restored and ready to run" /C:"Restoration took" /C:"Estado Kafka Streams"
```

Captura líneas como:

```text
Finished restoring changelog ventas-streams-dsy1107-ventanas-store-changelog-2 to store ventanas-store with a total number of 45 records
Finished restoring changelog ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-0000000031-changelog-2 ... with a total number of 78 records
task [2_3] Restored and ready to run
Restoration took 599 ms for all active tasks
Estado Kafka Streams: REBALANCING -> RUNNING
```

> Usá `--since 24h` (o quitá el `--since`) porque la restauración que se captura ocurrió hace más de 10 minutos. Si recién ejecutaste `failover.bat`, alcanza con `--since 10m`.

**Qué decir:** “Al reiniciar, Kafka Streams reconstruye `dedup-store`, `eventtime-store` y el store de `suppress` desde sus changelog topics; por eso no se pierde el estado de negocio.”

---

## Paso 11 — Grafana y Prometheus

```cmd
start "" http://localhost:9090/targets
start "" http://localhost:3000
```

Credenciales demo de Grafana: `admin` / `admin` (definidas como defaults en `docker-compose.yml`, no están en `.env.example`).

En Prometheus deben estar `UP` los 3 targets: `kafka-exporter`, `prometheus`, `ventas-streams`. En Grafana abre el dashboard **Streaming resiliente de ventas - DSY1107** (uid `ventas-dsy1107`) y captura los paneles de ISR (2), lag (0) y contadores `ventas_*_total`.

**Qué decir:** “Además del endpoint crudo, Prometheus y Grafana exponen la misma señal para monitoreo continuo.”

---

## Guion sugerido (7 diapositivas)

1. **Problema y arquitectura** — pasos 1 y 2.
2. **Contrato de datos y configuración del cluster** — paso 3.
3. **Validación y DLQ** — pasos 4 y 7.
4. **Agregación por ventana y dedup** — pasos 5 y 6.
5. **Resiliencia ante caída de broker** — pasos 8 y 9.
6. **Recuperación de estado** — paso 10.
7. **Observabilidad y cierre** — paso 11 + estado limpio de `git status` y `git log -1`.

---

## Si algo salió mal

| Síntoma | Acción en CMD |
|---|---|
| Escribes `consume.ps1` y se abre Notepad | Usa `scripts\consume.bat`. Para cambiar la asociación, ejecuta `scripts\ps1-open-with-powershell.reg` |
| `consume.bat` termina por timeout pero ya imprimió mensajes | Es normal: el timeout por inactividad se trata como fin correcto |
| Métricas en 0 | Reinició `streams-app`; vuelve a generar con una base nueva (`+2 días`) |
| Todo aparece como duplicado | La base elegida está a ≤ 24 h de una anterior; usa una base ≥ 2 días posterior |
| `UNKNOWN_TOPIC_OR_PARTITION` | Ejecuta `scripts\topics.bat` y espera a que `streams-app` muestre `RUNNING` |
| Acentos raros (`tard├¡o`) | Ejecuta `chcp 65001 >nul` en la misma ventana CMD |
| Logs de restauración vacíos | Usa `--since 24h` o quita el `--since` |
| Gráficos vacíos o target caído | Abre `http://localhost:9090/targets`; los 3 targets deben estar `UP` |
