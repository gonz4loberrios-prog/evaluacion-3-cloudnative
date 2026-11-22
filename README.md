# Streaming resiliente de ventas — DSY1107 EP5

Implementación completa del encargo: un productor publica ventas en `ventas-raw`, una aplicación **Kafka Streams** valida, deduplica, agrega por tienda en ventanas temporales y publica el resultado en `ventas-agregadas`. Los eventos rechazados (JSON inválido, campos incorrectos, duplicados de negocio y tardíos fuera de gracia) se envían a `ventas-raw-dlq` **sin detener el procesamiento**.

Incluye además: 2 brokers con replicación, scripts para crear/inspeccionar tópicos y grupos, prueba de caída de broker, generador reproducible de casos, pruebas automáticas con `TopologyTestDriver`, métricas Prometheus/Grafana y documentación de decisiones y evidencia.

---

## 1. Arquitectura

```
 productor ──▶ ventas-raw ──▶ Kafka Streams ──▶ ventas-agregadas
                   │              │
                   │              └─▶ ventas-raw-dlq
                   │
              (eventId como clave)
                   │
        validación → dedup → repartición por tienda → ventanas → suppress → salida
```

Detalle completo y diagrama Mermaid: [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md).

### Tópicos

| Tópico | Particiones | RF | `min.insync.replicas` | Retención | Clave |
|---|---:|---:|---:|---|---|
| `ventas-raw` | 4 | 2 | 1 | 7 días | `eventId` |
| `ventas-agregadas` | 4 | 2 | 1 | 7 días | `tiendaId\|windowStart` |
| `ventas-raw-dlq` | 4 | 2 | 1 | 7 días | clave original del evento |

> No se usa RF=3 porque el laboratorio tiene **2 brokers**. Se elige `min.insync.replicas=1` para que la producción con `acks=all` siga funcionando durante la caída de un broker. El tradeoff está explicado en [`docs/DECISIONES.md`](docs/DECISIONES.md).

Como los brokers tienen `auto.create.topics.enable=false`, `scripts/topics.ps1` (o `.sh`) aprovisiona los **tópicos internos** explícitos de Kafka Streams: dos de repartición (`por-tienda-repartition` y `ventanas-store-repartition`) y tres changelogs (`dedup-store`, `eventtime-store` y `ventanas-store`), todos con RF=2. El nombre `por-tienda` se fija explícitamente en la topología para que no dependa de un contador interno de nodos. `suppress(untilWindowCloses(...))` añade además su propio changelog (`KTABLE-SUPPRESS-STATE-STORE-…-changelog`), que Kafka Streams crea vía Admin API con el nombre que corresponda a la topología. Si cambia el `APPLICATION_ID`, se ajusta la variable del mismo nombre en los scripts.

### Salida agregada (ejemplo)

```json
{
  "tiendaId": "tienda-1",
  "windowStart": 1767261600000,
  "windowEnd": 1767261660000,
  "ventas": 3,
  "unidades": 6,
  "montoTotal": 4630.5,
  "ticketPromedio": 1543.5,
  "actualizadoEn": "2026-11-22T18:36:11.517102327Z"
}
```

### DLQ (ejemplo)

```json
{
  "categoria": "INVALIDO",
  "motivo": "TOTAL_NO_COINCIDE",
  "topicoFuente": "ventas-raw",
  "particion": 1,
  "offset": 3,
  "claveOriginal": "bad-total-1",
  "valorOriginal": "{\"eventId\":\"bad-total-1\",\"tiendaId\":\"tienda-3\",\"eventTime\":1767261613000,\"cantidad\":2,\"precio\":100.0,\"total\":999.99}",
  "eventId": "",
  "registradoEn": "2026-11-22T18:36:07.377193006Z"
}
```

Categorías de la DLQ: `INVALIDO`, `DUPLICADO`, `TARDIO`. Ningún tardío se clasifica como JSON inválido.

---

## 2. Prerrequisitos

- **Docker Desktop** con Docker Compose v2 (probado con Docker Engine 29.9.3).
- **Java 17 o superior** (para ejecutar pruebas/generador fuera de Docker). Se usó JDK 21; `JAVA_HOME` debe apuntar a una instalación JDK (no solo al `javapath` de Oracle).
- Maven no es necesario: se incluye **Maven Wrapper** (`mvnw` / `mvnw.cmd`).
- Puertos libres: `2181`, `9092`, `9093`, `18081`, `9090`, `9308`, `3000`.

Comprobar Docker:

```powershell
docker info --format "{{.ServerVersion}}"
```

---

## 3. Puesta en marcha (comandos exactos)

Todo se ejecuta desde la raíz del repositorio.

### 3.1 Levantar el stack

```powershell
docker compose up -d --build
docker compose ps
```

Servicios levantados: `zookeeper`, `kafka-1`, `kafka-2`, `streams-app`, `kafka-exporter`, `prometheus`, `grafana`.

### 3.2 Crear tópicos

```powershell
.\scripts\topics.ps1
# o para recrearlos desde cero:
.\scripts\topics.ps1 -Reset
```

En Linux/macOS:

```bash
bash scripts/topics.sh
```

### 3.3 Verificar que la app está procesando

```powershell
.\scripts\inspect.ps1
```

Muestra ISR de los tópicos, grupos de consumidores, lag del grupo `ventas-streams-dsy1107` y el endpoint `/metrics`.

También puedes abrir:

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (usuario `admin`, contraseña `admin` — solo demo, ver `.env.example`)
- Métricas de la app: <http://localhost:18081/metrics> (el puerto interno del contenedor sigue siendo 8081; el host 8081 está ocupado por otro proyecto)

### 3.4 Generar eventos de prueba

```powershell
# Todos los escenarios de una vez
.\scripts\generate.ps1 -Scenario all

# Un escenario específico
.\scripts\generate.ps1 -Scenario invalid-json
.\scripts\generate.ps1 -Scenario duplicate
.\scripts\generate.ps1 -Scenario late-beyond-grace

# Forzar cierre de ventanas y ver resultados
.\scripts\generate.ps1 -Scenario flush
.\scripts\consume.ps1 -Topic ventas-agregadas
.\scripts\consume.ps1 -Topic ventas-raw-dlq
```

Escenarios disponibles: `all`, `valid`, `invalid-json`, `missing-field`, `total-mismatch`, `invalid-cantidad`, `duplicate`, `late-within-grace`, `late-beyond-grace`, `flush`.

`consume.ps1` usa `--timeout-ms 5000` para dejar de escuchar cuando no hay mensajes nuevos. Si ya imprimió registros, el timeout de inactividad se considera una finalización normal y el script termina con código 0.

### 3.5 Probar caída de un broker

```powershell
.\scripts\failover.ps1
```

El script captura ISR **antes**, **durante** (con `kafka-2` detenido) y **después** (tras reiniciarlo), produce eventos con `acks=all` durante la falla, muestra el lag y finalmente reinicia `streams-app` para demostrar la restauración desde changelogs. Detalles y resultados reales: [`docs/EVIDENCIA.md`](docs/EVIDENCIA.md).

### 3.6 Pruebas automáticas

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"   # ajustar a tu JDK
.\mvnw.cmd -B test
```

Resultado verificado: `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0` — `BUILD SUCCESS`.

Para preparar una demo o presentación con capturas reales, sigue [`docs/GUIA_PRESENTACION.md`](docs/GUIA_PRESENTACION.md).

---

## 4. Variables de configuración

Todas tienen valor por defecto y pueden sobreescribirse en `.env` (copiar `.env.example`). **No subir un `.env` con secretos reales.**

| Variable | Default | Significado |
|---|---|---|
| `KAFKA_BOOTSTRAP_INTERNAL` | `kafka-1:29092,kafka-2:29092` | Brokers usados por la app dentro de Docker |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Brokers cuando se ejecuta la app en el host |
| `APPLICATION_ID` | `ventas-streams-dsy1107` | ID del grupo Kafka Streams |
| `TOPIC_RAW` / `TOPIC_AGG` / `TOPIC_DLQ` | `ventas-raw` / `ventas-agregadas` / `ventas-raw-dlq` | Nombres de tópicos |
| `WINDOW_MS` | `60000` | Duración de la ventana tumbling |
| `GRACE_MS` | `30000` | Gracia para eventos tardíos |
| `DEDUP_RETENTION_MS` | `86400000` | Retención lógica de `eventId` (24 h) |
| `NUM_STREAM_THREADS` | `2` | Hilos de procesamiento (uno por partición como máximo) |
| `STATE_DIR` | `/tmp/kafka-streams` | Directorio local de estado |
| `METRICS_PORT` | `8081` | Puerto interno del endpoint `/metrics` |
| `METRICS_HOST_PORT` | `18081` | Puerto expuesto en el host (8081 está ocupado por `despachos-api`) |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | `admin` / `admin` | Credenciales demo de Grafana |

---

## 5. Resultados esperados por escenario

Matriz completa: [`docs/MATRIZ_ESCENARIOS.md`](docs/MATRIZ_ESCENARIOS.md).

| Escenario | Entrada | Resultado |
|---|---|---|
| Válido | JSON completo, `total = cantidad × precio` | Se agrega a la ventana de su `tiendaId` |
| JSON malformado | `{...` truncado | DLQ `INVALIDO / JSON_MALFORMADO`; el flujo sigue |
| Campo ausente | sin `total` | DLQ `INVALIDO / TOTAL_AUSENTE` |
| Total inconsistente | `2 × 100` pero `total=999.99` | DLQ `INVALIDO / TOTAL_NO_COINCIDE` |
| `eventId` repetido | mismo evento dos veces | 1 sola venta en el agregado; DLQ `DUPLICADO / EVENTID_DUPLICADO` |
| Tardío dentro de gracia | `eventTime=+5s` tras reloj `+50s` | Se agrega normalmente |
| Tardío fuera de gracia | `eventTime=+5s` tras reloj `+240s` | DLQ `TARDIO / LATE_BEYOND_GRACE`, **no** se agrega, **no** es `INVALIDO` |

---

## 6. Límites y recuperación

- **Deduplicación**: se basa en `eventId` y una ventana de retención lógica de 24 h. Un evento repetido **después** de ese plazo se cuenta como nuevo. Es una decisión deliberada para acotar el state store; ver `docs/DECISIONES.md`.
- **Detección de tardíos**: se compara contra el mayor `eventTime` visto **por tienda** (state store `eventtime-store`). Es una heurística determinista y barata; no sustituye la semántica interna de cierre de ventanas de Kafka Streams, que además aplica su propia gracia.
- **Descartes internos**: además de la DLQ explícita, Kafka Streams puede descartar registros por ventanas ya cerradas. Se detectan con:
  - contadores propios en `/metrics` (`ventas_tardias_total`, `ventas_duplicadas_total`, …);
  - logs `WARN` de `WindowStore`/`suppress` con `SKIP_FOR_TIME`;
  - métricas JMX exportadas como `kafka_streams_*` en Prometheus.
- **Caída de broker**: con RF=2 y `min.insync.replicas=1` se mantiene disponibilidad. Al reiniciar, las réplicas se sincronizan (ISR vuelve a `[1,2]`) y los stores se restauran desde sus changelog topics.
- **`exactly_once_v2`**: garantiza que un evento se procese una sola vez **en el motor** (transacciones + idempotencia). No evita que el **productor** envíe dos veces el mismo `eventId`: eso es duplicado de negocio y se resuelve con `dedup-store`. Son conceptos distintos.
- **Reinicio limpio**: para repetir el laboratorio desde cero:

```powershell
docker compose down -v
docker compose up -d --build
.\scripts\topics.ps1
```

---

## 7. Estructura del repositorio

```
.
├── docker-compose.yml          # ZooKeeper + 2 brokers + app + exporter + Prometheus + Grafana
├── Dockerfile                  # build multi-stage con Maven 17
├── pom.xml                     # Kafka Streams 3.7.1, Jackson, JUnit 5
├── mvnw / mvnw.cmd              # Maven Wrapper 3.9.9
├── .env.example                # variables demo (sin secretos)
├── src/main/java/cl/dsy1107/ventas/
│   ├── VentasStreamsApp.java        # configuración y arranque
│   ├── config/AppConfig.java
│   ├── model/                       # Venta, EventoProcesado, Agregado, VentaAgregada, RegistroDlq, EstadoEvento
│   ├── serde/                       # JsonSerde, VentaTimestampExtractor
│   ├── validation/Validador.java
│   ├── topology/                    # Validacion, Dedup, Latencia, VentasTopology
│   ├── metrics/                     # MetricsRegistry, MetricsServer
│   └── generator/EventGenerator.java
├── src/test/java/.../VentasTopologyTest.java
├── scripts/                     # topics, inspect, generate, consume, failover (.ps1 y .sh)
├── monitoring/                  # Prometheus y Grafana
└── docs/                        # ARQUITECTURA, DECISIONES, MATRIZ_ESCENARIOS, EVIDENCIA, GUIA_PRESENTACION
```
