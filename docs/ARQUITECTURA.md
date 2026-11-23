# Arquitectura del streaming resiliente de ventas

## 1. Vista general

```mermaid
flowchart LR
    P[Productor<br/>EventGenerator] -->|eventId como clave| R[(ventas-raw<br/>4 part. / RF=2)]
    R --> S{Kafka Streams<br/>ventas-streams-dsy1107}

    subgraph TOP [Topología]
      direction TB
      V[1. Validación<br/>JSON + campos + reglas] --> D{¿válido?}
      D -- no --> DLQ
      D -- sí --> DD[2. Dedup por eventId<br/>dedup-store 24 h]
      DD --> DDQ{¿duplicado?}
      DDQ -- sí --> DLQ
      DDQ -- no --> RK[3. selectKey tiendaId<br/>+ repartición 4 part.]
      RK --> L[4. Control de latencia<br/>eventtime-store]
      L --> LQ{¿tardío fuera<br/>de gracia?}
      LQ -- sí --> DLQ
      LQ -- no --> W[5. Tumbling 60 s + gracia 30 s<br/>ventanas-store]
      W --> SUP[6. suppress untilWindowCloses]
      SUP --> M[7. map a tiendaId\|windowStart]
    end

    S --> TOP
    M --> A[(ventas-agregadas<br/>4 part. / RF=2)]
    DLQ[(ventas-raw-dlq<br/>4 part. / RF=2<br/>clave = clave original)]

    TOP --> MET[/metrics<br/>contadores + JMX/]
    MET --> EX[kafka-exporter / Prometheus / Grafana]

    Z[ZooKeeper] --- K1[(kafka-1)]
    Z --- K2[(kafka-2)]
    K1 --- K2
```

### Tópicos, claves y replicación

| Tópico | Clave | Particiones | RF | Creado por |
|---|---|---:|---:|---|
| `ventas-raw` | `eventId` | 4 | 2 | `scripts\topics.bat` |
| `ventas-agregadas` | `tiendaId\|windowStart` | 4 | 2 | `scripts\topics.bat` |
| `ventas-raw-dlq` | clave original del evento | 4 | 2 | `scripts\topics.bat` |
| `ventas-streams-dsy1107-por-tienda-repartition` | `tiendaId` | 4 | 2 | `scripts\topics.bat` |
| `ventas-streams-dsy1107-ventanas-store-repartition` | `tiendaId` | 4 | 2 | `scripts\topics.bat` |
| `ventas-streams-dsy1107-dedup-store-changelog` | `eventId` | 4 | 2 | `scripts\topics.bat` |
| `ventas-streams-dsy1107-eventtime-store-changelog` | `tiendaId` | 4 | 2 | `scripts\topics.bat` |
| `ventas-streams-dsy1107-ventanas-store-changelog` | `Windowed<tiendaId>` | 4 | 2 | `scripts\topics.bat` |
| `ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-…‑changelog` | ventana | 4 | 2 | Kafka Streams (Admin API) |

Los tres tópicos de negocio y los cinco internos explícitos usan `min.insync.replicas=1` y `retention.ms=604800000`. `auto.create.topics.enable=false` evita tópicos accidentales con RF por defecto.

## 2. Flujo de datos

1. **Productor** publica cada venta en `ventas-raw` con clave `eventId` y un timestamp de registro igual a `eventTime`. Clave por `eventId` ⇒ todos los reintentos del mismo evento caen en la misma partición, lo que permite deduplicar sin una repartición extra.
2. **`VentaTimestampExtractor`** extrae `eventTime` del JSON. Si el JSON está roto, usa el timestamp del broker; el validador ya registrará el rechazo. Así el tiempo de evento gobierna ventanas y tardíos.
3. **Validación**: `Validador` comprueba sintaxis JSON, existencia/tipo de `eventId`, `tiendaId`, `eventTime`, `cantidad`, `precio` y `total`, y la regla de negocio `total = cantidad × precio` (tolerancia 0,01). Los rechazos salen con categoría `INVALIDO` y un motivo específico.
4. **Deduplicación**: `dedup-store` (KeyValueStore con changelog) guarda el primer `eventTime` visto por `eventId`. Si reaparece dentro de 24 h, se clasifica `DUPLICADO` y se envía a DLQ. El agregado no se engorda.
5. **Repartición por tienda**: `selectKey(tiendaId)` + `repartition(4)` agrupa las ventas de una tienda en la misma tarea, necesaria para la agregación y para el estado de latencia por tienda.
6. **Control de latencia**: `eventtime-store` guarda el máximo `eventTime` por tienda. Se calcula la ventana del evento y se compara `finVentana + gracia < maxVisto`. Si es cierto, el evento es tardío fuera de gracia → DLQ con categoría `TARDIO` y motivo `LATE_BEYOND_GRACE`. Si no, se agrega (incluye tardíos dentro de gracia).
7. **Agregación**: `TimeWindows.ofSizeAndGrace(60 s, 30 s)` + `aggregate` mantiene `{ventas, unidades, montoTotal}`. `suppress(untilWindowCloses)` evita emitir resultados parciales: solo se publica el resumen final de cada ventana.
8. **Salida**: la clave se reescribe como `tiendaId|windowStart` y el valor `VentaAgregada` incluye `windowStart`, `windowEnd`, conteo, monto y ticket promedio.

## 3. Ramas de rechazo

Las tres ramas de rechazo convergen en la misma DLQ pero conservan su categoría:

| Origen | Categoría | Motivo de ejemplo | ¿Se agrega? |
|---|---|---|---|
| Validación | `INVALIDO` | `JSON_MALFORMADO`, `TOTAL_AUSENTE`, `CANTIDAD_INVALIDA`, `TOTAL_NO_COINCIDE` | No |
| Dedup | `DUPLICADO` | `EVENTID_DUPLICADO` | No |
| Latencia | `TARDIO` | `LATE_BEYOND_GRACE` | No |

Cada registro de DLQ conserva: **valor original**, **clave original**, **tópico fuente**, **partición**, **offset**, `eventId` y timestamp de registro. Esto permite correlacionar el descarte con el mensaje exacto sin pérdida de información.

## 4. Estado y tolerancia a fallos

| Store | Tipo | Clave | Valor | Changelog | Propósito |
|---|---|---|---|---|---|
| `dedup-store` | `KeyValueStore` en memoria + logging | `eventId` | primer `eventTime` | `…-dedup-store-changelog` | deduplicación |
| `eventtime-store` | `KeyValueStore` en memoria + logging | `tiendaId` | máximo `eventTime` | `…-eventtime-store-changelog` | detección de tardíos |
| `ventanas-store` | `WindowStore` | `Windowed<tiendaId>` | `Agregado` | changelog de la KTable | agregación |
| `KTABLE-SUPPRESS-STATE-STORE-…` | store interno de `suppress` | ventana | estado de emisión | `…-KTABLE-SUPPRESS-STATE-STORE-…-changelog` | emitir cada ventana una sola vez |

Los changelog topics usan RF=2 y `acks=all`. Si una tarea se migra a otro hilo/broker, el estado se **restaura** desde el changelog antes de seguir procesando. `exactly_once_v2` añade transacciones para que la lectura del changelog, la escritura del estado y la publicación en los tópicos de salida sean atómicas.

## 5. Tiempo y ventanas

- **Tiempo de evento**, no tiempo de procesamiento: `eventTime` del payload.
- **Ventana tumbling** de 60 s, sin solapamiento: `[start, start+60s)`.
- **Gracia** de 30 s: la ventana cierra cuando el stream avanza más allá de `end + 30s`; los eventos que llegan después se consideran fuera de gracia.
- **Detección de descartes**:
  1. DLQ explícita para los tres tipos de rechazo.
  2. Contadores propios en `/metrics`: `ventas_validas_total`, `ventas_invalidas_total`, `ventas_tardias_total`, `ventas_duplicadas_total`, `ventas_agregadas_total`.
  3. Métricas JMX de Kafka Streams exportadas como `kafka_streams_*` (drop, latency, task/state, etc.).
  4. Logs `WARN`/`INFO` del logger `org.apache.kafka.streams` con mensajes `SKIP_FOR_TIME` cuando `WindowStore`/`suppress` descarta un registro por ventana cerrada.

## 6. Componentes de infraestructura

- **ZooKeeper**: coordinación de los 2 brokers (Confluent Platform 7.7.1).
- **kafka-1 / kafka-2**: listeners internos `kafka-1:29092` / `kafka-2:29092` y externos `localhost:9092` / `localhost:9093`. `auto.create.topics.enable=false`.
- **streams-app**: imagen propia Java 17; expone `/metrics` en 8081.
- **kafka-exporter**: expone métricas de brokers, tópicos, particiones, ISR y consumer lag en 9308.
- **Prometheus**: scrapea exporter, app y a sí mismo cada 5 s.
- **Grafana**: datasource Prometheus + dashboard `ventas.json` con ISR, lag y contadores de negocio.

## 7. Secuencia de un evento inválido

```mermaid
sequenceDiagram
    participant P as Productor
    participant K as ventas-raw
    participant A as Kafka Streams
    participant D as ventas-raw-dlq
    participant O as ventas-agregadas

    P->>K: clave=bad-1, valor JSON roto
    K->>A: registro
    A->>A: Validador → INVALIDO / JSON_MALFORMADO
    A->>D: RegistroDlq(valor original, clave, tópico, offset, motivo)
    Note over A: no lanza excepción;<br/>sigue con el siguiente registro
    P->>K: clave=ok-1, valor válido
    K->>A: registro
    A->>O: resumen al cerrar la ventana
```
