# Decisiones de diseño y justificación

## 1. Stack y versiones

| Decisión | Justificación |
|---|---|
| Java 17 + Kafka Streams 3.7.1 | LTS de Java y versión estable de Kafka Streams compatible con Confluent Platform 7.7.1. |
| Confluent Platform 7.7.1 (con ZooKeeper) | Permite levantar 2 brokers fácilmente en Docker con configuración conocida para el laboratorio. |
| Maven Wrapper 3.9.9 | El entorno no tiene `mvn` instalado, pero sí red a Maven Central. El wrapper garantiza reproducibilidad sin instalación previa. |
| Jackson para JSON | Permite validar la estructura antes de mapear al DTO, distinguiendo "campo ausente" de "campo inválido" y conservando el valor original para la DLQ. |

## 2. Tópicos, particiones y replicación

| Parámetro | Valor | Justificación |
|---|---|---|
| Particiones | 4 | Paralelismo suficiente para 2 brokers y 2 stream threads (2 particiones por hilo). Permite observar repartición y lag sin sobrecargar el laboratorio. |
| Replication factor | 2 | Hay exactamente 2 brokers. **No se usa RF=3** porque no existe un tercer broker donde colocar la réplica. |
| `min.insync.replicas` | 1 | Con `acks=all`, un productor necesita al menos 1 réplica en ISR. Si se pusiera 2, al caer un broker las escrituras fallarían con `NotEnoughReplicasException`, contradiciendo el requisito de "seguir produciendo y consumiendo durante la caída". Tradeoff: se pierde la garantía de que el dato esté en ambas réplicas antes de confirmar; al reiniciar el broker, la réplica se reconcilia. Se documenta explícitamente. |
| Retención | 7 días (`604800000` ms) | Suficiente para repetir el laboratorio durante una semana sin perder los mensajes generados, sin crecer indefinidamente el disco. |
| `auto.create.topics.enable` | `false` | Obliga a crear los tópicos con la configuración correcta mediante `scripts/topics.ps1`; evita tópicos accidentales con RF por defecto. |
| Clave de `ventas-raw` | `eventId` | Todos los eventos con el mismo ID caen en la misma partición ⇒ dedup local sin `groupBy` extra. |
| Clave de `ventas-agregadas` | `tiendaId\|windowStart` | Identifica de forma única cada ventana por tienda; facilita inspección y reprocesamiento. |
| Clave de la DLQ | clave original | Conserva correlación con el mensaje de entrada. |

## 3. Ventana y gracia

- **Duración: 60 s.** Es una ventana tumbling corta que permite ver varios cierres en una sesión de laboratorio y hace evidente el efecto de los eventos tardíos.
- **Gracia: 30 s.** Mitad de la ventana: lo suficientemente amplia para absorber saltos de ordenación típicos del generador, lo suficientemente corta para que `suppress` no mantenga demasiadas ventanas abiertas en memoria.
- **`suppress(untilWindowCloses)`** garantiza que `ventas-agregadas` reciba **un único mensaje definitivo** por ventana, no actualizaciones parciales. Es coherente con el requisito "inicio/fin de ventana, conteo, monto y ticket promedio".

## 4. Deduplicación de negocio vs `exactly_once_v2`

| Concepto | Mecanismo | Qué evita |
|---|---|---|
| `exactly_once_v2` | Transacciones Kafka + idempotencia del productor interno de Streams | Que un mensaje se procese dos veces por reintentos, fallos de tarea o migraciones |
| Dedup por `eventId` | `dedup-store` (KeyValueStore con changelog) | Que el **productor** envíe dos veces el mismo evento de negocio (reentrega, reintento de API, etc.) |

Son complementarios: `exactly_once_v2` no detecta duplicados semánticos porque, desde Kafka, dos mensajes con distinto offset son dos mensajes distintos aunque tengan el mismo `eventId`.

**Retención de 24 h (`DEDUP_RETENTION_MS=86400000`)**: acota el tamaño del state store. Un `eventId` que reaparezca después de 24 h se tratará como nuevo. Es un límite deliberado y configurable; en producción se ajustaría según la política de reentregas del negocio.

Implementación: el store guarda el primer `eventTime` por `eventId`; se considera duplicado si `ahora - primero <= retentionMs`. La comparación usa tiempo de evento (no tiempo de pared), lo que hace la prueba determinista y reproducible con `--base-time`.

## 5. Clasificación de errores y DLQ

- Cada motivo tiene un código estable: `JSON_MALFORMADO`, `EVENTID_AUSENTE`, `TIENDAID_AUSENTE`, `EVENTTIME_AUSENTE`, `EVENTTIME_INVALIDO`, `CANTIDAD_AUSENTE`, `CANTIDAD_INVALIDA`, `PRECIO_AUSENTE`, `PRECIO_INVALIDO`, `TOTAL_AUSENTE`, `TOTAL_INVALIDO`, `TOTAL_NO_COINCIDE`, `EVENTID_DUPLICADO`, `LATE_BEYOND_GRACE`.
- La categoría (`INVALIDO`, `DUPLICADO`, `TARDIO`) viaja en el value object `EventoProcesado` y se escribe en el campo `categoria` de la DLQ.
- **Un tardío nunca es un JSON inválido**: el validador corre primero y solo marca `INVALIDO`; la detección de latencia es una etapa posterior con su propia categoría.
- La DLQ conserva el **valor original como string**, no un DTO reserializado, para no perder información de campos desconocidos ni alterar el mensaje.

## 6. Detección de tardíos

Se mantiene un máximo `eventTime` por tienda en `eventtime-store`. Un evento se considera fuera de gracia si:

```
finVentana(eventTime) + graceMs < maxEventTimeVistoPorTienda
```

Ventajas: determinista, barato (una lectura/escritura KeyValue por evento) y funciona con `TopologyTestDriver` sin depender del reloj del sistema.

Limitación: es una heurística propia que complementa —no reemplaza— la gracia interna de Kafka Streams. Un evento puede ser "dentro de gracia" según esta regla y aun así ser descartado por el `WindowStore` si la ventana ya fue purgada. Por eso se monitorizan también los logs `SKIP_FOR_TIME` y las métricas JMX.

## 7. Resiliencia y recuperación

- RF=2 en datos y changelogs ⇒ el estado sobrevive a la pérdida de un broker.
- `acks=all` + idempotencia en el productor del generador y en el de Streams (`exactly_once_v2`).
- `NUM_STREAM_THREADS=2` ⇒ 2 tareas activas como máximo por instancia; con 4 particiones, cada hilo procesa 2. Si cae la app, otra instancia con el mismo `APPLICATION_ID` puede asumir las tareas y restaurar el estado desde los changelogs.
- `setUncaughtExceptionHandler` → `SHUTDOWN_APPLICATION`: ante un error no recuperable se prefiere parar y dejar que el orquestador reinicie, antes que seguir con estado inconsistente.
- `STATE_DIR=/tmp/kafka-streams`: suficiente para el laboratorio. En producción se usaría un volumen persistente.

## 8. Métricas y observabilidad

- Contadores de negocio propios en `/metrics` permiten ver de un vistazo cuántos eventos se aceptaron, rechazaron, duplicaron y agregaron.
- `kafka-exporter` aporta ISR, offline partitions, consumer lag y throughput.
- Grafana agrega ambas fuentes en un dashboard con paneles de ISR, lag y contadores.
- El `peek` antes de publicar el agregado incrementa `ventas_agregadas_total`; es una métrica observacional, no transaccional, por lo que no afecta la corrección del resultado.

## 9. Generador reproducible

- `--base-time` fija `eventTime` de todos los eventos, de modo que los resultados no dependen de cuándo se ejecute el laboratorio.
- Cada escenario usa un `eventId` distinto, salvo el caso `duplicate`, que repite el mismo ID deliberadamente.
- El modo `flush` publica eventos con `eventTime` muy adelantado en 16 tiendas distintas para forzar el cierre de ventanas en todas las particiones y poder observar las salidas de inmediato.
