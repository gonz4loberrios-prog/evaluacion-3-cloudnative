# Evidencia de ejecución

Este documento registra la evidencia **real** obtenida en este entorno (Windows + Docker Desktop). Todas las salidas mostradas provienen de ejecutar los scripts incluidos; no son ejemplos hipotéticos.

> En PowerShell, si la política de ejecución bloquea los scripts, usar:
> `powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\<script>.ps1"`.

---

## 1. Pruebas automatizadas (ejecutado)

Comando:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
.\mvnw.cmd -B test
```

Salida relevante:

```text
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.170 s -- in cl.dsy1107.ventas.VentasTopologyTest
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

> Nota: `TopologyTestDriver` emite avisos `Failed to change permissions` sobre el directorio temporal de estado de Windows. No afectan el resultado: los stores en memoria y los changelogs simulados funcionan correctamente y las 5 pruebas pasan.

| Método | Qué demuestra |
|---|---|
| `jsonMalformadoVaALaDlqYNoDetieneElFlujo` | El JSON roto se envía a DLQ con valor original, clave y tópico; el siguiente evento válido se agrega igual |
| `erroresDeNegocioSeClasifican` | `TOTAL_AUSENTE` y `TOTAL_NO_COINCIDE` son motivos distintos |
| `duplicadoNoEngordaElAgregado` | El segundo `eventId` idéntico no incrementa `ventas` y genera DLQ `DUPLICADO` |
| `agregacionPorTiendaYVentana` | Conteo, unidades, monto, ticket promedio, `windowEnd` y separación por tienda/ventana |
| `semanticaTemporal` | Tardío dentro de gracia se agrega; fuera de gracia va a DLQ como `TARDIO` y no como `INVALIDO` |

---

## 2. Entorno verificado (ejecutado)

```text
Docker Engine:          29.9.3   (Docker Desktop iniciado correctamente)
JDK local:              C:\Program Files\Java\jdk-21
Maven:                  Maven Wrapper 3.9.9 (mvnw.cmd)
Kafka Streams:          3.7.1
Confluent Platform:     7.7.1
Conectividad Maven:     HTTP 200 desde repo.maven.apache.org
```

Estado final de los contenedores tras la prueba de failover:

```text
NAME                 STATUS
dsy-grafana          Up 3 minutes
dsy-kafka-1          Up 4 minutes
dsy-kafka-2          Up About a minute
dsy-kafka-exporter   Up 3 minutes
dsy-prometheus       Up 3 minutes
dsy-streams-app      Up About a minute
dsy-zookeeper        Up 4 minutes
```

---

## 3. Tópicos e ISR (ejecutado)

Comandos:

```powershell
.\scripts\topics.ps1
docker compose exec -T kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe
```

Salida real (extracto de los tres tópicos de negocio):

```text
Topic: ventas-raw	TopicId: DEb3fCUlRJCSURK26Mwy6g	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-raw	Partition: 0	Leader: 2	Replicas: 2,1	Isr: 2,1
	Topic: ventas-raw	Partition: 1	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-raw	Partition: 2	Leader: 2	Replicas: 2,1	Isr: 2,1
	Topic: ventas-raw	Partition: 3	Leader: 1	Replicas: 1,2	Isr: 1,2

Topic: ventas-agregadas	TopicId: rnMfrYAOQlefnfXSYkbNNA	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-agregadas	Partition: 0	Leader: 1	Replicas: 2,1	Isr: 1,2
	Topic: ventas-agregadas	Partition: 1	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-agregadas	Partition: 2	Leader: 1	Replicas: 2,1	Isr: 1,2
	Topic: ventas-agregadas	Partition: 3	Leader: 1	Replicas: 1,2	Isr: 1,2

Topic: ventas-raw-dlq	TopicId: QlFWyGpQQuq_8Aqu2Ugxhw	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-raw-dlq	Partition: 0	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-raw-dlq	Partition: 1	Leader: 1	Replicas: 2,1	Isr: 1,2
	Topic: ventas-raw-dlq	Partition: 2	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-raw-dlq	Partition: 3	Leader: 1	Replicas: 2,1	Isr: 1,2
```

Tópicos internos creados por los scripts (RF=2) y por Kafka Streams:

```text
ventas-streams-dsy1107-por-tienda-repartition
ventas-streams-dsy1107-ventanas-store-repartition
ventas-streams-dsy1107-dedup-store-changelog
ventas-streams-dsy1107-eventtime-store-changelog
ventas-streams-dsy1107-ventanas-store-changelog
ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-0000000031-changelog
```

Verificación: 4 particiones por tópico, RF=2, ISR con ambos brokers en estado sano, `min.insync.replicas=1` y `retention.ms=604800000`.

---

## 4. Mensajes representativos (ejecutado)

Comandos:

```powershell
.\scripts\generate.ps1 -Scenario all
.\scripts\generate.ps1 -Scenario flush
.\scripts\consume.ps1 -Topic ventas-agregadas
.\scripts\consume.ps1 -Topic ventas-raw-dlq
```

Base de tiempo: `2026-01-01T10:00:00Z` ⇒ `1767261600000` ms.

### Entrada (`ventas-raw`)

```text
[all] key=val-1 value={"eventId":"val-1","tiendaId":"tienda-1","eventTime":1767261601000,"cantidad":2,"precio":1990.0,"total":3980.0,"nota":"venta válida 1"}
[all] key=val-2 value={"eventId":"val-2","tiendaId":"tienda-1","eventTime":1767261602000,"cantidad":1,"precio":350.5,"total":350.5,"nota":"venta válida 2"}
[all] key=val-3 value={"eventId":"val-3","tiendaId":"tienda-1","eventTime":1767261603000,"cantidad":3,"precio":100.0,"total":300.0,"nota":"venta válida 3"}
[all] key=val-4 value={"eventId":"val-4","tiendaId":"tienda-2","eventTime":1767261610000,"cantidad":1,"precio":500.0,"total":500.0,"nota":"venta válida otra tienda"}
[all] key=bad-json-1 value={"eventId": "roto", "tiendaId":
[all] key=missing-total-1 value={"eventId":"missing-total-1","tiendaId":"tienda-3","eventTime":1767261612000,"cantidad":2,"precio":100.0}
[all] key=bad-total-1 value={"eventId":"bad-total-1","tiendaId":"tienda-3","eventTime":1767261613000,"cantidad":2,"precio":100.0,"total":999.99,"nota":"total no coincide"}
[all] key=dup-evento-1 value={"eventId":"dup-evento-1","tiendaId":"tienda-4","eventTime":1767261620000,"cantidad":1,"precio":750.0,"total":750.0,"nota":"reentrega idéntica"}
[all] key=dup-evento-1 value={"eventId":"dup-evento-1","tiendaId":"tienda-4","eventTime":1767261620000,"cantidad":1,"precio":750.0,"total":750.0,"nota":"reentrega idéntica"}
[all] key=late-bad-2 value={"eventId":"late-bad-2","tiendaId":"tienda-6","eventTime":1767261605000,"cantidad":1,"precio":100.0,"total":100.0,"nota":"tardío FUERA de gracia"}
```

### Salida agregada (`ventas-agregadas`)

```text
tienda-1|1767261600000 | {"tiendaId":"tienda-1","windowStart":1767261600000,"windowEnd":1767261660000,"ventas":3,"unidades":6,"montoTotal":4630.5,"ticketPromedio":1543.5,"actualizadoEn":"2026-11-22T18:36:11.517102327Z"}
tienda-2|1767261600000 | {"tiendaId":"tienda-2","windowStart":1767261600000,"windowEnd":1767261660000,"ventas":1,"unidades":1,"montoTotal":500.0,"ticketPromedio":500.0,"actualizadoEn":"2026-11-22T18:36:11.530009923Z"}
tienda-4|1767261600000 | {"tiendaId":"tienda-4","windowStart":1767261600000,"windowEnd":1767261660000,"ventas":1,"unidades":1,"montoTotal":750.0,"ticketPromedio":750.0,"actualizadoEn":"2026-11-22T18:36:11.517102377Z"}
tienda-5|1767261600000 | {"tiendaId":"tienda-5","windowStart":1767261600000,"windowEnd":1767261660000,"ventas":2,"unidades":3,"montoTotal":500.0,"ticketPromedio":250.0,"actualizadoEn":"2026-11-22T18:36:11.531649404Z"}
tienda-6|1767261840000 | {"tiendaId":"tienda-6","windowStart":1767261840000,"windowEnd":1767261900000,"ventas":1,"unidades":1,"montoTotal":100.0,"ticketPromedio":100.0,"actualizadoEn":"2026-11-22T18:36:11.677614845Z"}
tienda-1|1767434400000 | {"tiendaId":"tienda-1","windowStart":1767434400000,"windowEnd":1767434460000,"ventas":3,"unidades":6,"montoTotal":4630.5,"ticketPromedio":1543.5,"actualizadoEn":"2026-11-22T18:37:17.234825290Z"}
```

Observaciones reales verificadas:

- `tienda-1` acumula 3 ventas por `3980.0 + 350.5 + 300.0 = 4630.5`; ticket promedio `4630.5 / 3 = 1543.5`.
- `tienda-4` tiene **una sola** venta aunque haya dos offsets con el mismo `eventId`.
- `late-bad-2` no produce agregado en `[1767261600000, 1767261660000)`; solo `late-bad-1` se agrega en la ventana posterior `1767261840000`.
- La salida aparece al cerrar la ventana gracias a `suppress(untilWindowCloses(...))`.

### DLQ (`ventas-raw-dlq`)

```text
bad-json-1 | {"categoria":"INVALIDO","motivo":"JSON_MALFORMADO","topicoFuente":"ventas-raw","particion":1,"offset":1,"claveOriginal":"bad-json-1","valorOriginal":"{\"eventId\": \"roto\", \"tiendaId\":","eventId":"","registradoEn":"2026-11-22T18:36:07.367715911Z"}
missing-total-1 | {"categoria":"INVALIDO","motivo":"TOTAL_AUSENTE","topicoFuente":"ventas-raw","particion":1,"offset":2,"claveOriginal":"missing-total-1","valorOriginal":"{\"eventId\":\"missing-total-1\",\"tiendaId\":\"tienda-3\",\"eventTime\":1767261612000,\"cantidad\":2,\"precio\":100.0}","eventId":"","registradoEn":"2026-11-22T18:36:07.375451972Z"}
bad-total-1 | {"categoria":"INVALIDO","motivo":"TOTAL_NO_COINCIDE","topicoFuente":"ventas-raw","particion":1,"offset":3,"claveOriginal":"bad-total-1","valorOriginal":"{\"eventId\":\"bad-total-1\",\"tiendaId\":\"tienda-3\",\"eventTime\":1767261613000,\"cantidad\":2,\"precio\":100.0,\"total\":999.99}","eventId":"","registradoEn":"2026-11-22T18:36:07.377193006Z"}
dup-evento-1 | {"categoria":"DUPLICADO","motivo":"EVENTID_DUPLICADO","topicoFuente":"ventas-raw","particion":2,"offset":2,"claveOriginal":"dup-evento-1","valorOriginal":"{\"eventId\":\"dup-evento-1\",\"tiendaId\":\"tienda-4\",\"eventTime\":1767261620000,\"cantidad\":1,\"precio\":750.0,\"total\":750.0,\"nota\":\"reentrega idéntica\"}","eventId":"dup-evento-1","registradoEn":"2026-11-22T18:36:07.174423385Z"}
late-bad-2 | {"categoria":"TARDIO","motivo":"LATE_BEYOND_GRACE","topicoFuente":"ventas-raw","particion":3,"offset":1,"claveOriginal":"late-bad-2","valorOriginal":"{\"eventId\":\"late-bad-2\",\"tiendaId\":\"tienda-6\",\"eventTime\":1767261605000,\"cantidad\":1,\"precio\":100.0,\"total\":100.0,\"nota\":\"tardío FUERA de gracia\"}","eventId":"late-bad-2","registradoEn":"2026-11-22T18:36:08.471573332Z"}
```

El tardío aparece como `TARDIO`, no como `INVALIDO`, y su JSON de entrada es válido.

---

## 5. Caída de broker, ISR y lag (ejecutado)

Comando:

```powershell
.\scripts\failover.ps1
```

El script usó `--base-time 2026-01-03T10:00:00Z` (`1767434400000`) para no colisionar con la dedup de 24 h del escenario `all`.

### ISR antes / durante / después

```text
===== ISR ANTES (2 brokers, RF=2) =====
Topic: ventas-raw	TopicId: DEb3fCUlRJCSURK26Mwy6g	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-raw	Partition: 0	Leader: 2	Replicas: 2,1	Isr: 2,1
	Topic: ventas-raw	Partition: 1	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-raw	Partition: 2	Leader: 2	Replicas: 2,1	Isr: 2,1
	Topic: ventas-raw	Partition: 3	Leader: 1	Replicas: 1,2	Isr: 1,2

===== ISR DURANTE (kafka-2 detenido) =====
Topic: ventas-raw	TopicId: DEb3fCUlRJCSURK26Mwy6g	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-raw	Partition: 0	Leader: 1	Replicas: 2,1	Isr: 1
	Topic: ventas-raw	Partition: 1	Leader: 1	Replicas: 1,2	Isr: 1
	Topic: ventas-raw	Partition: 2	Leader: 1	Replicas: 2,1	Isr: 1
	Topic: ventas-raw	Partition: 3	Leader: 1	Replicas: 1,2	Isr: 1

===== ISR DESPUES (kafka-2 reiniciado) =====
Topic: ventas-raw	TopicId: DEb3fCUlRJCSURK26Mwy6g	PartitionCount: 4	ReplicationFactor: 2	Configs: min.insync.replicas=1,retention.ms=604800000
	Topic: ventas-raw	Partition: 0	Leader: 1	Replicas: 2,1	Isr: 1,2
	Topic: ventas-raw	Partition: 1	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: ventas-raw	Partition: 2	Leader: 1	Replicas: 2,1	Isr: 1,2
	Topic: ventas-raw	Partition: 3	Leader: 1	Replicas: 1,2	Isr: 1,2
```

### Producción con `acks=all` durante la falla

```text
Produciendo con acks=all contra el broker que queda...
(acks=-1 es el valor interno equivalente a 'all'; enable.idempotence=true)

	acks = -1
	enable.idempotence = true
[main] INFO org.apache.kafka.clients.producer.KafkaProducer - [Producer clientId=producer-1] Instantiated an 
idempotent producer.
[kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.producer.internals.TransactionManager - 
[Producer clientId=producer-1] ProducerId set to 1 with epoch 0
[valid] key=val-1 value={"eventId":"val-1","tiendaId":"tienda-1","eventTime":1767434401000,"cantidad":2,"precio":1990.0,"total":3980.0,"nota":"venta válida 1"}
[valid] key=val-2 value={"eventId":"val-2","tiendaId":"tienda-1","eventTime":1767434402000,"cantidad":1,"precio":350.5,"total":350.5,"nota":"venta válida 2"}
[valid] key=val-3 value={"eventId":"val-3","tiendaId":"tienda-1","eventTime":1767434403000,"cantidad":3,"precio":100.0,"total":300.0,"nota":"venta válida 3"}
[valid] key=val-4 value={"eventId":"val-4","tiendaId":"tienda-2","eventTime":1767434410000,"cantidad":1,"precio":500.0,"total":500.0,"nota":"venta válida otra tienda"}

Cerrando las ventanas del escenario de failover...
[flush] key=flush-13 value={"eventId":"flush-13","tiendaId":"tienda-flush-13","eventTime":1767435000000,"cantidad":1,"precio":1.0,"total":1.0,"nota":"cierre de ventanas"}
[flush] key=flush-14 value={"eventId":"flush-14","tiendaId":"tienda-flush-14","eventTime":1767435000000,"cantidad":1,"precio":1.0,"total":1.0,"nota":"cierre de ventanas"}
[flush] key=flush-15 value={"eventId":"flush-15","tiendaId":"tienda-flush-15","eventTime":1767435000000,"cantidad":1,"precio":1.0,"total":1.0,"nota":"cierre de ventanas"}
```

No apareció `NotEnoughReplicasException`: con `min.insync.replicas=1` basta la réplica de `kafka-1`.

### Lag durante y después

```text
===== LAG DURANTE LA FALLA =====
GROUP                  TOPIC                                             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
ventas-streams-dsy1107 ventas-raw                                        0          11              11              0
ventas-streams-dsy1107 ventas-raw                                        1          14              14              0
ventas-streams-dsy1107 ventas-raw                                        2          16              16              0
ventas-streams-dsy1107 ventas-raw                                        3          8               8               0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     0          8               8               0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     1          12              12              0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     2          30              30              0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     3          17              17              0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 0          6               6               0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 1          9               9               0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 2          26              26              0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 3          15              15              0

===== LAG TRAS RESTAURAR EL ESTADO =====
GROUP                  TOPIC                                             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
ventas-streams-dsy1107 ventas-raw                                        0          11              11              0
ventas-streams-dsy1107 ventas-raw                                        1          14              14              0
ventas-streams-dsy1107 ventas-raw                                        2          16              16              0
ventas-streams-dsy1107 ventas-raw                                        3          8               8               0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     0          8               8               0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     1          12              12              0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     2          30              30              0
ventas-streams-dsy1107 ventas-streams-dsy1107-por-tienda-repartition     3          17              17              0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 0          6               6               0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 1          9               9               0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 2          26              26              0
ventas-streams-dsy1107 ventas-streams-dsy1107-ventanas-store-repartition 3          15              15              0
```

Los `CURRENT-OFFSET` avanzaron respecto del escenario `all` (que terminaba en offsets 10/14/16/8 en `ventas-raw`) y el `LAG` quedó en 0 tanto durante la falla como tras la recuperación.

### Restauración desde changelogs

Tras reiniciar `streams-app`, la app volvió a `RUNNING` restaurando sus stores:

```text
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] Finished restoring changelog ventas-streams-dsy1107-dedup-store-changelog-3 to store dedup-store with a total number of 8 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] Finished restoring changelog ventas-streams-dsy1107-dedup-store-changelog-1 to store dedup-store with a total number of 11 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] Finished restoring changelog ventas-streams-dsy1107-eventtime-store-changelog-3 to store eventtime-store with a total number of 12 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] Finished restoring changelog ventas-streams-dsy1107-eventtime-store-changelog-2 to store eventtime-store with a total number of 20 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] task [1_1] Restored and ready to run
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] task [0_1] Restored and ready to run
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-2] Restoration took 293 ms for all active tasks [1_0, 0_3, 1_3, 0_1, 1_2, 1_1]

dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] Finished restoring changelog ventas-streams-dsy1107-ventanas-store-changelog-1 to store ventanas-store with a total number of 0 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] Finished restoring changelog ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-0000000031-changelog-1 to store KTABLE-SUPPRESS-STATE-STORE-0000000031 with a total number of 9 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] Finished restoring changelog ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-0000000031-changelog-2 to store KTABLE-SUPPRESS-STATE-STORE-0000000031 with a total number of 28 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] Finished restoring changelog ventas-streams-dsy1107-KTABLE-SUPPRESS-STATE-STORE-0000000031-changelog-3 to store KTABLE-SUPPRESS-STATE-STORE-0000000031 with a total number of 18 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] Finished restoring changelog ventas-streams-dsy1107-dedup-store-changelog-2 to store dedup-store with a total number of 15 records
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] task [2_2] Restored and ready to run
dsy-streams-app  | [ventas-streams-dsy1107-2eeff5c9-ce9b-4778-bf2b-2705c46c8aa4-StreamThread-1] Restoration took 1152 ms for all active tasks [2_1, 2_0, 0_2, 2_3, 2_2, 0_0]
dsy-streams-app  | [main] INFO cl.dsy1107.ventas.VentasStreamsApp - Estado Kafka Streams: REBALANCING -> RUNNING
```

Criterios de aceptación verificados:

1. No hubo `NotEnoughReplicasException` durante la caída.
2. La app siguió consumiendo: los offsets avanzaron y el lag volvió a 0.
3. La ISR volvió a incluir ambos brokers.
4. Los logs muestran el reinicio, la restauración de `dedup-store`, `eventtime-store` y `KTABLE-SUPPRESS-STATE-STORE-…`, y la transición final a `RUNNING`.

---

## 6. Métricas y detección de descartes (ejecutado)

Comandos:

```powershell
.\scripts\inspect.ps1
Invoke-WebRequest -UseBasicParsing "http://localhost:18081/metrics"
```

Salida real del endpoint **antes del reinicio de `streams-app`** (dentro de `failover.ps1`):

```text
# TYPE ventas_validas_total counter
ventas_validas_total 46
# TYPE ventas_invalidas_total counter
ventas_invalidas_total 3
# TYPE ventas_tardias_total counter
ventas_tardias_total 1
# TYPE ventas_duplicadas_total counter
ventas_duplicadas_total 1
# TYPE ventas_agregadas_total counter
ventas_agregadas_total 23
```

> Los contadores `ventas_*` son instancias `Counter` en memoria del proceso. Al reiniciar `streams-app` para la prueba de restauración vuelven a 0; este comportamiento se documenta para no confundirlo con pérdida de estado. El estado **de negocio** sí se restaura desde los changelogs, como se vio en la sección 5.

Verificación de targets en `http://localhost:9090/targets`:

```text
kafka-exporter  -> up
prometheus      -> up
ventas-streams  -> up
```

> Durante las pruebas se detectó que las métricas JMX de Kafka usan etiquetas con guiones (`client-id`), formato rechazado por Prometheus. `MetricsServer` las sanitiza a `client_id`, con lo que el target `ventas-streams` queda `UP` y los paneles muestran datos.

Grupo y lag (extracto real de `inspect.ps1`):

```text
===== CONSUMER GROUPS =====
ventas-streams-dsy1107

===== LAG: ventas-streams-dsy1107 =====
GROUP                  TOPIC                                             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
ventas-streams-dsy1107 ventas-raw                                        0          11              11              0
ventas-streams-dsy1107 ventas-raw                                        1          14              14              0
ventas-streams-dsy1107 ventas-raw                                        2          16              16              0
ventas-streams-dsy1107 ventas-raw                                        3          8               8               0
```

En Grafana (`http://localhost:3000`, dashboard **DSY1107 / Ventas**) se observan:

- `kafka_topic_partition_in_sync_replica` por partición (2 en estado sano).
- `kafka_consumergroup_lag` para `ventas-streams-dsy1107` (tiende a 0).
- Contadores de negocio `ventas_*_total`.

Los descartes internos de ventanas cerradas se detectan en los logs con mensajes `SKIP_FOR_TIME` y en las métricas JMX exportadas con prefijo `kafka_streams_`.

---

## 7. Seguridad (ejecutado)

- Solo existe `.env.example` con valores demo (`admin` / `admin`).
- No se creó `.env` con secretos reales; `.gitignore` excluye `.env` y `.env.*` excepto `.env.example`.
