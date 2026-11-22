# Matriz de escenarios

Base de tiempo usada por el generador: `2026-01-01T10:00:00Z` ⇒ `BASE = 1767261600000` ms.
Ventana tumbling: 60 s ⇒ `WINDOW_MS=60000`. Gracia: 30 s ⇒ `GRACE_MS=30000`.

Para ejecutarlos en Docker:

```powershell
.\scripts\generate.ps1 -Scenario <nombre>
.\scripts\generate.ps1 -Scenario flush
.\scripts\consume.ps1 -Topic ventas-agregadas
.\scripts\consume.ps1 -Topic ventas-raw-dlq
```

## 1. Escenarios de validación y DLQ

| # | Escenario (`--scenario`) | Entrada resumida | Categoría DLQ | Motivo DLQ | ¿Se agrega? | Cómo verificarlo |
|---:|---|---|---|---|---|---|
| 1 | `valid` | `eventId=val-1`, `tiendaId=tienda-1`, `cantidad=2`, `precio=1990.0`, `total=3980.0` | — | — | Sí | `ventas-agregadas` clave `tienda-1\|1767261600000`, `ventas=1`, `montoTotal=3980.0` |
| 2 | `invalid-json` | `{"eventId": "roto", "tiendaId":` (truncado) | `INVALIDO` | `JSON_MALFORMADO` | No | DLQ con `valorOriginal` exacto y `claveOriginal=bad-json-1` |
| 3 | `missing-field` | JSON sin `total` | `INVALIDO` | `TOTAL_AUSENTE` | No | Un registro en DLQ con `eventId=missing-total-1` |
| 4 | `total-mismatch` | `cantidad=2`, `precio=100.0`, `total=999.99` | `INVALIDO` | `TOTAL_NO_COINCIDE` | No | DLQ con `eventId=bad-total-1` |
| 5 | `invalid-cantidad` | `cantidad=0` | `INVALIDO` | `CANTIDAD_INVALIDA` | No | DLQ con `eventId=bad-cantidad-1` |

**Continuidad del flujo**: en el escenario `all`, después de los errores se publican ventas válidas. El agregado de `tienda-1` debe contenerlas; si el stream se hubiera detenido en el primer JSON roto, no habría salida. Esto está cubierto por la prueba `jsonMalformadoVaALaDlqYNoDetieneElFlujo`.

## 2. Escenarios de duplicados

| # | Escenario | Entrada | Resultado en agregado | DLQ | Verificación |
|---:|---|---|---|---|---|
| 6 | `duplicate` | Dos mensajes con `eventId=dup-evento-1`, `tienda-4`, `total=750.0` | `ventas=1`, `montoTotal=750.0`, `ticketPromedio=750.0` | 1 registro `DUPLICADO / EVENTID_DUPLICADO` | La suma no se duplica aunque haya 2 offsets |

Nota: `exactly_once_v2` no evita este duplicado porque son dos registros legítimos de Kafka. La dedup es de negocio (`dedup-store`).

## 3. Escenarios temporales

| # | Escenario | `eventTime` | Relación con la gracia | Resultado | DLQ |
|---:|---|---|---|---|---|
| 7 | `late-within-grace` | `late-ok-1` en `BASE+50s`; `late-ok-2` en `BASE+5s` | `finVentana (BASE+60s) + 30s = BASE+90s > maxVisto (BASE+50s)` ⇒ dentro | Ambos se agregan en la ventana `[BASE, BASE+60s)`: `ventas=2`, `montoTotal=500.0` | Ninguna |
| 8 | `late-beyond-grace` | `late-bad-1` en `BASE+240s`; `late-bad-2` en `BASE+5s` | `finVentana + 30s = BASE+90s < maxVisto (BASE+240s)` ⇒ fuera | Solo se agrega `late-bad-1` (`ventas=1`, `montoTotal=100.0`). No existe agregado para la ventana `[BASE, BASE+60s)` de `tienda-6` | 1 registro `TARDIO / LATE_BEYOND_GRACE` |

**Punto clave**: el tardío fuera de gracia se clasifica `TARDIO`, **nunca** `INVALIDO`. El JSON es perfectamente válido; lo que ocurre es que su ventana ya cerró.

## 4. Escenarios de agregación

| # | Entrada | Ventana | Salida esperada |
|---:|---|---|---|
| 9 | `v1` tienda-A `+1s` (2 × 100.0), `v2` tienda-A `+2s` (1 × 50.0) | `[BASE, BASE+60s)` tienda-A | `ventas=2`, `unidades=3`, `montoTotal=250.0`, `ticketPromedio=125.0`, `windowEnd=BASE+60000` |
| 10 | `v4` tienda-A `+61s` (1 × 10.0) | `[BASE+60s, BASE+120s)` tienda-A | `ventas=1`, `montoTotal=10.0` |
| 11 | `v3` tienda-B `+3s` (3 × 20.0) | `[BASE, BASE+60s)` tienda-B | `ventas=1`, `unidades=3`, `montoTotal=60.0` ⇒ confirma agrupación por tienda |

La salida solo aparece al cerrar la ventana (gracias a `suppress`), por eso los scripts ejecutan `flush` después.

## 5. Escenarios de infraestructura

| # | Escenario | Procedimiento | Resultado esperado |
|---:|---|---|---|
| 12 | Caída de broker | `.\scripts\failover.ps1` | ISR antes `[1,2]` → durante `[1]` → después `[1,2]`; se produce y consume durante la falla; lag vuelve a 0 y los stores se restauran |
| 13 | Reinicio limpio | `docker compose down -v` → `up -d --build` → `topics.ps1` | Sin tópicos huérfanos; configuración de RF/retención reaplicada |
| 14 | Observabilidad | `.\scripts\inspect.ps1`, Grafana | `/metrics` con contadores > 0; paneles de ISR y lag sin errores |

## 6. Cobertura de pruebas automáticas

| Prueba JUnit | Escenarios cubiertos |
|---|---|
| `jsonMalformadoVaALaDlqYNoDetieneElFlujo` | 1, 2 y continuidad |
| `erroresDeNegocioSeClasifican` | 3, 4 |
| `duplicadoNoEngordaElAgregado` | 6 |
| `agregacionPorTiendaYVentana` | 9, 10, 11 |
| `semanticaTemporal` | 7, 8 |

Resultado registrado: `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0` — `BUILD SUCCESS`.

---

## 7. Evidencia obtenida por escenario

Cada fila de las tablas anteriores se verificó con los comandos de la sección "Para ejecutarlos en Docker". La evidencia real está en [`EVIDENCIA.md`](EVIDENCIA.md):

| Escenarios | Evidencia obtenida |
|---|---|
| 1–5 (validación) | Salidas de `ventas-raw` y `ventas-raw-dlq` con `JSON_MALFORMADO`, `TOTAL_AUSENTE`, `TOTAL_NO_COINCIDE` y continuidad del flujo — §4 de `EVIDENCIA.md` |
| 6 (duplicados) | DLQ `DUPLICADO / EVENTID_DUPLICADO` y un solo agregado para `tienda-4` — §4 de `EVIDENCIA.md` |
| 7–8 (tiempo) | `late-ok-*` agregados; `late-bad-2` en DLQ como `TARDIO / LATE_BEYOND_GRACE` — §4 de `EVIDENCIA.md` |
| 9–11 (agregación) | `ventas-agregadas` con `windowEnd`, `unidades`, `montoTotal` y `ticketPromedio` — §4 de `EVIDENCIA.md` |
| 12 (caída de broker) | ISR `1,2 → 1 → 1,2`, `acks=all`, lag 0 y restauración desde changelogs — §5 de `EVIDENCIA.md` |
| 13 (reinicio limpio) | Tópicos reaprovisionados con RF=2 e ISR `1,2` — §3 de `EVIDENCIA.md` |
| 14 (observabilidad) | `/metrics` con contadores `ventas_*`, targets de Prometheus `UP` y paneles de Grafana — §6 de `EVIDENCIA.md` |
| Automáticas | 5/5 pruebas, `BUILD SUCCESS` — §1 de `EVIDENCIA.md` |

Para reproducir las capturas en orden, ver [`GUIA_PRESENTACION.md`](GUIA_PRESENTACION.md).
