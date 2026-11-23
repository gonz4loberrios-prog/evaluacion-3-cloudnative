# Capturas para el informe

Guarda aquí los pantallazos siguiendo el orden de [`../GUIA_PRESENTACION.md`](../GUIA_PRESENTACION.md).

| Archivo | Contenido |
|---|---|
| `00-stack.png` | `docker compose ps` con los 7 servicios `Up` |
| `01-repo.png` | `git status` + `git log --oneline -1` (commit `50204e0`) |
| `02-arquitectura.png` | Diagrama del flujo en `README.md` o `docs/ARQUITECTURA.md` |
| `03-topics-isr.png` | `kafka-topics --describe` con 4 particiones, RF=2, `Isr: 1,2` |
| `04-generacion.png` | Salida de `generate.ps1 -Scenario all -BaseTime 2026-01-12T10:00:00Z` |
| `05-metricas.png` | `http://localhost:18081/metrics` (`26/3/1/1/19`) |
| `06-agregados.png` | `ventas-agregadas`, ventana `1768212000000` |
| `07-dlq.png` | `ventas-raw-dlq` con las 5 categorías de rechazo |
| `08-lag.png` | `kafka-consumer-groups --describe`, `LAG = 0` |
| `09-failover.png` | Evidencia ISR `1,2 → 1 → 1,2` |
| `10-restauracion.png` | Logs `Restored and ready to run` / `Restoration took` |
| `11-grafana.png` | Dashboard **Streaming resiliente de ventas - DSY1107** |

Convención: PNG, nombres en minúsculas con guion y prefijo numérico para que el orden en el informe sea automático.
