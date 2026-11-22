package cl.dsy1107.ventas.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Contadores observacionales de negocio (no forman parte del estado transaccional). */
public final class MetricsRegistry {

    private static final MetricsRegistry INSTANCIA = new MetricsRegistry();

    private final AtomicLong validas = new AtomicLong();
    private final AtomicLong invalidas = new AtomicLong();
    private final AtomicLong tardias = new AtomicLong();
    private final AtomicLong duplicadas = new AtomicLong();
    private final AtomicLong agregadas = new AtomicLong();

    private MetricsRegistry() {
    }

    public static MetricsRegistry instance() {
        return INSTANCIA;
    }

    public void incrementarValidas() {
        validas.incrementAndGet();
    }

    public void incrementarInvalidas() {
        invalidas.incrementAndGet();
    }

    public void incrementarTardias() {
        tardias.incrementAndGet();
    }

    public void incrementarDuplicadas() {
        duplicadas.incrementAndGet();
    }

    public void incrementarAgregadas() {
        agregadas.incrementAndGet();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> resultado = new LinkedHashMap<>();
        resultado.put("ventas_validas_total", validas.get());
        resultado.put("ventas_invalidas_total", invalidas.get());
        resultado.put("ventas_tardias_total", tardias.get());
        resultado.put("ventas_duplicadas_total", duplicadas.get());
        resultado.put("ventas_agregadas_total", agregadas.get());
        return resultado;
    }
}
