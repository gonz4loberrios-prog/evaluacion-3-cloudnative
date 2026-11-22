package cl.dsy1107.ventas.topology;

import cl.dsy1107.ventas.metrics.MetricsRegistry;
import cl.dsy1107.ventas.model.EstadoEvento;
import cl.dsy1107.ventas.model.EventoProcesado;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Deduplicación de negocio por eventId usando un state store de Kafka Streams.
 * El store está particionado por la clave del stream (eventId), por lo que
 * todos los eventos con el mismo eventId llegan a la misma tarea.
 */
public final class DedupTransformer
        implements Transformer<String, EventoProcesado, KeyValue<String, EventoProcesado>> {

    public static final String NOMBRE_STORE = "dedup-store";
    private static final String MOTIVO = "EVENTID_DUPLICADO";

    private final long retentionMs;
    private ProcessorContext context;
    private KeyValueStore<String, Long> store;

    public DedupTransformer(long retentionMs) {
        this.retentionMs = retentionMs;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, Long>) context.getStateStore(NOMBRE_STORE);
    }

    @Override
    public KeyValue<String, EventoProcesado> transform(String clave, EventoProcesado valor) {
        if (valor.estado() != EstadoEvento.VALIDO) {
            return KeyValue.pair(clave, valor);
        }

        Long primero = store.get(clave);
        long ahora = valor.eventTime();
        if (primero != null && ahora - primero <= retentionMs) {
            MetricsRegistry.instance().incrementarDuplicadas();
            return KeyValue.pair(clave, valor.conEstado(EstadoEvento.DUPLICADO, MOTIVO));
        }

        store.put(clave, ahora);
        return KeyValue.pair(clave, valor);
    }

    @Override
    public void close() {
        // El store es gestionado por Kafka Streams.
    }
}
