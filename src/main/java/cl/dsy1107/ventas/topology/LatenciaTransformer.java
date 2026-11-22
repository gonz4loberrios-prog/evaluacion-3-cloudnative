package cl.dsy1107.ventas.topology;

import cl.dsy1107.ventas.metrics.MetricsRegistry;
import cl.dsy1107.ventas.model.EstadoEvento;
import cl.dsy1107.ventas.model.EventoProcesado;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Clasifica eventos tardíos comparando su ventana con el mayor eventTime visto
 * por tienda. Un evento dentro de gracia se agrega; fuera de gracia se envía a DLQ
 * con motivo LATE_BEYOND_GRACE (nunca como JSON inválido).
 */
public final class LatenciaTransformer
        implements Transformer<String, EventoProcesado, KeyValue<String, EventoProcesado>> {

    public static final String NOMBRE_STORE = "eventtime-store";
    public static final String MOTIVO = "LATE_BEYOND_GRACE";

    private final long windowMs;
    private final long graceMs;
    private ProcessorContext context;
    private KeyValueStore<String, Long> store;

    public LatenciaTransformer(long windowMs, long graceMs) {
        this.windowMs = windowMs;
        this.graceMs = graceMs;
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

        String tienda = valor.tiendaId();
        Long anterior = store.get(tienda);
        long maximoVisto = anterior == null ? valor.eventTime() : Math.max(anterior, valor.eventTime());

        long inicioVentana = valor.eventTime() - (valor.eventTime() % windowMs);
        long finVentana = inicioVentana + windowMs;

        if (finVentana + graceMs < maximoVisto) {
            MetricsRegistry.instance().incrementarTardias();
            return KeyValue.pair(clave, valor.conEstado(EstadoEvento.TARDIO, MOTIVO));
        }

        store.put(tienda, maximoVisto);
        return KeyValue.pair(clave, valor);
    }

    @Override
    public void close() {
        // El store es gestionado por Kafka Streams.
    }
}
