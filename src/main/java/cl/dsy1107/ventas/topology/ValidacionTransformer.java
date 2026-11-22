package cl.dsy1107.ventas.topology;

import cl.dsy1107.ventas.metrics.MetricsRegistry;
import cl.dsy1107.ventas.model.EstadoEvento;
import cl.dsy1107.ventas.model.EventoProcesado;
import cl.dsy1107.ventas.validation.Validador;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

/** Valida JSON, campos y reglas de negocio; los rechazos se clasifican como INVALIDO. */
public final class ValidacionTransformer
        implements Transformer<String, String, KeyValue<String, EventoProcesado>> {

    private final Validador validador = new Validador();
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<String, EventoProcesado> transform(String clave, String valor) {
        Validador.Resultado resultado = validador.validar(valor);
        if (!resultado.esValido()) {
            MetricsRegistry.instance().incrementarInvalidas();
            EventoProcesado rechazo = EventoProcesado.rechazado(
                    EstadoEvento.INVALIDO, resultado.motivo(), valor, clave, context, null);
            return KeyValue.pair(clave, rechazo);
        }
        MetricsRegistry.instance().incrementarValidas();
        EventoProcesado evento = EventoProcesado.valido(resultado.venta(), clave, valor, context);
        return KeyValue.pair(clave, evento);
    }

    @Override
    public void close() {
        // Sin recursos propios.
    }
}
