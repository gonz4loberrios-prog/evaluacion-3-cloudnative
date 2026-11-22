package cl.dsy1107.ventas.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/** Usa eventTime del payload (no el timestamp de llegada) como tiempo de evento. */
public final class VentaTimestampExtractor implements TimestampExtractor {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Object valor = record.value();
        try {
            JsonNode nodo = null;
            if (valor instanceof String texto) {
                nodo = mapper.readTree(texto);
            } else if (valor instanceof byte[] bytes) {
                nodo = mapper.readTree(bytes);
            } else if (valor != null) {
                nodo = mapper.valueToTree(valor);
            }
            if (nodo != null && nodo.isObject()) {
                JsonNode tiempo = nodo.get("eventTime");
                if (tiempo != null && tiempo.isIntegralNumber() && tiempo.canConvertToLong()) {
                    return tiempo.asLong();
                }
            }
        } catch (Exception ignorado) {
            // JSON malformado: se usa el timestamp del broker; el validador ya registrará el rechazo.
        }
        return record.timestamp();
    }
}
