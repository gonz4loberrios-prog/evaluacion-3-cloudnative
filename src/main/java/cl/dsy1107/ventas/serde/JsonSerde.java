package cl.dsy1107.ventas.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/** Serde JSON genérico basado en Jackson para los DTOs de la topología. */
public final class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper mapper;
    private final Class<T> tipo;

    public JsonSerde(Class<T> tipo) {
        this.tipo = tipo;
        this.mapper = new ObjectMapper();
    }

    @Override
    public Serializer<T> serializer() {
        return (topico, dato) -> {
            if (dato == null) {
                return null;
            }
            try {
                return mapper.writeValueAsBytes(dato);
            } catch (Exception e) {
                throw new SerializationException("No se pudo serializar " + tipo.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topico, datos) -> {
            if (datos == null) {
                return null;
            }
            try {
                return mapper.readValue(datos, tipo);
            } catch (Exception e) {
                throw new SerializationException("No se pudo deserializar " + tipo.getSimpleName(), e);
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Sin configuración adicional.
    }

    @Override
    public void close() {
        // Sin recursos que liberar.
    }
}
