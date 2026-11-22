package cl.dsy1107.ventas.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generador reproducible de eventos. Ejemplo:
 * --scenario all --bootstrap localhost:9092,localhost:9093 --base-time 2026-01-01T10:00:00Z
 */
public final class EventGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Producer<String, String> producer;
    private static String topico;
    private static long baseMillis;
    private static String escenario;
    private static long sleepMs;

    private EventGenerator() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opciones = parse(args);
        String bootstrap = opciones.getOrDefault("--bootstrap", "localhost:9092,localhost:9093");
        topico = opciones.getOrDefault("--topic", "ventas-raw");
        escenario = opciones.getOrDefault("--scenario", "all");
        baseMillis = Instant.parse(opciones.getOrDefault("--base-time", "2026-01-01T10:00:00Z")).toEpochMilli();
        sleepMs = Long.parseLong(opciones.getOrDefault("--sleep-ms", "30"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        try (Producer<String, String> p = new KafkaProducer<>(props)) {
            producer = p;
            switch (escenario) {
                case "all" -> {
                    validos();
                    jsonMalformado();
                    campoAusente();
                    totalNoCoincide();
                    duplicado();
                    tardioDentroGracia();
                    tardioFueraGracia();
                    vaciarVentanas();
                }
                case "valid" -> validos();
                case "invalid-json" -> jsonMalformado();
                case "missing-field" -> campoAusente();
                case "total-mismatch" -> totalNoCoincide();
                case "invalid-cantidad" -> cantidadInvalida();
                case "duplicate" -> duplicado();
                case "late-within-grace" -> tardioDentroGracia();
                case "late-beyond-grace" -> tardioFueraGracia();
                case "flush" -> vaciarVentanas();
                default -> throw new IllegalArgumentException("Escenario desconocido: " + escenario);
            }
        }
    }

    private static void validos() throws Exception {
        enviarVenta("val-1", "tienda-1", 1, 2, 1990.0, "venta válida 1");
        enviarVenta("val-2", "tienda-1", 2, 1, 350.5, "venta válida 2");
        enviarVenta("val-3", "tienda-1", 3, 3, 100.0, "venta válida 3");
        enviarVenta("val-4", "tienda-2", 10, 1, 500.0, "venta válida otra tienda");
    }

    private static void jsonMalformado() throws Exception {
        enviarRaw("bad-json-1", "{\"eventId\": \"roto\", \"tiendaId\":");
    }

    private static void campoAusente() throws Exception {
        ObjectNode nodo = nodoBase("missing-total-1", "tienda-3", 12, 2, 100.0);
        nodo.remove("total");
        enviarRaw("missing-total-1", nodo.toString());
    }

    private static void totalNoCoincide() throws Exception {
        ObjectNode nodo = nodoBase("bad-total-1", "tienda-3", 13, 2, 100.0);
        nodo.put("total", 999.99);
        enviarRaw("bad-total-1", nodo.toString());
    }

    private static void cantidadInvalida() throws Exception {
        ObjectNode nodo = nodoBase("bad-cantidad-1", "tienda-3", 14, 0, 100.0);
        enviarRaw("bad-cantidad-1", nodo.toString());
    }

    private static void duplicado() throws Exception {
        enviarVenta("dup-evento-1", "tienda-4", 20, 1, 750.0, "primera entrega");
        enviarVenta("dup-evento-1", "tienda-4", 20, 1, 750.0, "reentrega idéntica");
    }

    private static void tardioDentroGracia() throws Exception {
        // max eventTime de tienda-5 = +50s. La ventana [base, base+60) cierra en base+90s.
        enviarVenta("late-ok-1", "tienda-5", 50, 1, 100.0, "avanza reloj de evento");
        enviarVenta("late-ok-2", "tienda-5", 5, 2, 200.0, "tardío DENTRO de gracia");
    }

    private static void tardioFueraGracia() throws Exception {
        // max eventTime de tienda-6 = +240s. La ventana [base, base+60) ya cerró (base+90s).
        enviarVenta("late-bad-1", "tienda-6", 240, 1, 100.0, "avanza reloj de evento");
        enviarVenta("late-bad-2", "tienda-6", 5, 1, 100.0, "tardío FUERA de gracia");
    }

    /** Eventos de reloj muy adelantado para forzar el cierre de ventanas en todas las particiones. */
    private static void vaciarVentanas() throws Exception {
        for (int i = 0; i < 16; i++) {
            enviarVenta("flush-" + i, "tienda-flush-" + i, 600, 1, 1.0, "cierre de ventanas");
        }
    }

    private static ObjectNode nodoBase(String eventId, String tienda, int offsetSeg, int cantidad, double precio) {
        ObjectNode nodo = MAPPER.createObjectNode();
        nodo.put("eventId", eventId);
        nodo.put("tiendaId", tienda);
        nodo.put("eventTime", baseMillis + offsetSeg * 1000L);
        nodo.put("cantidad", cantidad);
        nodo.put("precio", precio);
        nodo.put("total", Math.round(cantidad * precio * 100.0) / 100.0);
        return nodo;
    }

    private static void enviarVenta(String eventId, String tienda, int offsetSeg,
                                    int cantidad, double precio, String nota) throws Exception {
        ObjectNode nodo = nodoBase(eventId, tienda, offsetSeg, cantidad, precio);
        nodo.put("nota", nota);
        enviarRaw(eventId, nodo.toString());
    }

    private static void enviarRaw(String clave, String valor) throws Exception {
        long marcaTiempo = baseMillis;
        try {
            var nodo = MAPPER.readTree(valor);
            if (nodo.has("eventTime")) {
                marcaTiempo = nodo.get("eventTime").asLong();
            }
        } catch (Exception ignorado) {
            // JSON malformado: se conserva el marcaTiempo por defecto.
        }
        producer.send(new ProducerRecord<>(topico, null, marcaTiempo, clave, valor)).get();
        System.out.printf("[%s] key=%s value=%s%n", escenario, clave, valor);
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> resultado = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length || !args[i].startsWith("--")) {
                throw new IllegalArgumentException("Uso: --clave valor ... (argumentos en pares)");
            }
            resultado.put(args[i], args[i + 1]);
        }
        return resultado;
    }
}
