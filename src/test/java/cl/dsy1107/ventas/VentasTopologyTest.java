package cl.dsy1107.ventas;

import cl.dsy1107.ventas.config.AppConfig;
import cl.dsy1107.ventas.model.RegistroDlq;
import cl.dsy1107.ventas.model.VentaAgregada;
import cl.dsy1107.ventas.serde.JsonSerde;
import cl.dsy1107.ventas.serde.VentaTimestampExtractor;
import cl.dsy1107.ventas.topology.VentasTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pruebas de la topología: validación, DLQ, duplicados, agregación y semántica temporal. */
class VentasTopologyTest {

    private static final long BASE = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, RegistroDlq> dlq;
    private TestOutputTopic<String, VentaAgregada> agregadas;

    @BeforeEach
    void configurar() {
        AppConfig cfg = new AppConfig(
                "dummy:9092", "test-app", "ventas-raw", "ventas-agregadas", "ventas-raw-dlq",
                60_000L, 30_000L, 86_400_000L, 1, "/tmp/test-state", 0);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, VentaTimestampExtractor.class.getName());

        driver = new TopologyTestDriver(VentasTopology.build(cfg), props);
        raw = driver.createInputTopic(cfg.rawTopic(), new StringSerializer(), new StringSerializer());
        dlq = driver.createOutputTopic(cfg.dlqTopic(),
                new StringDeserializer(), new JsonSerde<>(RegistroDlq.class).deserializer());
        agregadas = driver.createOutputTopic(cfg.aggTopic(),
                new StringDeserializer(), new JsonSerde<>(VentaAgregada.class).deserializer());
    }

    @AfterEach
    void cerrar() {
        driver.close();
    }

    @Test
    @DisplayName("JSON malformado va a DLQ con contexto y el flujo sigue procesando")
    void jsonMalformadoVaALaDlqYNoDetieneElFlujo() {
        String roto = "{\"eventId\": \"roto\", \"tiendaId\":";
        raw.pipeInput("bad-1", roto, BASE + 1_000L);
        raw.pipeInput("val-1", venta("val-1", "tienda-1", 2, 2, 100.0), BASE + 2_000L);
        cerrarVentanas();

        List<RegistroDlq> rechazos = dlq.readValuesToList();
        assertEquals(1, rechazos.size());
        RegistroDlq rechazo = rechazos.get(0);
        assertEquals("INVALIDO", rechazo.categoria());
        assertEquals("JSON_MALFORMADO", rechazo.motivo());
        assertEquals(roto, rechazo.valorOriginal());
        assertEquals("ventas-raw", rechazo.topicoFuente());
        assertEquals("bad-1", rechazo.claveOriginal());

        VentaAgregada agregado = unico("tienda-1|" + BASE);
        assertEquals(1L, agregado.ventas());
        assertEquals(200.0, agregado.montoTotal());
    }

    @Test
    @DisplayName("Campo ausente y total inconsistente se clasifican con motivos distintos")
    void erroresDeNegocioSeClasifican() {
        ObjectNode sinTotal = ventaNode("sin-total", "tienda-9", 3, 1, 50.0);
        sinTotal.remove("total");
        raw.pipeInput("sin-total", sinTotal.toString(), BASE + 3_000L);

        ObjectNode totalMalo = ventaNode("total-malo", "tienda-9", 4, 2, 50.0);
        totalMalo.put("total", 999.0);
        raw.pipeInput("total-malo", totalMalo.toString(), BASE + 4_000L);
        cerrarVentanas();

        List<String> motivos = dlq.readValuesToList().stream().map(RegistroDlq::motivo).toList();
        assertTrue(motivos.contains("TOTAL_AUSENTE"), motivos.toString());
        assertTrue(motivos.contains("TOTAL_NO_COINCIDE"), motivos.toString());
    }

    @Test
    @DisplayName("Mismo eventId dos veces: el agregado no se duplica y la DLQ explica el descarte")
    void duplicadoNoEngordaElAgregado() {
        String evento = venta("dup-1", "tienda-4", 20, 1, 750.0);
        raw.pipeInput("dup-1", evento, BASE + 20_000L);
        raw.pipeInput("dup-1", evento, BASE + 20_000L);
        cerrarVentanas();

        VentaAgregada agregado = unico("tienda-4|" + BASE);
        assertEquals(1L, agregado.ventas());
        assertEquals(750.0, agregado.montoTotal());

        List<KeyValue<String, RegistroDlq>> rechazos = dlq.readKeyValuesToList();
        assertEquals(1, rechazos.size());
        assertEquals("dup-1", rechazos.get(0).key);
        assertEquals("DUPLICADO", rechazos.get(0).value.categoria());
        assertEquals("EVENTID_DUPLICADO", rechazos.get(0).value.motivo());
        assertEquals(evento, rechazos.get(0).value.valorOriginal());
        assertEquals("dup-1", rechazos.get(0).value.claveOriginal());
    }

    @Test
    @DisplayName("Agregación por tienda y ventana con conteo, monto y ticket promedio")
    void agregacionPorTiendaYVentana() {
        raw.pipeInput("v1", venta("v1", "tienda-A", 1, 2, 100.0), BASE + 1_000L);
        raw.pipeInput("v2", venta("v2", "tienda-A", 2, 1, 50.0), BASE + 2_000L);
        raw.pipeInput("v3", venta("v3", "tienda-B", 3, 3, 20.0), BASE + 3_000L);
        raw.pipeInput("v4", venta("v4", "tienda-A", 61, 1, 10.0), BASE + 61_000L);
        cerrarVentanas();

        List<VentaAgregada> salidas = agregadas.readValuesToList();
        VentaAgregada ventana0A = unico(salidas, "tienda-A|" + BASE);
        assertEquals(2L, ventana0A.ventas());
        assertEquals(3L, ventana0A.unidades());
        assertEquals(250.0, ventana0A.montoTotal());
        assertEquals(125.0, ventana0A.ticketPromedio());
        assertEquals(BASE + 60_000L, ventana0A.windowEnd());

        VentaAgregada ventana1A = unico(salidas, "tienda-A|" + (BASE + 60_000L));
        assertEquals(1L, ventana1A.ventas());
        assertEquals(10.0, ventana1A.montoTotal());

        VentaAgregada ventana0B = unico(salidas, "tienda-B|" + BASE);
        assertEquals(1L, ventana0B.ventas());
        assertEquals(60.0, ventana0B.montoTotal());
        assertTrue(dlq.readValuesToList().isEmpty());
    }

    @Test
    @DisplayName("Tardío dentro de gracia se agrega; fuera de gracia va a DLQ como TARDIO")
    void semanticaTemporal() {
        raw.pipeInput("t1", venta("t1", "tienda-T", 50, 1, 100.0), BASE + 50_000L);
        raw.pipeInput("t2", venta("t2", "tienda-T", 5, 2, 200.0), BASE + 5_000L);
        raw.pipeInput("u1", venta("u1", "tienda-U", 240, 1, 100.0), BASE + 240_000L);
        raw.pipeInput("u2", venta("u2", "tienda-U", 5, 1, 100.0), BASE + 5_000L);
        cerrarVentanas();

        List<VentaAgregada> salidas = agregadas.readValuesToList();
        VentaAgregada dentro = unico(salidas, "tienda-T|" + BASE);
        assertEquals(2L, dentro.ventas(), "el evento dentro de gracia debe sumarse. Salidas: " + salidas);
        assertEquals(500.0, dentro.montoTotal());

        assertTrue(salidas.stream().noneMatch(v -> ("tienda-U|" + BASE).equals(claveDe(v))),
                "no debe existir agregado de tienda-U en la ventana cerrada");
        assertEquals(1L, salidas.stream()
                .filter(v -> "tienda-U".equals(v.tiendaId()))
                .count(), "solo se agrega el evento a tiempo de tienda-U");

        List<KeyValue<String, RegistroDlq>> rechazos = dlq.readKeyValuesToList();
        assertEquals(1, rechazos.size());
        assertEquals("u2", rechazos.get(0).key, "la DLQ conserva la clave original tras el reparto");
        assertEquals("TARDIO", rechazos.get(0).value.categoria());
        assertEquals("LATE_BEYOND_GRACE", rechazos.get(0).value.motivo());
        assertTrue(rechazos.get(0).value.valorOriginal() != null
                        && rechazos.get(0).value.valorOriginal().contains("u2"),
                "DLQ tardía: " + rechazos.get(0));
        assertTrue(rechazos.stream().noneMatch(r -> "INVALIDO".equals(r.value.categoria())));
    }

    private void cerrarVentanas() {
        raw.pipeInput("flush-test", venta("flush-test", "tienda-flush", 600, 1, 1.0), BASE + 600_000L);
    }

    private VentaAgregada unico(String clave) {
        return unico(agregadas.readValuesToList(), clave);
    }

    private VentaAgregada unico(List<VentaAgregada> salidas, String clave) {
        List<VentaAgregada> encontrados = salidas.stream()
                .filter(v -> clave.equals(claveDe(v)))
                .toList();
        assertEquals(1, encontrados.size(),
                "Se esperaba un único agregado para " + clave + ". Salidas: " + salidas);
        return encontrados.get(0);
    }

    private String claveDe(VentaAgregada v) {
        return v.tiendaId() + "|" + v.windowStart();
    }

    private static String venta(String id, String tienda, int offsetSeg, int cantidad, double precio) {
        return ventaNode(id, tienda, offsetSeg, cantidad, precio).toString();
    }

    private static ObjectNode ventaNode(String id, String tienda, int offsetSeg, int cantidad, double precio) {
        ObjectNode nodo = MAPPER.createObjectNode();
        nodo.put("eventId", id);
        nodo.put("tiendaId", tienda);
        nodo.put("eventTime", BASE + offsetSeg * 1000L);
        nodo.put("cantidad", cantidad);
        nodo.put("precio", precio);
        nodo.put("total", Math.round(cantidad * precio * 100.0) / 100.0);
        return nodo;
    }
}
