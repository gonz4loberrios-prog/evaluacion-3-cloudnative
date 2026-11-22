package cl.dsy1107.ventas.metrics;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Endpoint /metrics en formato Prometheus: contadores de negocio + métricas JMX de Kafka Streams. */
public final class MetricsServer {

    private final HttpServer servidor;
    private final KafkaStreams streams;

    public MetricsServer(int puerto, KafkaStreams streams) throws IOException {
        this.streams = streams;
        this.servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        this.servidor.createContext("/metrics", intercambio -> {
            byte[] cuerpo = renderizar().getBytes(StandardCharsets.UTF_8);
            intercambio.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            intercambio.sendResponseHeaders(200, cuerpo.length);
            try (OutputStream salida = intercambio.getResponseBody()) {
                salida.write(cuerpo);
            }
        });
        this.servidor.setExecutor(null);
    }

    public void start() {
        servidor.start();
    }

    public void stop() {
        servidor.stop(0);
    }

    private String renderizar() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : MetricsRegistry.instance().snapshot().entrySet()) {
            sb.append("# TYPE ").append(entry.getKey()).append(" counter\n");
            sb.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
        }

        Set<String> tiposEscritos = new HashSet<>();
        if (streams != null) {
            for (Metric metric : streams.metrics().values()) {
                if (!(metric.metricValue() instanceof Number numero)) {
                    continue;
                }
                String nombre = "kafka_streams_" + sanitizar(metric.metricName().name());
                StringBuilder etiquetas = new StringBuilder();
                metric.metricName().tags().forEach((clave, valor) -> {
                    if (etiquetas.length() > 0) {
                        etiquetas.append(',');
                    }
                    etiquetas.append(clave).append("=\"")
                            .append(valor.replace("\\", "\\\\").replace("\"", "\\\""))
                            .append('"');
                });
                if (tiposEscritos.add(nombre)) {
                    sb.append("# TYPE ").append(nombre).append(" gauge\n");
                }
                sb.append(nombre).append('{').append(etiquetas).append("} ")
                        .append(numero).append('\n');
            }
        }
        return sb.toString();
    }

    private static String sanitizar(String nombre) {
        String limpio = nombre.replaceAll("[^a-zA-Z0-9_]", "_");
        return limpio.isEmpty() ? "metric" : limpio;
    }
}
