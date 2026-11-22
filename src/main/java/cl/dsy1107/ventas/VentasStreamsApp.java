package cl.dsy1107.ventas;

import cl.dsy1107.ventas.config.AppConfig;
import cl.dsy1107.ventas.metrics.MetricsServer;
import cl.dsy1107.ventas.serde.VentaTimestampExtractor;
import cl.dsy1107.ventas.topology.VentasTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** Punto de entrada de la aplicación Kafka Streams. */
public final class VentasStreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(VentasStreamsApp.class);

    private VentasStreamsApp() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.fromEnv();
        Topology topology = VentasTopology.build(cfg);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, VentaTimestampExtractor.class.getName());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, cfg.numStreamThreads());
        props.put(StreamsConfig.STATE_DIR_CONFIG, cfg.stateDir());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setStateListener((nuevo, anterior) ->
                LOG.info("Estado Kafka Streams: {} -> {}", anterior, nuevo));
        streams.setUncaughtExceptionHandler(error -> {
            LOG.error("Error no recuperable en Kafka Streams", error);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        MetricsServer metrics = new MetricsServer(cfg.metricsPort(), streams);
        metrics.start();
        streams.start();
        LOG.info("Aplicación iniciada. bootstrap={} topicos={}/{}/{} window={}ms grace={}ms",
                cfg.bootstrapServers(), cfg.rawTopic(), cfg.aggTopic(), cfg.dlqTopic(),
                cfg.windowMs(), cfg.graceMs());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Cerrando Kafka Streams y endpoint de métricas...");
            streams.close();
            metrics.stop();
        }, "shutdown-hook"));
    }
}
