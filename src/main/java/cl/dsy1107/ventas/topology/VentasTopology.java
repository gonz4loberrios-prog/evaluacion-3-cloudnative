package cl.dsy1107.ventas.topology;

import cl.dsy1107.ventas.config.AppConfig;
import cl.dsy1107.ventas.metrics.MetricsRegistry;
import cl.dsy1107.ventas.model.Agregado;
import cl.dsy1107.ventas.model.EstadoEvento;
import cl.dsy1107.ventas.model.EventoProcesado;
import cl.dsy1107.ventas.model.RegistroDlq;
import cl.dsy1107.ventas.model.VentaAgregada;
import cl.dsy1107.ventas.serde.JsonSerde;
import cl.dsy1107.ventas.serde.VentaTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Topología:
 * ventas-raw → validación → dedup por eventId → repartición por tienda
 *            → control de latencia → ventanas tumbling → ventas-agregadas
 * Rechazos (INVALIDO / DUPLICADO / TARDIO) → ventas-raw-dlq.
 */
public final class VentasTopology {

    public static Topology build(AppConfig cfg) {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<EventoProcesado> eventoSerde = new JsonSerde<>(EventoProcesado.class);
        Serde<Agregado> agregadoSerde = new JsonSerde<>(Agregado.class);
        Serde<VentaAgregada> dtoSerde = new JsonSerde<>(VentaAgregada.class);
        Serde<RegistroDlq> dlqSerde = new JsonSerde<>(RegistroDlq.class);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(DedupTransformer.NOMBRE_STORE),
                Serdes.String(), Serdes.Long()).withLoggingEnabled(Map.of()));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(LatenciaTransformer.NOMBRE_STORE),
                Serdes.String(), Serdes.Long()).withLoggingEnabled(Map.of()));

        KStream<String, EventoProcesado> clasificados = builder
                .stream(cfg.rawTopic(), Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new VentaTimestampExtractor()))
                .<String, EventoProcesado>transform(ValidacionTransformer::new);

        KStream<String, EventoProcesado>[] porValidacion = clasificados.branch(
                (clave, valor) -> valor.estado() == EstadoEvento.VALIDO,
                (clave, valor) -> true);
        escribirDlq(porValidacion[1], cfg.dlqTopic(), dlqSerde);

        KStream<String, EventoProcesado>[] porDedup = porValidacion[0]
                .transform(() -> new DedupTransformer(cfg.dedupRetentionMs()),
                        DedupTransformer.NOMBRE_STORE)
                .branch(
                        (clave, valor) -> valor.estado() == EstadoEvento.VALIDO,
                        (clave, valor) -> true);
        escribirDlq(porDedup[1], cfg.dlqTopic(), dlqSerde);

        KStream<String, EventoProcesado>[] porLatencia = porDedup[0]
                .selectKey((clave, valor) -> valor.tiendaId())
                .repartition(Repartitioned.with(Serdes.String(), eventoSerde)
                        .withName("por-tienda")
                        .withNumberOfPartitions(4))
                .transform(() -> new LatenciaTransformer(cfg.windowMs(), cfg.graceMs()),
                        LatenciaTransformer.NOMBRE_STORE)
                .branch(
                        (clave, valor) -> valor.estado() == EstadoEvento.VALIDO,
                        (clave, valor) -> true);
        escribirDlq(porLatencia[1], cfg.dlqTopic(), dlqSerde);

        KTable<Windowed<String>, Agregado> ventanas = porLatencia[0]
                .groupByKey(Grouped.with(Serdes.String(), eventoSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofMillis(cfg.windowMs()), Duration.ofMillis(cfg.graceMs())))
                .aggregate(
                        Agregado::vacio,
                        (clave, valor, acumulado) -> acumulado.sumar(valor),
                        Materialized.<String, Agregado, WindowStore<Bytes, byte[]>>as("ventanas-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(agregadoSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        ventanas.toStream()
                .map((Windowed<String> clave, Agregado acumulado) -> {
                    String tienda = clave.key();
                    long inicio = clave.window().start();
                    long fin = clave.window().end();
                    long ventas = acumulado.ventas();
                    double promedio = ventas == 0L ? 0.0d : acumulado.montoTotal() / ventas;
                    VentaAgregada resumen = new VentaAgregada(
                            tienda, inicio, fin, ventas, acumulado.unidades(),
                            acumulado.montoTotal(), Math.round(promedio * 100.0d) / 100.0d,
                            Instant.now().toString());
                    return KeyValue.pair(tienda + "|" + inicio, resumen);
                })
                .peek((clave, valor) -> MetricsRegistry.instance().incrementarAgregadas())
                .to(cfg.aggTopic(), Produced.with(Serdes.String(), dtoSerde));

        return builder.build();
    }

    private static void escribirDlq(
            KStream<String, EventoProcesado> stream, String topico, Serde<RegistroDlq> dlqSerde) {
        // La ruta de latencia re-clavea por tienda; al publicar en la DLQ se restaura
        // la clave original del evento para mantener la correlación con ventas-raw.
        stream.selectKey((clave, valor) -> valor.claveOriginal())
                .mapValues(EventoProcesado::aRegistroDlq)
                .to(topico, Produced.with(Serdes.String(), dlqSerde));
    }
}
