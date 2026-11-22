package cl.dsy1107.ventas.model;

import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Instant;

/**
 * Value object interno que viaja por la topología. Conserva la metadata original
 * (tópico/partición/offset/clave) para que la DLQ sea correlacionable.
 */
public record EventoProcesado(
        String eventId,
        String tiendaId,
        long eventTime,
        int cantidad,
        double precio,
        double total,
        EstadoEvento estado,
        String motivo,
        String valorOriginal,
        String claveOriginal,
        String topicoFuente,
        int particion,
        long offset) {

    public static EventoProcesado valido(
            Venta venta, String claveOriginal, String valorOriginal, ProcessorContext ctx) {
        return new EventoProcesado(
                venta.eventId(), venta.tiendaId(), venta.eventTime(),
                venta.cantidad(), venta.precio(), venta.total(),
                EstadoEvento.VALIDO, null, valorOriginal, claveOriginal,
                ctx == null ? "" : ctx.topic(),
                ctx == null ? -1 : ctx.partition(),
                ctx == null ? -1L : ctx.offset());
    }

    public static EventoProcesado rechazado(
            EstadoEvento estado, String motivo, String raw,
            String claveOriginal, ProcessorContext ctx, Venta venta) {
        return new EventoProcesado(
                venta == null ? "" : venta.eventId(),
                venta == null ? "" : venta.tiendaId(),
                venta == null ? 0L : venta.eventTime(),
                venta == null ? 0 : venta.cantidad(),
                venta == null ? 0.0d : venta.precio(),
                venta == null ? 0.0d : venta.total(),
                estado, motivo, raw, claveOriginal,
                ctx == null ? "" : ctx.topic(),
                ctx == null ? -1 : ctx.partition(),
                ctx == null ? -1L : ctx.offset());
    }

    public EventoProcesado conEstado(EstadoEvento nuevoEstado, String nuevoMotivo) {
        return new EventoProcesado(
                eventId, tiendaId, eventTime, cantidad, precio, total,
                nuevoEstado, nuevoMotivo, valorOriginal, claveOriginal,
                topicoFuente, particion, offset);
    }

    public RegistroDlq aRegistroDlq() {
        return new RegistroDlq(
                estado.name(),
                motivo == null ? "" : motivo,
                topicoFuente,
                particion,
                offset,
                claveOriginal,
                valorOriginal,
                eventId,
                Instant.now().toString());
    }
}
