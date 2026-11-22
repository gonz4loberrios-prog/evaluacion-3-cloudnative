package cl.dsy1107.ventas.model;

/** DTO tipado del evento de venta publicado por el productor en ventas-raw. */
public record Venta(
        String eventId,
        String tiendaId,
        long eventTime,
        int cantidad,
        double precio,
        double total) {
}
