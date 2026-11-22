package cl.dsy1107.ventas.model;

/** Resumen publicado en ventas-agregadas al cerrarse la ventana. */
public record VentaAgregada(
        String tiendaId,
        long windowStart,
        long windowEnd,
        long ventas,
        long unidades,
        double montoTotal,
        double ticketPromedio,
        String actualizadoEn) {
}
