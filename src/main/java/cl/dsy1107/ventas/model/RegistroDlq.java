package cl.dsy1107.ventas.model;

/** Registro escrito en ventas-raw-dlq: preserva valor original, clave, tópico y motivo. */
public record RegistroDlq(
        String categoria,
        String motivo,
        String topicoFuente,
        int particion,
        long offset,
        String claveOriginal,
        String valorOriginal,
        String eventId,
        String registradoEn) {
}
