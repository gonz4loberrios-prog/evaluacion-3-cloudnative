package cl.dsy1107.ventas.validation;

import cl.dsy1107.ventas.model.Venta;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

/** Validación de sintaxis JSON, campos obligatorios, eventTime y reglas de negocio. */
public final class Validador {

    private static final BigDecimal TOLERANCIA = new BigDecimal("0.01");
    private static final long EVENTTIME_MINIMO = 946_684_800_000L; // 2000-01-01

    private final ObjectMapper mapper = new ObjectMapper();

    public Resultado validar(String raw) {
        if (raw == null || raw.isBlank()) {
            return Resultado.rechazado("VALOR_VACIO");
        }

        JsonNode nodo;
        try {
            nodo = mapper.readTree(raw);
        } catch (Exception e) {
            return Resultado.rechazado("JSON_MALFORMADO");
        }
        if (nodo == null || !nodo.isObject()) {
            return Resultado.rechazado("JSON_NO_OBJETO");
        }

        String eventId = texto(nodo, "eventId");
        if (eventId == null || eventId.isBlank()) {
            return Resultado.rechazado("EVENTID_AUSENTE");
        }
        if (eventId.length() > 128) {
            return Resultado.rechazado("EVENTID_INVALIDO");
        }

        String tiendaId = texto(nodo, "tiendaId");
        if (tiendaId == null || tiendaId.isBlank()) {
            return Resultado.rechazado("TIENDAID_AUSENTE");
        }
        if (tiendaId.length() > 128) {
            return Resultado.rechazado("TIENDAID_INVALIDO");
        }

        JsonNode nodoTiempo = nodo.get("eventTime");
        if (nodoTiempo == null || nodoTiempo.isNull()) {
            return Resultado.rechazado("EVENTTIME_AUSENTE");
        }
        if (!nodoTiempo.isIntegralNumber() || !nodoTiempo.canConvertToLong()) {
            return Resultado.rechazado("EVENTTIME_INVALIDO");
        }
        long eventTime = nodoTiempo.asLong();
        if (eventTime < EVENTTIME_MINIMO) {
            return Resultado.rechazado("EVENTTIME_INVALIDO");
        }

        JsonNode nodoCantidad = nodo.get("cantidad");
        if (nodoCantidad == null || nodoCantidad.isNull()) {
            return Resultado.rechazado("CANTIDAD_AUSENTE");
        }
        if (!nodoCantidad.isIntegralNumber() || !nodoCantidad.canConvertToInt() || nodoCantidad.asInt() <= 0) {
            return Resultado.rechazado("CANTIDAD_INVALIDA");
        }
        int cantidad = nodoCantidad.asInt();

        Double precio = numero(nodo, "precio", "PRECIO_AUSENTE", "PRECIO_INVALIDO");
        if (precio == null) {
            return Resultado.rechazado(motivoNumeroTemporal(nodo, "precio", "PRECIO_AUSENTE", "PRECIO_INVALIDO"));
        }
        Double total = numero(nodo, "total", "TOTAL_AUSENTE", "TOTAL_INVALIDO");
        if (total == null) {
            return Resultado.rechazado(motivoNumeroTemporal(nodo, "total", "TOTAL_AUSENTE", "TOTAL_INVALIDO"));
        }

        BigDecimal esperado = BigDecimal.valueOf(cantidad).multiply(BigDecimal.valueOf(precio));
        BigDecimal recibido = BigDecimal.valueOf(total);
        if (esperado.subtract(recibido).abs().compareTo(TOLERANCIA) > 0) {
            return Resultado.rechazado("TOTAL_NO_COINCIDE");
        }

        return Resultado.aceptado(new Venta(eventId, tiendaId, eventTime, cantidad, precio, total));
    }

    private String motivoNumeroTemporal(JsonNode nodo, String campo, String ausente, String invalido) {
        JsonNode valor = nodo.get(campo);
        return valor == null || valor.isNull() ? ausente : invalido;
    }

    private static String texto(JsonNode nodo, String campo) {
        JsonNode valor = nodo.get(campo);
        return valor == null || valor.isNull() || !valor.isTextual() ? null : valor.asText();
    }

    private static Double numero(JsonNode nodo, String campo, String ausente, String invalido) {
        JsonNode valor = nodo.get(campo);
        if (valor == null || valor.isNull() || !valor.isNumber()) {
            return null;
        }
        double resultado = valor.asDouble();
        return resultado > 0.0d && Double.isFinite(resultado) ? resultado : null;
    }

    /** Resultado de la validación: o un DTO válido o un motivo de rechazo. */
    public record Resultado(Venta venta, String motivo) {

        public static Resultado aceptado(Venta venta) {
            return new Resultado(venta, null);
        }

        public static Resultado rechazado(String motivo) {
            return new Resultado(null, motivo);
        }

        public boolean esValido() {
            return venta != null;
        }
    }
}
