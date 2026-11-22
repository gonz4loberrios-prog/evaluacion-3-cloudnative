package cl.dsy1107.ventas.model;

/** Estado inmutable de una ventana: se reemplaza en cada agregación (Kafka Streams state store). */
public record Agregado(long ventas, long unidades, double montoTotal) {

    public static Agregado vacio() {
        return new Agregado(0L, 0L, 0.0d);
    }

    public Agregado sumar(EventoProcesado evento) {
        return new Agregado(
                ventas + 1L,
                unidades + evento.cantidad(),
                redondear(montoTotal + evento.total()));
    }

    private static double redondear(double valor) {
        return Math.round(valor * 100.0d) / 100.0d;
    }
}
