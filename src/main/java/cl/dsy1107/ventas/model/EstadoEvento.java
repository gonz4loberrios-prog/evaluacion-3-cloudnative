package cl.dsy1107.ventas.model;

/** Clasificación de cada evento. TARDIO y DUPLICADO NO son errores de sintaxis. */
public enum EstadoEvento {
    VALIDO,
    INVALIDO,
    TARDIO,
    DUPLICADO
}
