package cl.dsy1107.ventas.config;

/** Configuración centralizada. Todos los valores pueden sobreescribirse por variables de entorno. */
public record AppConfig(
        String bootstrapServers,
        String applicationId,
        String rawTopic,
        String aggTopic,
        String dlqTopic,
        long windowMs,
        long graceMs,
        long dedupRetentionMs,
        int numStreamThreads,
        String stateDir,
        int metricsPort) {

    public static AppConfig fromEnv() {
        return new AppConfig(
                env("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093"),
                env("APPLICATION_ID", "ventas-streams-dsy1107"),
                env("TOPIC_RAW", "ventas-raw"),
                env("TOPIC_AGG", "ventas-agregadas"),
                env("TOPIC_DLQ", "ventas-raw-dlq"),
                envLong("WINDOW_MS", 60_000L),
                envLong("GRACE_MS", 30_000L),
                envLong("DEDUP_RETENTION_MS", 86_400_000L),
                (int) envLong("NUM_STREAM_THREADS", 2L),
                env("STATE_DIR", "/tmp/kafka-streams"),
                (int) envLong("METRICS_PORT", 8081L));
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static long envLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Variable " + key + " debe ser numérica: " + value, e);
        }
    }
}
