package io.astryon.processing;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record ProcessingConfig(
        List<String> brokers,
        String groupId,
        String checkpointUri,
        Duration checkpointInterval,
        Duration dedupWindow,
        String pipelineId,
        int pipelineVersion) implements Serializable {

    static final String RAW_TOPIC = "raw.events.v1";
    static final String VALIDATED_TOPIC = "validated.events.v1";
    static final String PROCESSED_TOPIC = "processed.events.v1";
    static final String QUARANTINE_TOPIC = "events.quarantine.v1";
    static final String DLQ_TOPIC = "events.dlq.v1";

    public ProcessingConfig {
        brokers = List.copyOf(brokers);
        if (brokers.isEmpty() || brokers.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("PROCESSING_KAFKA_BROKERS must contain host:port values");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("PROCESSING_KAFKA_GROUP_ID is required");
        }
        URI checkpoint = URI.create(checkpointUri);
        if (!"s3".equals(checkpoint.getScheme()) || checkpoint.getHost() == null || checkpoint.getPath().isBlank()) {
            throw new IllegalArgumentException("PROCESSING_CHECKPOINT_URI must be s3://<bucket>/<prefix>");
        }
        if (checkpointInterval.isNegative() || checkpointInterval.isZero()) {
            throw new IllegalArgumentException("PROCESSING_CHECKPOINT_INTERVAL must be positive");
        }
        if (dedupWindow.isNegative() || dedupWindow.isZero()) {
            throw new IllegalArgumentException("PROCESSING_DEDUP_WINDOW must be positive");
        }
        UUID.fromString(pipelineId);
        if (pipelineVersion < 1) {
            throw new IllegalArgumentException("PROCESSING_PIPELINE_VERSION must be >= 1");
        }
    }

    public static ProcessingConfig fromEnvironment() {
        return from(System.getenv());
    }

    static ProcessingConfig from(Map<String, String> env) {
        String rawBrokers = required(env, "PROCESSING_KAFKA_BROKERS");
        List<String> brokers = Arrays.stream(rawBrokers.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
        return new ProcessingConfig(
                brokers,
                env.getOrDefault("PROCESSING_KAFKA_GROUP_ID", "astryon-processing-v1"),
                required(env, "PROCESSING_CHECKPOINT_URI"),
                duration(env, "PROCESSING_CHECKPOINT_INTERVAL", "30s"),
                duration(env, "PROCESSING_DEDUP_WINDOW", "24h"),
                required(env, "PROCESSING_PIPELINE_ID"),
                integer(env, "PROCESSING_PIPELINE_VERSION", 1));
    }

    private static String required(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static Duration duration(Map<String, String> env, String name, String defaultValue) {
        String value = env.getOrDefault(name, defaultValue).trim().toLowerCase();
        try {
            if (value.endsWith("ms")) return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            if (value.endsWith("s")) return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            if (value.endsWith("m")) return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
            if (value.endsWith("h")) return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be a positive duration such as 30s or 24h", exception);
        }
        throw new IllegalArgumentException(name + " must use ms, s, m, or h");
    }

    private static int integer(Map<String, String> env, String name, int defaultValue) {
        try {
            return Integer.parseInt(env.getOrDefault(name, Integer.toString(defaultValue)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be an integer", exception);
        }
    }

    public String brokersCsv() {
        return String.join(",", brokers);
    }
}
