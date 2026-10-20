package io.astryon.processing;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessingConfigTest {
    @Test
    void readsBoundedWindowAndS3CheckpointConfiguration() {
        Map<String, String> env = baseEnvironment();
        env.put("PROCESSING_KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092");
        env.put("PROCESSING_CHECKPOINT_INTERVAL", "45s");
        env.put("PROCESSING_DEDUP_WINDOW", "6h");
        env.put("PROCESSING_PIPELINE_VERSION", "9");

        ProcessingConfig config = ProcessingConfig.from(env);

        assertEquals(2, config.brokers().size());
        assertEquals(Duration.ofSeconds(45), config.checkpointInterval());
        assertEquals(Duration.ofHours(6), config.dedupWindow());
        assertEquals(9, config.pipelineVersion());
    }

    @Test
    void refusesNonDurableCheckpointLocation() {
        Map<String, String> env = baseEnvironment();
        env.put("PROCESSING_CHECKPOINT_URI", "file:///tmp/checkpoints");

        assertThrows(IllegalArgumentException.class, () -> ProcessingConfig.from(env));
    }

    @Test
    void refusesUnboundedOrZeroDedupWindow() {
        Map<String, String> env = baseEnvironment();
        env.put("PROCESSING_DEDUP_WINDOW", "0h");

        assertThrows(IllegalArgumentException.class, () -> ProcessingConfig.from(env));
    }

    private static Map<String, String> baseEnvironment() {
        Map<String, String> env = new HashMap<>();
        env.put("PROCESSING_KAFKA_BROKERS", "kafka:9092");
        env.put("PROCESSING_CHECKPOINT_URI", "s3://checkpoints/processing");
        env.put("PROCESSING_PIPELINE_ID", "33333333-3333-4333-8333-333333333333");
        return env;
    }
}
