package io.astryon.processing;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingMetricsTest {
    /** Records counters by "key=value/.../name" path; only the MetricGroup methods used by ProcessingMetrics. */
    private static MetricGroup group(String prefix, Map<String, Counter> sink) {
        return (MetricGroup) Proxy.newProxyInstance(MetricGroup.class.getClassLoader(), new Class<?>[] {MetricGroup.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "counter":
                            return sink.computeIfAbsent(prefix + args[0], k -> new SimpleCounter());
                        case "addGroup":
                            return args.length == 2
                                    ? group(prefix + args[0] + "=" + args[1] + "/", sink)
                                    : group(prefix + args[0] + "/", sink);
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    @Test
    void countsValidatedProcessedAndQuarantinedByReason() {
        Map<String, Counter> sink = new LinkedHashMap<>();
        ProcessingMetrics metrics = new ProcessingMetrics(group("", sink));

        metrics.validated();
        metrics.validated();
        metrics.processed();
        metrics.quarantined("ERR_SCHEMA_VERSION_INVALID");
        metrics.quarantined("ERR_SCHEMA_VERSION_INVALID");
        metrics.quarantined("ERR_PAYLOAD_TOO_LARGE");

        assertEquals(2, sink.get("records_validated_total").getCount());
        assertEquals(1, sink.get("records_processed_total").getCount());
        assertEquals(2, sink.get("reason=ERR_SCHEMA_VERSION_INVALID/records_quarantined_total").getCount());
        assertEquals(1, sink.get("reason=ERR_PAYLOAD_TOO_LARGE/records_quarantined_total").getCount());
    }

    @Test
    void unknownOrAttackerControlledReasonsCollapseToOther() {
        Map<String, Counter> sink = new LinkedHashMap<>();
        ProcessingMetrics metrics = new ProcessingMetrics(group("", sink));

        for (int i = 0; i < 1000; i++) metrics.quarantined("ERR_RANDOM_" + i);
        metrics.quarantined(null);

        assertEquals(1, sink.size());
        assertEquals(1001, sink.get("reason=OTHER/records_quarantined_total").getCount());
    }

    @Test
    void everyReasonEmittedByEnvelopeProcessorIsAllowListed() throws Exception {
        String source = java.nio.file.Files.readString(java.nio.file.Path.of("src/main/java/io/astryon/processing/EnvelopeProcessor.java"));
        var matcher = java.util.regex.Pattern.compile("\"(ERR_[A-Z_]+)[\":]").matcher(source);
        int found = 0;
        while (matcher.find()) {
            found++;
            assertTrue(ProcessingMetrics.REASONS.contains(matcher.group(1)), matcher.group(1));
        }
        assertTrue(found > 10);
    }

    @Test
    void processorSuppliesTheReasonCodeForQuarantinedResults() {
        ProcessingResult accepted = ProcessingResult.accepted(null);
        assertNull(accepted.reasonCode());
        ProcessingResult rejected = ProcessingResult.quarantined(new OutputRecord("t", new byte[0], new byte[0]), "ERR_TRACE_ID_INVALID");
        assertEquals("ERR_TRACE_ID_INVALID", rejected.reasonCode());
    }
}
