package io.astryon.processing;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Custom Flink counters. Names are exported by the Prometheus reporter as
 * {@code flink_taskmanager_job_task_operator_<name>}. The quarantine counter carries a single
 * {@code reason} label whose value comes from a fixed allow-list (anything else is {@code OTHER}),
 * so label cardinality is bounded; no tenant, source or event identifiers are ever used as labels.
 */
final class ProcessingMetrics {
    static final String VALIDATED = "records_validated_total";
    static final String PROCESSED = "records_processed_total";
    static final String QUARANTINED = "records_quarantined_total";
    static final String REASON_LABEL = "reason";
    static final String OTHER = "OTHER";

    static final Set<String> REASONS = Set.of(
            "ERR_CHECKSUM_INVALID", "ERR_CHECKSUM_MISMATCH", "ERR_CHECKSUM_REQUIRED", "ERR_ENTITY_KEY_INVALID",
            "ERR_ENVELOPE_MALFORMED", "ERR_EVENT_ID_INVALID", "ERR_EVENT_TIME_FUTURE", "ERR_EVENT_TIME_MISSING",
            "ERR_EVENT_TIME_NOT_UTC", "ERR_EVENT_TIME_TOO_OLD", "ERR_INGESTED_AT_INVALID", "ERR_NORMALIZATION_INTERNAL",
            "ERR_NORMALIZATION_UNSUPPORTED", "ERR_OBJECT_REF_INVALID", "ERR_PAYLOAD_FORMAT_INVALID",
            "ERR_PAYLOAD_MALFORMED", "ERR_PAYLOAD_TOO_LARGE", "ERR_PAYLOAD_XOR_OBJECT_REF",
            "ERR_PROCESSING_VERSION_INVALID", "ERR_SCHEMA_ID_INVALID", "ERR_SCHEMA_VERSION_INVALID",
            "ERR_SOURCE_ID_INVALID", "ERR_TENANT_ID_INVALID", "ERR_TRACE_ID_INVALID");

    private final MetricGroup group;
    private final Map<String, Counter> quarantined = new HashMap<>();
    private Counter validated;
    private Counter processed;

    ProcessingMetrics(MetricGroup group) {
        this.group = group;
    }

    static String boundedReason(String code) {
        return code != null && REASONS.contains(code) ? code : OTHER;
    }

    void validated() {
        if (validated == null) validated = group.counter(VALIDATED);
        validated.inc();
    }

    void processed() {
        if (processed == null) processed = group.counter(PROCESSED);
        processed.inc();
    }

    void quarantined(String reasonCode) {
        String reason = boundedReason(reasonCode);
        quarantined.computeIfAbsent(reason, r -> group.addGroup(REASON_LABEL, r).counter(QUARANTINED)).inc();
    }
}
