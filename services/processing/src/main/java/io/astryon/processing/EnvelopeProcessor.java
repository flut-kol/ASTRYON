package io.astryon.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

final class EnvelopeProcessor {
    private static final int MAX_INLINE_PAYLOAD_BYTES = 1 << 20;
    private static final int MAX_QUARANTINE_BYTES = 64 << 10;
    private static final byte[] QUARANTINE_ID_DOMAIN = "astryon/quarantine-record/v1\0".getBytes(StandardCharsets.UTF_8);
    private static final String QUARANTINE_PRODUCER = "flink-processing";
    private static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(5);
    private static final Duration MAX_EVENT_AGE = Duration.ofDays(3660);
    private static final Pattern UUID_PATTERN = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private static final Pattern KAFKA_TOPIC_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final Pattern ULID_PATTERN = Pattern.compile("^[0-7][0-9A-HJKMNP-TV-Z]{25}$", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHECKSUM_PATTERN = Pattern.compile("^[0-9a-f]{64}$");
    private static final Set<String> ALLOWED_FIELDS = Set.of(
            "event_id", "tenant_id", "source_id", "schema_id", "schema_version", "event_time",
            "event_time_missing", "ingested_at", "entity_key", "trace_id", "payload_format", "payload",
            "object_ref", "checksum", "processing_version");

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final ProcessingConfig config;
    private final Clock clock;

    EnvelopeProcessor(ProcessingConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
    }

    ProcessingResult process(RawRecord raw) {
        ObjectNode envelope;
        try {
            JsonNode parsed = mapper.readTree(raw.value());
            if (parsed == null || !parsed.isObject()) {
                return reject(raw, "ERR_ENVELOPE_MALFORMED", null);
            }
            envelope = (ObjectNode) parsed;
        } catch (Exception exception) {
            return reject(raw, "ERR_ENVELOPE_MALFORMED", null);
        }

        String validationError = validate(envelope);
        if (validationError != null) {
            int separator = validationError.indexOf(':');
            return reject(raw, validationError.substring(0, separator), text(envelope, "tenant_id"));
        }
        if (!"json".equals(text(envelope, "payload_format")) || !envelope.has("payload")) {
            return reject(raw, "ERR_NORMALIZATION_UNSUPPORTED", text(envelope, "tenant_id"));
        }

        try {
            ObjectNode processed = envelope.deepCopy();
            processed.put("pipeline_id", config.pipelineId());
            processed.put("pipeline_version", config.pipelineVersion());
            processed.put("event_type", text(processed, "schema_id"));
            processed.set("attributes", mapper.createObjectNode());
            processed.set("quality_flags", mapper.createArrayNode());
            processed.put("raw_ref", "kafka:" + raw.topic() + "/" + raw.partition() + "/" + raw.offset());

            byte[] validated = mapper.writeValueAsBytes(envelope);
            byte[] processedBytes = mapper.writeValueAsBytes(processed);
            byte[] key = raw.key() == null ? partitionKey(envelope) : raw.key();
            String dedupKey = text(envelope, "tenant_id") + '\u001f'
                    + text(envelope, "source_id") + '\u001f' + text(envelope, "event_id") + '\u001f'
                    + envelope.path("processing_version").intValue();
            return ProcessingResult.accepted(new AcceptedRecord(key, dedupKey, validated, processedBytes));
        } catch (Exception exception) {
            return reject(raw, "ERR_NORMALIZATION_INTERNAL", text(envelope, "tenant_id"));
        }
    }

    private String validate(ObjectNode envelope) {
        Iterator<String> names = envelope.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            if (!ALLOWED_FIELDS.contains(name)) return "ERR_ENVELOPE_MALFORMED:unknown field " + name;
        }
        String eventId = text(envelope, "event_id");
        if (!UUID_PATTERN.matcher(eventId).matches() && !ULID_PATTERN.matcher(eventId).matches()) return "ERR_EVENT_ID_INVALID:event_id must be UUID or ULID";
        if (!uuid(text(envelope, "tenant_id"))) return "ERR_TENANT_ID_INVALID:tenant_id must be UUID";
        if (!uuid(text(envelope, "source_id"))) return "ERR_SOURCE_ID_INVALID:source_id must be UUID";
        if (!boundedText(text(envelope, "schema_id"), 256)) return "ERR_SCHEMA_ID_INVALID:schema_id is required and limited to 256 bytes";
        if (!integerAtLeast(envelope.get("schema_version"), 1)) return "ERR_SCHEMA_VERSION_INVALID:schema_version must be >= 1";
        if (!boundedText(text(envelope, "trace_id"), 128)) return "ERR_TRACE_ID_INVALID:trace_id is required and limited to 128 bytes";
        if (envelope.has("entity_key") && (!envelope.get("entity_key").isTextual() || !boundedTextAllowEmpty(text(envelope, "entity_key"), 256))) return "ERR_ENTITY_KEY_INVALID:entity_key must be text limited to 256 bytes";
        if (!integerAtLeast(envelope.get("processing_version"), 1)) return "ERR_PROCESSING_VERSION_INVALID:processing_version must be >= 1";

        String format = text(envelope, "payload_format");
        if (!Set.of("json", "protobuf", "avro", "parquet-reference").contains(format)) return "ERR_PAYLOAD_FORMAT_INVALID:unsupported payload_format";
        boolean hasPayload = envelope.has("payload") && !envelope.get("payload").isNull();
        if (envelope.has("object_ref") && !envelope.get("object_ref").isTextual()) return "ERR_OBJECT_REF_INVALID:object_ref must be text";
        boolean hasRef = !text(envelope, "object_ref").isEmpty();
        if (hasPayload == hasRef) return "ERR_PAYLOAD_XOR_OBJECT_REF:exactly one of payload and object_ref is required";
        if ("parquet-reference".equals(format) && !hasRef) return "ERR_PAYLOAD_XOR_OBJECT_REF:parquet-reference requires object_ref";
        if (hasPayload) {
            try {
                if (mapper.writeValueAsBytes(envelope.get("payload")).length > MAX_INLINE_PAYLOAD_BYTES) return "ERR_PAYLOAD_TOO_LARGE:inline payload exceeds 1 MiB";
            } catch (JsonProcessingException exception) {
                return "ERR_PAYLOAD_MALFORMED:payload cannot be encoded";
            }
        }
        if (hasRef && !validObjectRef(text(envelope, "object_ref"))) return "ERR_OBJECT_REF_INVALID:object_ref must be s3://<bucket>/<key> without traversal, userinfo, query, or fragment";
        if (hasRef && text(envelope, "checksum").isEmpty()) return "ERR_CHECKSUM_REQUIRED:checksum is required with object_ref";
        if (envelope.has("checksum") && (!envelope.get("checksum").isTextual() || !CHECKSUM_PATTERN.matcher(text(envelope, "checksum")).matches())) return "ERR_CHECKSUM_INVALID:checksum must be lowercase SHA-256 hex";
        if (hasPayload && envelope.has("checksum")) {
            try {
                byte[] canonicalPayload = mapper.writeValueAsBytes(envelope.get("payload"));
                String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonicalPayload));
                if (!actual.equals(text(envelope, "checksum"))) return "ERR_CHECKSUM_MISMATCH:checksum does not match canonical payload bytes";
            } catch (Exception exception) {
                return "ERR_PAYLOAD_MALFORMED:payload checksum could not be calculated";
            }
        }

        if (envelope.has("event_time_missing") && !envelope.get("event_time_missing").isBoolean()) return "ERR_ENVELOPE_MALFORMED:event_time_missing must be boolean";
        boolean missingMarker = envelope.path("event_time_missing").asBoolean(false);
        boolean hasEventTime = envelope.hasNonNull("event_time");
        if (hasEventTime == missingMarker) return "ERR_EVENT_TIME_MISSING:event_time must be present unless event_time_missing=true";
        if (hasEventTime) {
            String issue = timestampIssue(text(envelope, "event_time"), true);
            if (issue != null) return issue;
        }
        String ingestedIssue = timestampIssue(text(envelope, "ingested_at"), false);
        if (ingestedIssue != null) return ingestedIssue;
        return null;
    }

    private String timestampIssue(String value, boolean eventTime) {
        try {
            OffsetDateTime parsed = OffsetDateTime.parse(value);
            if (parsed.getOffset().getTotalSeconds() != 0) {
                return eventTime ? "ERR_EVENT_TIME_NOT_UTC:event_time must be UTC" : "ERR_INGESTED_AT_INVALID:ingested_at must be UTC";
            }
            Instant instant = parsed.toInstant();
            Instant now = clock.instant();
            if (instant.isAfter(now.plus(MAX_FUTURE_SKEW))) {
                return eventTime ? "ERR_EVENT_TIME_FUTURE:event_time is too far in the future" : "ERR_INGESTED_AT_INVALID:ingested_at is too far in the future";
            }
            if (eventTime && instant.isBefore(now.minus(MAX_EVENT_AGE))) return "ERR_EVENT_TIME_TOO_OLD:event_time is implausibly old";
            return null;
        } catch (DateTimeParseException exception) {
            return eventTime ? "ERR_ENVELOPE_MALFORMED:event_time must be RFC3339" : "ERR_INGESTED_AT_INVALID:ingested_at must be RFC3339";
        }
    }

    private ProcessingResult reject(RawRecord raw, String code, String tenantId) {
        if (!uuid(tenantId)) {
            throw new IllegalStateException("cannot quarantine without a validated tenant_id");
        }
        String normalizedTenantId = tenantId.toLowerCase(Locale.ROOT);
        String deviationId = quarantineDeviationId(raw);

        ObjectNode quarantine = mapper.createObjectNode();
        quarantine.put("contract_version", 1);
        quarantine.put("deviation_id", deviationId);
        quarantine.put("tenant_id", normalizedTenantId);
        ObjectNode parsed = parsedObject(raw.value());
        if (parsed != null) {
            String sourceId = text(parsed, "source_id");
            if (uuid(sourceId)) quarantine.put("source_id", sourceId.toLowerCase(Locale.ROOT));
            String eventId = text(parsed, "event_id");
            if (uuid(eventId)) quarantine.put("event_id", eventId.toLowerCase(Locale.ROOT));
            JsonNode processingVersion = parsed.get("processing_version");
            if (validQuarantineProcessingVersion(processingVersion)) {
                quarantine.put("processing_version", processingVersion.longValue());
            }
        }
        quarantine.put("quarantined_at", clock.instant().toString());
        quarantine.put("stage", QUARANTINE_PRODUCER);
        quarantine.put("reason_code", code);
        quarantine.put("diagnostic", diagnostic(code));

        ObjectNode provenance = quarantine.putObject("provenance");
        provenance.put("source_topic", raw.topic());
        provenance.put("source_partition", raw.partition());
        provenance.put("source_offset", raw.offset());

        byte[] original = raw.value() == null ? new byte[0] : raw.value();
        boolean truncated = original.length > MAX_QUARANTINE_BYTES;
        int length = Math.min(original.length, MAX_QUARANTINE_BYTES);
        ObjectNode originalNode = quarantine.putObject("original");
        originalNode.put("encoding", "base64");
        originalNode.put("data", Base64.getEncoder().encodeToString(java.util.Arrays.copyOf(original, length)));
        originalNode.put("truncated", truncated);
        try {
            byte[] key = (normalizedTenantId + '\u001f' + deviationId).getBytes(StandardCharsets.UTF_8);
            return ProcessingResult.quarantined(new OutputRecord(ProcessingConfig.QUARANTINE_TOPIC, key, mapper.writeValueAsBytes(quarantine)), code);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("cannot serialize quarantine record", exception);
        }
    }

    private ObjectNode parsedObject(byte[] value) {
        if (value == null) return null;
        try {
            JsonNode parsed = mapper.readTree(value);
            return parsed != null && parsed.isObject() ? (ObjectNode) parsed : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String diagnostic(String code) {
        return switch (code) {
            case "ERR_NORMALIZATION_UNSUPPORTED" -> "normalization is not supported";
            case "ERR_NORMALIZATION_INTERNAL" -> "normalization failed";
            default -> "envelope validation failed";
        };
    }

    private static String quarantineDeviationId(RawRecord raw) {
        if (raw.topic() == null
                || raw.topic().getBytes(StandardCharsets.UTF_8).length > 249
                || !KAFKA_TOPIC_PATTERN.matcher(raw.topic()).matches()
                || raw.partition() < 0
                || raw.offset() < 0) {
            throw new IllegalArgumentException("invalid Kafka provenance");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(QUARANTINE_ID_DOMAIN);
            updateIdentityPart(digest, QUARANTINE_PRODUCER.getBytes(StandardCharsets.UTF_8));
            updateIdentityPart(digest, raw.topic().getBytes(StandardCharsets.UTF_8));
            ByteBuffer coordinate = ByteBuffer.allocate(12);
            coordinate.putInt(raw.partition());
            coordinate.putLong(raw.offset());
            updateIdentityPart(digest, coordinate.array());
            byte[] bytes = java.util.Arrays.copyOf(digest.digest(), 16);
            bytes[6] = (byte) ((bytes[6] & 0x0f) | 0x80);
            bytes[8] = (byte) ((bytes[8] & 0x3f) | 0x80);
            ByteBuffer uuid = ByteBuffer.wrap(bytes);
            return new UUID(uuid.getLong(), uuid.getLong()).toString();
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static void updateIdentityPart(MessageDigest digest, byte[] part) {
        digest.update(ByteBuffer.allocate(4).putInt(part.length).array());
        digest.update(part);
    }

    private static byte[] partitionKey(ObjectNode envelope) {
        String entity = text(envelope, "entity_key");
        if (entity.isEmpty()) entity = text(envelope, "event_id");
        return (text(envelope, "tenant_id") + '\u001f' + entity).getBytes(StandardCharsets.UTF_8);
    }

    private static boolean validObjectRef(String value) {
        if (value.getBytes(StandardCharsets.UTF_8).length > 2048) return false;
        try {
            URI uri = new URI(value);
            String path = uri.getPath();
            return "s3".equals(uri.getScheme())
                    && uri.getHost() != null && !uri.getHost().isEmpty()
                    && path != null && path.length() > 1
                    && uri.getUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && !path.contains("/../")
                    && !path.endsWith("/..");
        } catch (URISyntaxException exception) {
            return false;
        }
    }

    private static String text(ObjectNode object, String field) {
        JsonNode node = object.get(field);
        return node != null && node.isTextual() ? node.textValue() : "";
    }

    private static boolean uuid(String value) {
        if (value == null || !UUID_PATTERN.matcher(value).matches()) return false;
        try { UUID.fromString(value); return true; } catch (IllegalArgumentException exception) { return false; }
    }

    private static boolean integerAtLeast(JsonNode node, int minimum) {
        return node != null && node.isIntegralNumber() && node.canConvertToInt() && node.intValue() >= minimum;
    }

    private static boolean validQuarantineProcessingVersion(JsonNode node) {
        return node != null
                && node.isIntegralNumber()
                && node.canConvertToLong()
                && node.longValue() >= 1
                && node.longValue() <= 0xffff_ffffL;
    }

    private static boolean boundedText(String value, int maxBytes) {
        return !value.isEmpty() && boundedTextAllowEmpty(value, maxBytes);
    }

    private static boolean boundedTextAllowEmpty(String value, int maxBytes) {
        return value.getBytes(StandardCharsets.UTF_8).length <= maxBytes && value.chars().noneMatch(c -> c < 0x20 || c == 0x7f);
    }
}
