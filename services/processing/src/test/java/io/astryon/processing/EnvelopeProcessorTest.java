package io.astryon.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EnvelopeProcessorTest {
    private static final Instant NOW = Instant.parse("2026-10-20T12:00:00Z");
    private static final byte[] KEY = "tenant-key".getBytes(StandardCharsets.UTF_8);
    private final ObjectMapper mapper = new ObjectMapper();
    private final ProcessingConfig config = new ProcessingConfig(
            List.of("kafka:9092"), "test", "s3://checkpoints/processing",
            java.time.Duration.ofSeconds(30), java.time.Duration.ofHours(24),
            "33333333-3333-4333-8333-333333333333", 7);
    private final EnvelopeProcessor processor = new EnvelopeProcessor(config, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void validatesAndNormalizesInlineJsonWithoutChangingIdentity() throws Exception {
        RawRecord raw = new RawRecord("raw.events.v1", 4, 91, KEY, goodEnvelope());

        ProcessingResult result = processor.process(raw);

        assertTrue(result.isAccepted());
        assertNull(result.quarantined());
        assertArrayEquals(KEY, result.accepted().key());
        assertEquals("11111111-1111-4111-8111-111111111111\u001f22222222-2222-4222-8222-222222222222\u001faaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\u001f1",
                result.accepted().dedupKey());

        JsonNode validated = mapper.readTree(result.accepted().validatedValue());
        assertFalse(validated.has("pipeline_id"));
        JsonNode processed = mapper.readTree(result.accepted().processedValue());
        assertEquals("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", processed.path("event_id").asText());
        assertEquals(7, processed.path("pipeline_version").asInt());
        assertEquals(config.pipelineId(), processed.path("pipeline_id").asText());
        assertEquals("orders.v1", processed.path("event_type").asText());
        assertEquals("kafka:raw.events.v1/4/91", processed.path("raw_ref").asText());
        assertTrue(processed.path("attributes").isObject());
        assertTrue(processed.path("quality_flags").isArray());
    }

    @Test
    void rejectsUnknownFieldsAndCarriesBoundedOriginalForReplay() throws Exception {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"processing_version\":1,\"admin\":true");
        RawRecord raw = new RawRecord("raw.events.v1", 0, 12, KEY, invalid.getBytes(StandardCharsets.UTF_8));

        ProcessingResult result = processor.process(raw);

        assertFalse(result.isAccepted());
        String expected = "{\"contract_version\":1,"
                + "\"deviation_id\":\"f2a52d23-a5fb-8a3e-bbda-5bf00abc11fb\","
                + "\"tenant_id\":\"11111111-1111-4111-8111-111111111111\","
                + "\"source_id\":\"22222222-2222-4222-8222-222222222222\","
                + "\"event_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\","
                + "\"processing_version\":1,"
                + "\"quarantined_at\":\"2026-10-20T12:00:00Z\","
                + "\"stage\":\"flink-processing\","
                + "\"reason_code\":\"ERR_ENVELOPE_MALFORMED\","
                + "\"diagnostic\":\"envelope validation failed\","
                + "\"provenance\":{\"source_topic\":\"raw.events.v1\",\"source_partition\":0,\"source_offset\":12},"
                + "\"original\":{\"encoding\":\"base64\",\"data\":\""
                + Base64.getEncoder().encodeToString(invalid.getBytes(StandardCharsets.UTF_8))
                + "\",\"truncated\":false}}";
        assertEquals(expected, new String(result.quarantined().value(), StandardCharsets.UTF_8));
        assertArrayEquals(
                "11111111-1111-4111-8111-111111111111\u001ff2a52d23-a5fb-8a3e-bbda-5bf00abc11fb".getBytes(StandardCharsets.UTF_8),
                result.quarantined().key());
    }

    @Test
    void deterministicDeviationIdMatchesGoOffsetFixture() throws Exception {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"processing_version\":1,\"unknown\":true");

        ProcessingResult first = processor.process(new RawRecord("raw.events.v1", 7, 91, KEY, invalid.getBytes(StandardCharsets.UTF_8)));
        ProcessingResult second = processor.process(new RawRecord("raw.events.v1", 7, 91, null, invalid.getBytes(StandardCharsets.UTF_8)));

        assertEquals("a0a11cfd-92f4-8cc7-b83b-065b5d9d48f0",
                mapper.readTree(first.quarantined().value()).path("deviation_id").asText());
        assertArrayEquals(first.quarantined().value(), second.quarantined().value());
        assertArrayEquals(first.quarantined().key(), second.quarantined().key());
    }

    @Test
    void boundsOriginalAt64KiBAndMarksTruncation() throws Exception {
        String padding = "x".repeat(70 << 10);
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"processing_version\":1,\"unknown\":\"" + padding + "\"");

        ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 2, 33, KEY, invalid.getBytes(StandardCharsets.UTF_8)));

        JsonNode original = mapper.readTree(result.quarantined().value()).path("original");
        assertEquals("base64", original.path("encoding").asText());
        assertEquals(64 << 10, Base64.getDecoder().decode(original.path("data").asText()).length);
        assertTrue(original.path("truncated").asBoolean());
    }

    @Test
    void diagnosticNeverCopiesPayloadOrUnknownFieldName() throws Exception {
        String secret = "sensitive-payload-marker";
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"processing_version\":1,\"" + secret + "\":true");

        ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 0, 9, KEY, invalid.getBytes(StandardCharsets.UTF_8)));

        JsonNode quarantine = mapper.readTree(result.quarantined().value());
        assertEquals("envelope validation failed", quarantine.path("diagnostic").asText());
        assertFalse(quarantine.path("diagnostic").asText().contains(secret));
        assertTrue(quarantine.path("diagnostic").asText().getBytes(StandardCharsets.UTF_8).length <= 2048);
    }

    @Test
    void omitsInvalidOptionalBusinessIdentifiers() throws Exception {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "not-an-event")
                .replace("22222222-2222-4222-8222-222222222222", "not-a-source");

        ProcessingResult result = processor.process(new RawRecord(
                "raw.events.v1", 3, 15, KEY, invalid.getBytes(StandardCharsets.UTF_8)));

        JsonNode quarantine = mapper.readTree(result.quarantined().value());
        assertFalse(quarantine.has("event_id"));
        assertFalse(quarantine.has("source_id"));
        assertEquals(1, quarantine.path("processing_version").asLong());
    }

    @Test
    void rejectsForeignFormatUntilObjectAndBinaryNormalizationExists() throws Exception {
        String objectRef = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"payload\":{\"amount\":42}", "\"object_ref\":\"s3://raw/events/1\",\"checksum\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")
                .replace("\"payload_format\":\"json\"", "\"payload_format\":\"parquet-reference\"");

        ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 1, 2, KEY, objectRef.getBytes(StandardCharsets.UTF_8)));

        assertFalse(result.isAccepted());
        JsonNode quarantine = mapper.readTree(result.quarantined().value());
        assertEquals("ERR_NORMALIZATION_UNSUPPORTED", quarantine.path("reason_code").asText());
    }

    @Test
    void sendsTenantlessInputToDlqWithoutInventingTenant() throws Exception {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("11111111-1111-4111-8111-111111111111", "tenant-secret-not-valid")
                .replace("\"payload\":{\"amount\":42}", "\"payload\":{\"amount\":42,\"secret\":\"tenant-secret-not-valid\"}");

        ProcessingResult result = processor.process(new RawRecord(
                "raw.events.v1", 0, 1, KEY, invalid.getBytes(StandardCharsets.UTF_8)));
        assertEquals(ProcessingConfig.DLQ_TOPIC, result.quarantined().topic());
        JsonNode dlq = mapper.readTree(result.quarantined().value());
        assertFalse(dlq.has("tenant_id"));
        assertFalse(dlq.has("original"));
        assertFalse(new String(result.quarantined().value(), StandardCharsets.UTF_8).contains("tenant-secret-not-valid"));
        assertEquals("ERR_TENANT_ID_INVALID", dlq.path("reason_code").asText());
        assertEquals("f83d5745-260c-8507-9b4f-f7710e051988", dlq.path("dead_letter_id").asText());
    }

    @Test
    void sendsMalformedAndMissingTenantInputsToDlq() throws Exception {
        ProcessingResult malformed = processor.process(new RawRecord(
                "raw.events.v1", 0, 1, KEY, "not-json".getBytes(StandardCharsets.UTF_8)));
        ProcessingResult missing = processor.process(new RawRecord(
                "raw.events.v1", 0, 2, KEY, "{\"source_id\":\"22222222-2222-4222-8222-222222222222\"}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(ProcessingConfig.DLQ_TOPIC, malformed.quarantined().topic());
        JsonNode malformedDlq = mapper.readTree(malformed.quarantined().value());
        assertEquals("ERR_ENVELOPE_MALFORMED", malformedDlq.path("reason_code").asText());
        assertFalse(malformedDlq.has("tenant_id"));
        assertFalse(malformedDlq.has("original"));
        assertEquals(ProcessingConfig.DLQ_TOPIC, missing.quarantined().topic());
        JsonNode missingDlq = mapper.readTree(missing.quarantined().value());
        assertEquals("ERR_EVENT_ID_INVALID", missingDlq.path("reason_code").asText());
        assertFalse(missingDlq.has("tenant_id"));
        assertFalse(missingDlq.has("original"));
    }

    @Test
    void failsClosedForInvalidKafkaProvenance() {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"processing_version\":1,\"unknown\":true");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> processor.process(new RawRecord("bad topic", 0, 1, KEY, invalid.getBytes(StandardCharsets.UTF_8))));
        assertEquals("invalid Kafka provenance", failure.getMessage());
    }

    @Test
    void rejectsInlineChecksumMismatchInsteadOfRewritingIt() throws Exception {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"checksum\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"processing_version\":1");

        ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 0, 3, KEY, invalid.getBytes(StandardCharsets.UTF_8)));

        assertFalse(result.isAccepted());
        assertEquals("ERR_CHECKSUM_MISMATCH", mapper.readTree(result.quarantined().value()).path("reason_code").asText());
    }

    @Test
    void preservesMatchingInlineChecksum() throws Exception {
        byte[] canonicalPayload = "{\"amount\":42}".getBytes(StandardCharsets.UTF_8);
        String checksum = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(canonicalPayload));
        String valid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"checksum\":\"" + checksum + "\",\"processing_version\":1");

        ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 0, 4, KEY, valid.getBytes(StandardCharsets.UTF_8)));

        assertTrue(result.isAccepted());
        assertEquals(checksum, mapper.readTree(result.accepted().processedValue()).path("checksum").asText());
    }

    @Test
    void replayIntoNewProcessingVersionHasDifferentDedupIdentity() {
        ProcessingResult first = processor.process(new RawRecord("raw.events.v1", 0, 5, KEY, goodEnvelope()));
        byte[] replay = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"processing_version\":1", "\"processing_version\":2")
                .getBytes(StandardCharsets.UTF_8);
        ProcessingResult second = processor.process(new RawRecord("raw.events.v1", 0, 6, KEY, replay));

        assertTrue(first.isAccepted());
        assertTrue(second.isAccepted());
        assertNotEquals(first.accepted().dedupKey(), second.accepted().dedupKey());
    }

    @Test
    void rejectsUnsafeObjectReferencesBeforeUnsupportedNormalization() throws Exception {
        for (String ref : List.of(
                "s3://bucket",
                "s3://bucket/key?version=1",
                "s3://user@bucket/key",
                "s3://bucket/a/..",
                "https://bucket/key")) {
            String objectRef = objectReferenceEnvelope(ref);
            ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 0, 5, KEY, objectRef.getBytes(StandardCharsets.UTF_8)));
            assertFalse(result.isAccepted(), ref);
            assertEquals("ERR_OBJECT_REF_INVALID", mapper.readTree(result.quarantined().value()).path("reason_code").asText(), ref);
        }
    }

    private static String objectReferenceEnvelope(String ref) {
        return new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("\"payload\":{\"amount\":42}", "\"object_ref\":\"" + ref + "\",\"checksum\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")
                .replace("\"payload_format\":\"json\"", "\"payload_format\":\"parquet-reference\"");
    }

    private static byte[] goodEnvelope() {
        return ("{" +
                "\"event_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\"," +
                "\"tenant_id\":\"11111111-1111-4111-8111-111111111111\"," +
                "\"source_id\":\"22222222-2222-4222-8222-222222222222\"," +
                "\"schema_id\":\"orders.v1\"," +
                "\"schema_version\":1," +
                "\"event_time\":\"2026-10-20T11:59:00Z\"," +
                "\"ingested_at\":\"2026-10-20T11:59:01Z\"," +
                "\"entity_key\":\"order-7\"," +
                "\"trace_id\":\"trace-1\"," +
                "\"payload_format\":\"json\"," +
                "\"payload\":{\"amount\":42}," +
                "\"processing_version\":1" +
                "}").getBytes(StandardCharsets.UTF_8);
    }
}
