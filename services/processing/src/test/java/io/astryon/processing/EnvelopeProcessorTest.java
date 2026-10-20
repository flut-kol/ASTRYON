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
        JsonNode quarantine = mapper.readTree(result.quarantined().value());
        assertEquals("ERR_ENVELOPE_MALFORMED", quarantine.path("reason_code").asText());
        assertEquals("flink-processing", quarantine.path("stage").asText());
        assertEquals(invalid, new String(Base64.getDecoder().decode(quarantine.path("original_b64").asText()), StandardCharsets.UTF_8));
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
    void rejectsTenantThatIsNotAUuid() throws Exception {
        String invalid = new String(goodEnvelope(), StandardCharsets.UTF_8)
                .replace("11111111-1111-4111-8111-111111111111", "not-a-tenant");

        ProcessingResult result = processor.process(new RawRecord("raw.events.v1", 0, 1, KEY, invalid.getBytes(StandardCharsets.UTF_8)));

        assertFalse(result.isAccepted());
        assertEquals("ERR_TENANT_ID_INVALID", mapper.readTree(result.quarantined().value()).path("reason_code").asText());
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
