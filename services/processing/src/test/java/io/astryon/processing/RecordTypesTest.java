package io.astryon.processing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Guards against Flink falling back to Kryo for the types that cross operators (Java records did, and failed at runtime). */
class RecordTypesTest {
    private static final List<Class<?>> FLOWING = List.of(RawRecord.class, AcceptedRecord.class, OutputRecord.class);

    @Test
    void flowingTypesAreFlinkPojosNotGenericTypes() {
        for (Class<?> type : FLOWING) {
            TypeInformation<?> info = TypeInformation.of(type);
            assertFalse(info instanceof GenericTypeInfo, type.getSimpleName() + " would be serialised with Kryo");
            assertTrue(info instanceof PojoTypeInfo, type.getSimpleName() + " is not a POJO: " + info);
        }
    }

    @Test
    void roundTripWithGenericTypesDisabled() throws IOException {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes(); // any Kryo fallback now throws while the serializer is created

        RawRecord raw = new RawRecord("raw.events.v1", 3, 41L, new byte[] {1, 2}, new byte[] {9, 8, 7});
        RawRecord rawBack = roundTrip(RawRecord.class, config, raw);
        assertEquals("raw.events.v1", rawBack.topic());
        assertEquals(3, rawBack.partition());
        assertEquals(41L, rawBack.offset());
        assertArrayEquals(new byte[] {1, 2}, rawBack.key());
        assertArrayEquals(new byte[] {9, 8, 7}, rawBack.value());

        RawRecord nullKey = roundTrip(RawRecord.class, config, new RawRecord("t", 0, 0, null, new byte[0]));
        assertEquals(null, nullKey.key());

        AcceptedRecord accepted = new AcceptedRecord(new byte[] {5}, "dedup", new byte[] {6}, new byte[] {7});
        AcceptedRecord acceptedBack = roundTrip(AcceptedRecord.class, config, accepted);
        assertEquals("dedup", acceptedBack.dedupKey());
        assertArrayEquals(new byte[] {7}, acceptedBack.processedValue());
        assertEquals(ProcessingConfig.PROCESSED_TOPIC, acceptedBack.processed().topic());

        OutputRecord out = roundTrip(OutputRecord.class, config, new OutputRecord("q", null, new byte[] {1}));
        assertEquals("q", out.topic());
        assertArrayEquals(new byte[] {1}, out.value());
    }

    private static <T> T roundTrip(Class<T> type, ExecutionConfig config, T value) throws IOException {
        TypeSerializer<T> serializer = TypeInformation.of(type).createSerializer(config);
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }
}
