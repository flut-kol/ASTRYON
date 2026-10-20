package io.astryon.processing;

import java.io.Serializable;

record RawRecord(String topic, int partition, long offset, byte[] key, byte[] value) implements Serializable {}

record OutputRecord(String topic, byte[] key, byte[] value) implements Serializable {}

record AcceptedRecord(
        byte[] key,
        String dedupKey,
        byte[] validatedValue,
        byte[] processedValue) implements Serializable {

    OutputRecord validated() {
        return new OutputRecord(ProcessingConfig.VALIDATED_TOPIC, key, validatedValue);
    }

    OutputRecord processed() {
        return new OutputRecord(ProcessingConfig.PROCESSED_TOPIC, key, processedValue);
    }
}

record ProcessingResult(AcceptedRecord accepted, OutputRecord quarantined) implements Serializable {
    static ProcessingResult accepted(AcceptedRecord record) {
        return new ProcessingResult(record, null);
    }

    static ProcessingResult quarantined(OutputRecord record) {
        return new ProcessingResult(null, record);
    }

    boolean isAccepted() {
        return accepted != null;
    }
}
