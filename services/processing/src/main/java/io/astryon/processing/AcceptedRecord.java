package io.astryon.processing;

import java.io.Serializable;

public final class AcceptedRecord implements Serializable {
    public byte[] key;
    public String dedupKey;
    public byte[] validatedValue;
    public byte[] processedValue;

    public AcceptedRecord() {}

    AcceptedRecord(byte[] key, String dedupKey, byte[] validatedValue, byte[] processedValue) {
        this.key = key;
        this.dedupKey = dedupKey;
        this.validatedValue = validatedValue;
        this.processedValue = processedValue;
    }

    byte[] key() { return key; }
    String dedupKey() { return dedupKey; }
    byte[] validatedValue() { return validatedValue; }
    byte[] processedValue() { return processedValue; }

    OutputRecord validated() {
        return new OutputRecord(ProcessingConfig.VALIDATED_TOPIC, key, validatedValue);
    }

    OutputRecord processed() {
        return new OutputRecord(ProcessingConfig.PROCESSED_TOPIC, key, processedValue);
    }
}
