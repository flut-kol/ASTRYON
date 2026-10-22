package io.astryon.processing;

import java.io.Serializable;

public final class OutputRecord implements Serializable {
    public String topic;
    public byte[] key;
    public byte[] value;

    public OutputRecord() {}

    OutputRecord(String topic, byte[] key, byte[] value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    String topic() { return topic; }
    byte[] key() { return key; }
    byte[] value() { return value; }
}
