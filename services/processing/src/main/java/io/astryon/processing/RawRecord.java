package io.astryon.processing;

import java.io.Serializable;

/*
 * Types that cross Flink operator boundaries must be POJOs (public no-arg constructor, public non-final fields):
 * Flink 1.20 does not analyse Java records, falls back to Kryo's FieldSerializer and fails on the first element with
 * "can't get field offset on a record class". RecordTypesTest fails if any of these becomes a generic type again.
 * The accessor-style methods keep the former record call sites unchanged.
 */
public final class RawRecord implements Serializable {
    public String topic;
    public int partition;
    public long offset;
    public byte[] key;
    public byte[] value;

    public RawRecord() {}

    RawRecord(String topic, int partition, long offset, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    String topic() { return topic; }
    int partition() { return partition; }
    long offset() { return offset; }
    byte[] key() { return key; }
    byte[] value() { return value; }
}
