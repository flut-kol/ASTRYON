package io.astryon.processing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

final class RawRecordDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) throws IOException {
        out.collect(new RawRecord(record.topic(), record.partition(), record.offset(), record.key(), record.value()));
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() {
        return TypeInformation.of(RawRecord.class);
    }
}

final class OutputRecordSerializer implements KafkaRecordSerializationSchema<OutputRecord> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(OutputRecord record, KafkaSinkContext context, Long timestamp) {
        return new ProducerRecord<>(record.topic(), record.key(), record.value());
    }
}
