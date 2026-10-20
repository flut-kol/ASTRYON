package io.astryon.processing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Properties;

public final class ProcessingJob {
    private ProcessingJob() {}

    public static void main(String[] args) throws Exception {
        ProcessingConfig config = ProcessingConfig.fromEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpoints(env, config);

        KafkaSource<RawRecord> source = KafkaSource.<RawRecord>builder()
                .setBootstrapServers(config.brokersCsv())
                .setTopics(ProcessingConfig.RAW_TOPIC)
                .setGroupId(config.groupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RawRecordDeserializer())
                .build();

        DataStream<RawRecord> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "raw-events-v1");
        SingleOutputStreamOperator<AcceptedRecord> accepted = raw
                .process(new ValidateNormalizeFunction(config))
                .name("validate-normalize")
                .uid("validate-normalize-v1");

        accepted
                .map(AcceptedRecord::validated)
                .returns(TypeInformation.of(OutputRecord.class))
                .sinkTo(kafkaSink(config))
                .name("validated-events-v1")
                .uid("validated-events-v1");

        accepted
                .keyBy(AcceptedRecord::dedupKey)
                .process(new BoundedDeduplicateFunction(config.dedupWindow()))
                .name("bounded-dedup")
                .uid("bounded-dedup-v1")
                .map(AcceptedRecord::processed)
                .returns(TypeInformation.of(OutputRecord.class))
                .sinkTo(kafkaSink(config))
                .name("processed-events-v1")
                .uid("processed-events-v1");

        accepted.getSideOutput(ValidateNormalizeFunction.QUARANTINE)
                .sinkTo(kafkaSink(config))
                .name("quarantine-events-v1")
                .uid("quarantine-events-v1");

        env.execute("astryon-processing-v1");
    }

    private static void configureCheckpoints(StreamExecutionEnvironment env, ProcessingConfig config) {
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.enableCheckpointing(config.checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(config.checkpointUri()));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Math.min(10_000L, config.checkpointInterval().toMillis() / 2));
        env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(10).toMillis());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Duration.ofSeconds(1), Duration.ofMinutes(1), 2.0, Duration.ofSeconds(1), 0.1));
    }

    private static KafkaSink<OutputRecord> kafkaSink(ProcessingConfig config) {
        Properties properties = new Properties();
        properties.setProperty("acks", "all");
        properties.setProperty("enable.idempotence", "true");
        return KafkaSink.<OutputRecord>builder()
                .setBootstrapServers(config.brokersCsv())
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(new OutputRecordSerializer())
                // ADR-011: cross-system delivery is at-least-once; downstream is idempotent.
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
