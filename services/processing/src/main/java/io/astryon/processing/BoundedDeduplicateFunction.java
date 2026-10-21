package io.astryon.processing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed processing-time dedup window. A duplicate does not extend the window,
 * so every key is bounded by its first-seen time even under a duplicate storm.
 */
final class BoundedDeduplicateFunction extends KeyedProcessFunction<String, AcceptedRecord, AcceptedRecord> {
    private final long windowMillis;
    private transient ValueState<Long> expiresAt;
    private transient Counter unique;
    private transient Counter duplicates;
    private transient Counter expirations;
    private transient ProcessingMetrics metrics;
    /** Keys currently tracked by this subtask. Not restored from a checkpoint: it restarts at 0 and is clamped at 0. */
    private transient AtomicLong activeKeys;

    BoundedDeduplicateFunction(Duration window) {
        this.windowMillis = window.toMillis();
        if (windowMillis <= 0) throw new IllegalArgumentException("dedup window must be positive");
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("dedup-expires-at", Long.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMillis(windowMillis))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        expiresAt = getRuntimeContext().getState(descriptor);
        unique = getRuntimeContext().getMetricGroup().counter("dedup_unique_total");
        duplicates = getRuntimeContext().getMetricGroup().counter("dedup_dropped_total");
        expirations = getRuntimeContext().getMetricGroup().counter("dedup_expirations_total");
        activeKeys = new AtomicLong();
        getRuntimeContext().getMetricGroup().gauge("dedup_active_keys", (Gauge<Long>) activeKeys::get);
        metrics = new ProcessingMetrics(getRuntimeContext().getMetricGroup());
    }

    @Override
    public void processElement(AcceptedRecord value, Context context, Collector<AcceptedRecord> out) throws Exception {
        long now = context.timerService().currentProcessingTime();
        Long expiry = expiresAt.value();
        if (expiry != null && expiry > now) {
            duplicates.inc();
            return;
        }
        if (expiry != null) context.timerService().deleteProcessingTimeTimer(expiry);
        long newExpiry = Math.addExact(now, windowMillis);
        expiresAt.update(newExpiry);
        if (expiry == null) activeKeys.incrementAndGet();
        context.timerService().registerProcessingTimeTimer(newExpiry);
        unique.inc();
        metrics.processed();
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<AcceptedRecord> out) throws Exception {
        Long expiry = expiresAt.value();
        if (expiry != null && expiry <= timestamp) {
            expiresAt.clear();
            expirations.inc();
            activeKeys.updateAndGet(v -> Math.max(0, v - 1));
        }
    }
}
