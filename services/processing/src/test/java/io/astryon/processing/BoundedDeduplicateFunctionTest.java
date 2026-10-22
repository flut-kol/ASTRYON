package io.astryon.processing;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BoundedDeduplicateFunctionTest {
    private static AcceptedRecord rec(String key) {
        return new AcceptedRecord(new byte[] {1}, key, new byte[] {2}, new byte[] {3});
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, AcceptedRecord, AcceptedRecord> harness(Duration window)
            throws Exception {
        var h = new KeyedOneInputStreamOperatorTestHarness<String, AcceptedRecord, AcceptedRecord>(
                new KeyedProcessOperator<>(new BoundedDeduplicateFunction(window)), AcceptedRecord::dedupKey, Types.STRING);
        h.open();
        return h;
    }

    @Test
    void duplicateInsideWindowIsDropped() throws Exception {
        try (var h = harness(Duration.ofSeconds(10))) {
            h.setProcessingTime(1_000);
            h.processElement(rec("a"), 0);
            h.processElement(rec("a"), 0);
            h.processElement(rec("b"), 0);
            assertEquals(2, h.extractOutputValues().size());
        }
    }

    @Test
    void duplicateDoesNotExtendWindow() throws Exception {
        try (var h = harness(Duration.ofSeconds(10))) {
            h.setProcessingTime(0);
            h.processElement(rec("a"), 0);
            h.setProcessingTime(9_000);
            h.processElement(rec("a"), 0); // dropped, must not push expiry to 19_000
            h.setProcessingTime(10_001);
            h.processElement(rec("a"), 0); // window over: accepted again
            assertEquals(2, h.extractOutputValues().size());
        }
    }

    @Test
    void expiredKeyIsClearedByTimer() throws Exception {
        try (var h = harness(Duration.ofSeconds(10))) {
            h.setProcessingTime(0);
            h.processElement(rec("a"), 0);
            h.processElement(rec("b"), 0);
            assertEquals(2, h.numKeyedStateEntries());
            h.setProcessingTime(10_000);
            assertEquals(0, h.numKeyedStateEntries());
            assertEquals(0, h.numProcessingTimeTimers());
        }
    }

    @Test
    void sameEventInOtherProcessingVersionIsNotADuplicate() throws Exception {
        try (var h = harness(Duration.ofSeconds(10))) {
            h.processElement(rec("t\u001fs\u001fe\u001f1"), 0);
            h.processElement(rec("t\u001fs\u001fe\u001f2"), 0);
            assertEquals(2, h.extractOutputValues().size());
        }
    }

    @Test
    void zeroWindowIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedDeduplicateFunction(Duration.ZERO));
    }

    @Test
    void stateSurvivesSnapshotRestore() throws Exception {
        var first = harness(Duration.ofSeconds(10));
        first.setProcessingTime(0);
        first.processElement(rec("a"), 0);
        var snapshot = first.snapshot(1, 0);
        first.close();
        var second = new KeyedOneInputStreamOperatorTestHarness<String, AcceptedRecord, AcceptedRecord>(
                new KeyedProcessOperator<>(new BoundedDeduplicateFunction(Duration.ofSeconds(10))),
                AcceptedRecord::dedupKey, Types.STRING);
        second.setup();
        second.initializeState(snapshot);
        second.open();
        second.setProcessingTime(5_000);
        second.processElement(rec("a"), 0);
        assertEquals(0, second.extractOutputValues().size());
        second.close();
    }
}
