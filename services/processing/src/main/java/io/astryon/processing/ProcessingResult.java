package io.astryon.processing;

import java.io.Serializable;

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
