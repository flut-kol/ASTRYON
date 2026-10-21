package io.astryon.processing;

import java.io.Serializable;

record ProcessingResult(AcceptedRecord accepted, OutputRecord quarantined, String reasonCode) implements Serializable {
    static ProcessingResult accepted(AcceptedRecord record) {
        return new ProcessingResult(record, null, null);
    }

    static ProcessingResult quarantined(OutputRecord record, String reasonCode) {
        return new ProcessingResult(null, record, reasonCode);
    }

    boolean isAccepted() {
        return accepted != null;
    }
}
