package com.gridgain.demo.siem.streaming;

import java.time.Instant;
import java.util.Objects;

public record TimeWindow(int sequence, Instant startInclusive, Instant endExclusive) {
    public TimeWindow {
        Objects.requireNonNull(startInclusive, "startInclusive");
        Objects.requireNonNull(endExclusive, "endExclusive");
        if (sequence <= 0) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        if (!endExclusive.isAfter(startInclusive)) {
            throw new IllegalArgumentException("endExclusive must be after startInclusive");
        }
    }

    public boolean contains(Instant timestamp) {
        Objects.requireNonNull(timestamp, "timestamp");
        return !timestamp.isBefore(startInclusive) && timestamp.isBefore(endExclusive);
    }

    public boolean sameRange(TimeWindow other) {
        return startInclusive.equals(other.startInclusive) && endExclusive.equals(other.endExclusive);
    }
}
