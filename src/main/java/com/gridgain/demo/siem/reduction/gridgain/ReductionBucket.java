package com.gridgain.demo.siem.reduction.gridgain;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public final class ReductionBucket implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String sourceTypeName;
    private final String reductionKey;
    private final int eventCount;
    private final long lastSeenEpochMillis;

    public ReductionBucket(String sourceTypeName, String reductionKey, int eventCount, long lastSeenEpochMillis) {
        this.sourceTypeName = Objects.requireNonNull(sourceTypeName, "sourceTypeName");
        this.reductionKey = Objects.requireNonNull(reductionKey, "reductionKey");
        if (eventCount <= 0) {
            throw new IllegalArgumentException("eventCount must be positive");
        }
        this.eventCount = eventCount;
        this.lastSeenEpochMillis = lastSeenEpochMillis;
    }

    public String sourceTypeName() {
        return sourceTypeName;
    }

    public String reductionKey() {
        return reductionKey;
    }

    public int eventCount() {
        return eventCount;
    }

    public Instant lastSeen() {
        return Instant.ofEpochMilli(lastSeenEpochMillis);
    }

    public int maxRemovableEvents() {
        return Math.max(0, eventCount - 1);
    }

    public ReductionBucket increment(long timestampEpochMillis) {
        long updatedLastSeen = Math.max(timestampEpochMillis, lastSeenEpochMillis);
        return new ReductionBucket(sourceTypeName, reductionKey, eventCount + 1, updatedLastSeen);
    }
}
