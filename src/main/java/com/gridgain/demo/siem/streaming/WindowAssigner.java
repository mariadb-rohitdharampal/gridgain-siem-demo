package com.gridgain.demo.siem.streaming;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class WindowAssigner {
    private final Duration windowSize;
    private final long windowSizeSeconds;

    public WindowAssigner(Duration windowSize) {
        this.windowSize = Objects.requireNonNull(windowSize, "windowSize");
        if (windowSize.isZero() || windowSize.isNegative()) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
        if (windowSize.getNano() != 0) {
            throw new IllegalArgumentException("windowSize must be expressed in whole seconds");
        }
        this.windowSizeSeconds = windowSize.toSeconds();
    }

    public Duration windowSize() {
        return windowSize;
    }

    public TimeWindow assign(Instant timestamp, int sequence) {
        Objects.requireNonNull(timestamp, "timestamp");
        long startEpochSecond = Math.floorDiv(timestamp.getEpochSecond(), windowSizeSeconds) * windowSizeSeconds;
        Instant start = Instant.ofEpochSecond(startEpochSecond);
        return new TimeWindow(sequence, start, start.plus(windowSize));
    }
}
