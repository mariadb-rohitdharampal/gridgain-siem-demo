package com.gridgain.demo.siem.streaming;

import com.gridgain.demo.siem.metrics.DemoMetrics;

import java.time.Instant;
import java.util.Objects;

public record WindowMetrics(
        int sequence,
        Instant startTime,
        Instant endTime,
        long rawEvents,
        long reducedEvents,
        double reductionPercentage,
        long securityEventsObserved,
        long securityEventsPreserved
) {
    public WindowMetrics {
        Objects.requireNonNull(startTime, "startTime");
        Objects.requireNonNull(endTime, "endTime");
        if (sequence <= 0) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        if (!endTime.isAfter(startTime)) {
            throw new IllegalArgumentException("endTime must be after startTime");
        }
        if (rawEvents < 0 || reducedEvents < 0 || securityEventsObserved < 0 || securityEventsPreserved < 0) {
            throw new IllegalArgumentException("event counts must not be negative");
        }
    }

    public static WindowMetrics from(
            TimeWindow window,
            long rawEvents,
            long reducedEvents,
            long securityEventsObserved,
            long securityEventsPreserved
    ) {
        return new WindowMetrics(
                window.sequence(),
                window.startInclusive(),
                window.endExclusive(),
                rawEvents,
                reducedEvents,
                DemoMetrics.reductionPercentage(rawEvents, reducedEvents),
                securityEventsObserved,
                securityEventsPreserved
        );
    }
}
