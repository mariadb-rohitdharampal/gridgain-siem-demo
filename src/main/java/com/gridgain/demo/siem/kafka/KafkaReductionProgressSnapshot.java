package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.metrics.DemoMetrics;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record KafkaReductionProgressSnapshot(
        long recordsSeen,
        long validConsumedEvents,
        long producedCleanEvents,
        int malformedRecords,
        long securityEventsObserved,
        long securityEventsPreserved,
        int windowsProcessed,
        int openWindows,
        Duration elapsed,
        Map<String, Integer> consumedRawTopicCounts,
        Map<String, Integer> producedCleanTopicCounts,
        Map<String, String> proofMetrics
) {
    public KafkaReductionProgressSnapshot {
        consumedRawTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(consumedRawTopicCounts));
        producedCleanTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(producedCleanTopicCounts));
        proofMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(proofMetrics));
    }

    public KafkaReductionProgressSnapshot(
            long recordsSeen,
            long validConsumedEvents,
            long producedCleanEvents,
            int malformedRecords,
            long securityEventsObserved,
            long securityEventsPreserved,
            int windowsProcessed,
            int openWindows,
            Duration elapsed,
            Map<String, String> proofMetrics
    ) {
        this(
                recordsSeen,
                validConsumedEvents,
                producedCleanEvents,
                malformedRecords,
                securityEventsObserved,
                securityEventsPreserved,
                windowsProcessed,
                openWindows,
                elapsed,
                Map.of(),
                Map.of(),
                proofMetrics
        );
    }

    public double reductionPercentage() {
        return DemoMetrics.reductionPercentage(validConsumedEvents, producedCleanEvents);
    }

    public String primaryOwnerNodesObserved() {
        return proofMetrics.getOrDefault("primary owner nodes observed", "n/a");
    }
}
