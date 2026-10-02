package com.gridgain.demo.siem.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record LogEvent(
        String id,
        LogSourceType sourceType,
        Instant timestamp,
        String eventType,
        String rawPayload,
        boolean securityRelevant,
        String reductionKey,
        int representedEventCount
) {
    public LogEvent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(sourceType, "sourceType");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(rawPayload, "rawPayload");
        Objects.requireNonNull(reductionKey, "reductionKey");
        if (representedEventCount <= 0) {
            throw new IllegalArgumentException("representedEventCount must be positive");
        }
    }

    public static LogEvent observed(
            LogSourceType sourceType,
            Instant timestamp,
            String eventType,
            String rawPayload,
            boolean securityRelevant,
            String reductionKey
    ) {
        return new LogEvent(
                UUID.randomUUID().toString(),
                sourceType,
                timestamp,
                eventType,
                rawPayload,
                securityRelevant,
                reductionKey,
                1
        );
    }

    public static LogEvent summary(
            LogSourceType sourceType,
            Instant timestamp,
            String reductionKey,
            int representedEventCount
    ) {
        return summary(sourceType, timestamp, timestamp, reductionKey, representedEventCount);
    }

    public static LogEvent summary(
            LogSourceType sourceType,
            Instant firstSeen,
            Instant lastSeen,
            String reductionKey,
            int representedEventCount
    ) {
        Objects.requireNonNull(firstSeen, "firstSeen");
        Objects.requireNonNull(lastSeen, "lastSeen");
        if (lastSeen.isBefore(firstSeen)) {
            throw new IllegalArgumentException("lastSeen must not be before firstSeen");
        }

        String payload = "REDUCTION_SUMMARY source=\"%s\" reductionKey=\"%s\" firstSeen=\"%s\" lastSeen=\"%s\" representedEvents=%d count=%d"
                .formatted(sourceType.displayName(), reductionKey, firstSeen, lastSeen, representedEventCount, representedEventCount);
        return new LogEvent(
                UUID.randomUUID().toString(),
                sourceType,
                lastSeen,
                "REDUCTION_SUMMARY",
                payload,
                false,
                reductionKey,
                representedEventCount
        );
    }
}
