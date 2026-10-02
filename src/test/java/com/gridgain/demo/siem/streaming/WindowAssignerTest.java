package com.gridgain.demo.siem.streaming;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowAssignerTest {
    @Test
    void assignsTimestampToEpochAlignedWindow() {
        WindowAssigner assigner = new WindowAssigner(Duration.ofSeconds(60));

        TimeWindow window = assigner.assign(Instant.parse("2026-06-22T13:00:42Z"), 1);

        assertEquals(1, window.sequence());
        assertEquals(Instant.parse("2026-06-22T13:00:00Z"), window.startInclusive());
        assertEquals(Instant.parse("2026-06-22T13:01:00Z"), window.endExclusive());
        assertTrue(window.contains(Instant.parse("2026-06-22T13:00:59Z")));
    }

    @Test
    void assignsBoundaryTimestampToNextWindow() {
        WindowAssigner assigner = new WindowAssigner(Duration.ofSeconds(60));

        TimeWindow window = assigner.assign(Instant.parse("2026-06-22T13:01:00Z"), 2);

        assertEquals(2, window.sequence());
        assertEquals(Instant.parse("2026-06-22T13:01:00Z"), window.startInclusive());
        assertEquals(Instant.parse("2026-06-22T13:02:00Z"), window.endExclusive());
    }

    @Test
    void rejectsInvalidWindowSizes() {
        assertThrows(IllegalArgumentException.class, () -> new WindowAssigner(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new WindowAssigner(Duration.ofMillis(500)));
    }
}
