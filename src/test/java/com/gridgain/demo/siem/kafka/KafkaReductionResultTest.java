package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.streaming.WindowMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaReductionResultTest {
    @Test
    void reductionPercentageUsesValidConsumedEventsNotMalformedRecords() {
        KafkaReductionResult result = new KafkaReductionResult(
                Map.of("raw-dns", 3),
                Map.of("clean-dns", 1),
                2,
                1,
                0,
                0,
                Duration.ofMillis(5),
                "test",
                Map.of(),
                java.util.List.of()
        );

        assertEquals(3, result.totalKafkaRecordsSeen());
        assertEquals(2, result.consumedEvents());
        assertEquals(1, result.producedEvents());
        assertEquals(50.0, result.reductionPercentage());
        assertTrue(result.toReport().contains("Kafka records seen: 3"));
        assertTrue(result.toReport().contains("Valid consumed events: 2"));
        assertTrue(result.toReport().contains("Malformed records: 1"));
    }

    @Test
    void conciseReportIncludesTopFiveWindowsOnly() {
        KafkaReductionResult result = resultWithSixWindows();

        String report = result.toReport();

        assertTrue(report.contains("Top 5 windows by raw volume"));
        assertTrue(report.contains("Windows processed: 6"));
        assertTrue(report.contains("- window 1 ["));
        assertTrue(report.contains("- window 5 ["));
        assertFalse(report.contains("- window 6 ["));
        assertFalse(report.contains("All Window Details"));
    }

    @Test
    void verboseReportIncludesIndividualWindowLines() {
        KafkaReductionResult result = resultWithSixWindows();

        String report = result.toReport(true);

        assertTrue(report.contains("Top 5 windows by raw volume"));
        assertTrue(report.contains("All Window Details"));
        assertTrue(report.contains("- window 6 ["));
    }

    @Test
    void topWindowsByRawVolumeOrdersLargestWindowsFirst() {
        KafkaReductionResult result = resultWithSixWindows();

        List<WindowMetrics> topWindows = result.topWindowsByRawVolume(3);

        assertEquals(List.of(1, 2, 3), topWindows.stream().map(WindowMetrics::sequence).toList());
    }

    @Test
    void reportIncludesOwnershipProofMetricsWithoutRawFingerprintKeys() {
        KafkaReductionResult result = new KafkaReductionResult(
                Map.of("raw-dns", 10),
                Map.of("clean-dns", 6),
                10,
                0,
                1,
                1,
                Duration.ofMillis(20),
                "gridgain / embedded Apache Ignite",
                Map.of(
                        "primary owner nodes observed", "3 / 3",
                        "backup owner nodes observed", "3 / 3",
                        "primary reduction state ownership by node", "node-1=8, node-2=7, node-3=7",
                        "backup reduction state ownership by node", "node-1=7, node-2=8, node-3=7",
                        "partitions touched", "22"
                ),
                List.of()
        );

        String report = result.toReport();

        assertTrue(report.contains("- primary owner nodes observed: 3 / 3"));
        assertTrue(report.contains("- backup owner nodes observed: 3 / 3"));
        assertTrue(report.contains("- primary reduction state ownership by node: node-1=8, node-2=7, node-3=7"));
        assertTrue(report.contains("- partitions touched: 22"));
        assertFalse(report.contains("distributed-fingerprint"));
    }

    private static KafkaReductionResult resultWithSixWindows() {
        Instant start = Instant.parse("2026-06-22T13:00:00Z");
        return new KafkaReductionResult(
                Map.of("raw-dns", 601),
                Map.of("clean-dns", 301),
                601,
                0,
                12,
                12,
                Duration.ofMillis(25),
                "gridgain / embedded Apache Ignite",
                Map.of("embedded node count", "3"),
                List.of(
                        window(1, start, 100),
                        window(2, start.plusSeconds(60), 90),
                        window(3, start.plusSeconds(120), 80),
                        window(4, start.plusSeconds(180), 70),
                        window(5, start.plusSeconds(240), 60),
                        window(6, start.plusSeconds(300), 1)
                )
        );
    }

    private static WindowMetrics window(int sequence, Instant start, long rawEvents) {
        return new WindowMetrics(
                sequence,
                start,
                start.plusSeconds(60),
                rawEvents,
                rawEvents / 2,
                50.0,
                2,
                2
        );
    }
}
