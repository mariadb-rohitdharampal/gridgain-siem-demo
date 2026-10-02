package com.gridgain.demo.siem.metrics;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.streaming.WindowMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoMetricsTest {
    @Test
    void calculatesOverallAndPerSourceMetrics() {
        List<LogEvent> rawEvents = List.of(
                event(LogSourceType.FIREWALL, false),
                event(LogSourceType.FIREWALL, false),
                event(LogSourceType.DNS, true),
                event(LogSourceType.DNS, false)
        );
        List<LogEvent> reducedEvents = List.of(
                LogEvent.summary(LogSourceType.FIREWALL, Instant.parse("2026-06-22T13:00:00Z"), "firewall:allow", 2),
                event(LogSourceType.DNS, true),
                event(LogSourceType.DNS, false)
        );

        DemoMetrics metrics = DemoMetrics.from(rawEvents, reducedEvents, Duration.ofMillis(7));

        assertEquals(4, metrics.rawEvents());
        assertEquals(3, metrics.reducedEvents());
        assertEquals(25.0, metrics.reductionPercentage());
        assertEquals(1, metrics.securityEventsObserved());
        assertEquals(1, metrics.securityEventsPreserved());
        assertEquals(50.0, metrics.perSourceMetrics().get(LogSourceType.FIREWALL).reductionPercentage());
        assertEquals(0.0, metrics.perSourceMetrics().get(LogSourceType.DNS).reductionPercentage());
    }

    @Test
    void reportIncludesRequiredPhaseOneMetricLabels() {
        DemoMetrics metrics = DemoMetrics.from(List.of(), List.of(), Duration.ZERO);

        String report = metrics.toReport();

        assertTrue(report.contains("Raw events:"));
        assertTrue(report.contains("Reducer mode: inmemory"));
        assertTrue(report.contains("Reduced events:"));
        assertTrue(report.contains("Reduction:"));
        assertTrue(report.contains("Security events preserved:"));
        assertTrue(report.contains("Processing time:"));
        assertTrue(report.contains("Firewall: raw="));
        assertTrue(report.contains("DNS: raw="));
        assertTrue(report.contains("Windows AD: raw="));
        assertTrue(report.contains("Cloud / Zero Trust: raw="));
    }

    @Test
    void reportIncludesReducerModeAndProofMetricsWhenProvided() {
        DemoMetrics metrics = DemoMetrics.from(
                List.of(),
                List.of(),
                Duration.ZERO,
                "gridgain / embedded Apache Ignite",
                Map.of(
                        "cache name", "siem-reduction-state",
                        "reduction state bucket count", "3",
                        "embedded node count", "3",
                        "cache mode", "PARTITIONED",
                        "backup count", "1"
                )
        );

        String report = metrics.toReport();

        assertTrue(report.contains("Reducer mode: gridgain / embedded Apache Ignite"));
        assertTrue(report.contains("Ignite Proof Metrics"));
        assertTrue(report.contains("cache name: siem-reduction-state"));
        assertTrue(report.contains("reduction state bucket count: 3"));
        assertTrue(report.contains("embedded node count: 3"));
        assertTrue(report.contains("cache mode: PARTITIONED"));
        assertTrue(report.contains("backup count: 1"));
    }

    @Test
    void reportIncludesKafkaSimulationTopicMetricsWhenProvided() {
        DemoMetrics metrics = DemoMetrics.from(
                List.of(),
                List.of(),
                Duration.ZERO,
                "inmemory",
                Map.of(),
                List.of(new DemoMetrics.TopicMetrics("raw-firewall", "clean-firewall", 10, 6, 40.0))
        );

        String report = metrics.toReport();

        assertTrue(report.contains("Kafka Simulation Topic Metrics"));
        assertTrue(report.contains("raw-firewall -> clean-firewall: raw=10, clean=6, reduction=40.00%"));
    }

    @Test
    void reportIncludesWindowMetricsWhenProvided() {
        DemoMetrics metrics = DemoMetrics.from(
                List.of(),
                List.of(),
                Duration.ZERO,
                "inmemory",
                Map.of(),
                List.of(),
                List.of(new WindowMetrics(
                        1,
                        Instant.parse("2026-06-22T13:00:00Z"),
                        Instant.parse("2026-06-22T13:01:00Z"),
                        10,
                        6,
                        40.0,
                        2,
                        2
                ))
        );

        String report = metrics.toReport();

        assertTrue(report.contains("Window Metrics"));
        assertTrue(report.contains("Windows processed: 1"));
        assertTrue(report.contains("window 1 [2026-06-22T13:00:00Z -> 2026-06-22T13:01:00Z): raw=10, reduced=6, reduction=40.00%, security=2 / 2"));
    }

    @Test
    void reportWarnsWhenSecurityEventsAreNotFullyPreserved() {
        List<LogEvent> rawEvents = List.of(
                event(LogSourceType.DNS, true),
                event(LogSourceType.DNS, true)
        );
        List<LogEvent> reducedEvents = List.of(event(LogSourceType.DNS, true));

        DemoMetrics metrics = DemoMetrics.from(rawEvents, reducedEvents, Duration.ZERO);

        assertTrue(metrics.toReport().contains("WARNING: Security-relevant preserved count is less than generated security-relevant count."));
    }

    private static LogEvent event(LogSourceType sourceType, boolean securityRelevant) {
        return LogEvent.observed(
                sourceType,
                Instant.parse("2026-06-22T13:00:00Z"),
                securityRelevant ? "SECURITY" : "BENIGN",
                "payload",
                securityRelevant,
                securityRelevant ? "security:key" : "benign:key"
        );
    }
}
