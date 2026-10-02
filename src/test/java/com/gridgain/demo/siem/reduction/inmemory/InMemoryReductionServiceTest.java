package com.gridgain.demo.siem.reduction.inmemory;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryReductionServiceTest {
    private final ReductionService reductionService = new InMemoryReductionService();

    @Test
    void reportsInMemoryReducerMode() {
        assertEquals("inmemory", reductionService.reducerMode());
    }

    @Test
    void preservesAllSecurityRelevantEvents() {
        LogEvent securityEventOne = LogEvent.observed(
                LogSourceType.FIREWALL,
                Instant.parse("2026-06-22T13:00:00Z"),
                "FIREWALL_BLOCK",
                "CEF:0|test security one",
                true,
                "security:one"
        );
        LogEvent securityEventTwo = LogEvent.observed(
                LogSourceType.DNS,
                Instant.parse("2026-06-22T13:00:01Z"),
                "DNS_THREAT_INTEL_MATCH",
                "<132> security two",
                true,
                "security:two"
        );
        List<LogEvent> rawEvents = List.of(
                securityEventOne,
                benignEvent(LogSourceType.FIREWALL, "firewall:allow:443"),
                benignEvent(LogSourceType.FIREWALL, "firewall:allow:443"),
                securityEventTwo
        );

        ReductionResult result = reductionService.reduce(rawEvents);

        assertEquals(2, result.rawSecurityEventCount());
        assertEquals(2, result.preservedSecurityEventCount());
        assertTrue(result.reducedEvents().contains(securityEventOne));
        assertTrue(result.reducedEvents().contains(securityEventTwo));
    }

    @Test
    void defaultReductionUsesConservativeBuyerTarget() {
        List<LogEvent> rawEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rawEvents.add(benignEvent(LogSourceType.DNS, "dns:query:A:intranet.example.internal"));
        }
        rawEvents.add(benignEvent(LogSourceType.DNS, "dns:query:A:updates.example.com"));

        ReductionResult result = reductionService.reduce(rawEvents);

        assertEquals(11, result.rawEventCount());
        assertEquals(7, result.reducedEventCount());
        assertTrue(result.reducedEvents().stream()
                .anyMatch(event -> event.eventType().equals("REDUCTION_SUMMARY")
                        && event.representedEventCount() == 5));
    }

    @Test
    void targetReductionControlsHowMuchBenignRepetitionIsReduced() {
        List<LogEvent> rawEvents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rawEvents.add(benignEvent(LogSourceType.DNS, "dns:query:A:intranet.example.internal"));
        }
        for (int i = 0; i < 10; i++) {
            rawEvents.add(securityEvent(LogSourceType.DNS, "security:dns:" + i));
        }

        ReductionResult result = reductionService.reduce(rawEvents, 40.0);

        assertEquals(110, result.rawEventCount());
        assertEquals(66, result.reducedEventCount());
        assertEquals(10, result.rawSecurityEventCount());
        assertEquals(10, result.preservedSecurityEventCount());
    }

    @Test
    void technicalStressTargetCanUseAggressiveSyntheticReduction() {
        List<LogEvent> rawEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rawEvents.add(benignEvent(LogSourceType.DNS, "dns:query:A:intranet.example.internal"));
        }
        rawEvents.add(benignEvent(LogSourceType.DNS, "dns:query:A:updates.example.com"));

        ReductionResult result = reductionService.reduce(rawEvents, 100.0);

        assertEquals(11, result.rawEventCount());
        assertEquals(2, result.reducedEventCount());
        assertTrue(result.reducedEvents().stream()
                .anyMatch(event -> event.eventType().equals("REDUCTION_SUMMARY")
                        && event.representedEventCount() == 10));
    }

    @Test
    void keepsSourceMetricsSeparateByIncludingSourceInFingerprint() {
        List<LogEvent> rawEvents = List.of(
                benignEvent(LogSourceType.FIREWALL, "shared-benign-key"),
                benignEvent(LogSourceType.FIREWALL, "shared-benign-key"),
                benignEvent(LogSourceType.DNS, "shared-benign-key"),
                benignEvent(LogSourceType.DNS, "shared-benign-key")
        );

        ReductionResult result = reductionService.reduce(rawEvents);

        assertEquals(2, result.reducedEventCount());
        assertTrue(result.reducedEvents().stream().anyMatch(event -> event.sourceType() == LogSourceType.FIREWALL));
        assertTrue(result.reducedEvents().stream().anyMatch(event -> event.sourceType() == LogSourceType.DNS));
    }

    private static LogEvent benignEvent(LogSourceType sourceType, String reductionKey) {
        return LogEvent.observed(
                sourceType,
                Instant.parse("2026-06-22T13:00:00Z"),
                "BENIGN",
                "benign payload",
                false,
                reductionKey
        );
    }

    private static LogEvent securityEvent(LogSourceType sourceType, String reductionKey) {
        return LogEvent.observed(
                sourceType,
                Instant.parse("2026-06-22T13:00:00Z"),
                "SECURITY",
                "security payload",
                true,
                reductionKey
        );
    }
}
