package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.generator.LogGenerator;
import com.gridgain.demo.siem.reduction.inmemory.InMemoryReductionService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowedSecurityPreservationTest {
    @Test
    void securityEventsBypassWindowReduction() {
        PipelineResult result = new WindowedKafkaSimulationPipeline(
                List.of(new SecurityOnlyGenerator()),
                Duration.ofSeconds(60)
        ).run(6, 90.0, new InMemoryReductionService());

        assertEquals(6, result.rawEvents().size());
        assertEquals(6, result.reducedEvents().size());
        assertEquals(2, result.windowMetrics().size());
        assertEquals(3, result.windowMetrics().get(0).securityEventsObserved());
        assertEquals(3, result.windowMetrics().get(0).securityEventsPreserved());
        assertEquals(3, result.windowMetrics().get(1).securityEventsObserved());
        assertEquals(3, result.windowMetrics().get(1).securityEventsPreserved());
        assertTrue(result.reducedEvents().stream().noneMatch(event -> event.eventType().equals("REDUCTION_SUMMARY")));
    }

    private static final class SecurityOnlyGenerator implements LogGenerator {
        @Override
        public LogSourceType sourceType() {
            return LogSourceType.CLOUD_ZERO_TRUST;
        }

        @Override
        public List<LogEvent> generate(int count) {
            List<LogEvent> events = new ArrayList<>();
            Instant baseTime = Instant.parse("2026-06-22T13:00:00Z");
            for (int i = 0; i < count; i++) {
                Instant timestamp = i < 3 ? baseTime.plusSeconds(i) : baseTime.plusSeconds(60 + i);
                events.add(LogEvent.observed(
                        sourceType(),
                        timestamp,
                        "SECURITY",
                        "security payload",
                        true,
                        "security:cloud:" + i
                ));
            }
            return events;
        }
    }
}
