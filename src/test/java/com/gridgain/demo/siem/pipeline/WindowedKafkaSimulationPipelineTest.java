package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.generator.LogGenerator;
import com.gridgain.demo.siem.reduction.gridgain.GridGainReductionService;
import com.gridgain.demo.siem.reduction.inmemory.InMemoryReductionService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowedKafkaSimulationPipelineTest {
    @Test
    void processesSimulatedKafkaTopicsByTimeWindow() {
        PipelineResult result = new WindowedKafkaSimulationPipeline(windowGenerators(), Duration.ofSeconds(60))
                .run(16, 50.0, new InMemoryReductionService());

        assertEquals(4, result.rawTopicCounts().get("raw-firewall"));
        assertEquals(4, result.rawTopicCounts().get("raw-dns"));
        assertEquals(4, result.rawTopicCounts().get("raw-windows-ad"));
        assertEquals(4, result.rawTopicCounts().get("raw-cloud-zero-trust"));

        assertEquals(3, result.cleanTopicCounts().get("clean-firewall"));
        assertEquals(3, result.cleanTopicCounts().get("clean-dns"));
        assertEquals(3, result.cleanTopicCounts().get("clean-windows-ad"));
        assertEquals(3, result.cleanTopicCounts().get("clean-cloud-zero-trust"));

        assertEquals(2, result.windowMetrics().size());
        assertEquals(1, result.windowMetrics().get(0).sequence());
        assertEquals(8, result.windowMetrics().get(0).rawEvents());
        assertEquals(8, result.windowMetrics().get(0).reducedEvents());
        assertEquals(0.0, result.windowMetrics().get(0).reductionPercentage());
        assertEquals(4, result.windowMetrics().get(0).securityEventsObserved());
        assertEquals(4, result.windowMetrics().get(0).securityEventsPreserved());

        assertEquals(2, result.windowMetrics().get(1).sequence());
        assertEquals(8, result.windowMetrics().get(1).rawEvents());
        assertEquals(4, result.windowMetrics().get(1).reducedEvents());
        assertEquals(50.0, result.windowMetrics().get(1).reductionPercentage());
        assertEquals(0, result.windowMetrics().get(1).securityEventsObserved());

        long rawSecurityEvents = result.rawEvents().stream().filter(LogEvent::securityRelevant).count();
        long reducedSecurityEvents = result.reducedEvents().stream().filter(LogEvent::securityRelevant).count();
        assertEquals(rawSecurityEvents, reducedSecurityEvents);
        assertTrue(result.reducedEvents().stream()
                .anyMatch(event -> event.eventType().equals("REDUCTION_SUMMARY")
                        && event.rawPayload().contains("firstSeen=")
                        && event.rawPayload().contains("lastSeen=")
                        && event.rawPayload().contains("reductionKey=")
                        && event.rawPayload().contains("count=2")));
    }

    @Test
    void windowedPipelineWorksWithThreeNodeGridGainReducer() {
        try (GridGainReductionService reductionService = new GridGainReductionService()) {
            PipelineResult result = new WindowedKafkaSimulationPipeline(windowGenerators(), Duration.ofSeconds(60))
                    .run(16, 50.0, reductionService);

            assertEquals(2, result.windowMetrics().size());
            assertEquals(12, result.reducedEvents().size());
            assertEquals("3", result.proofMetrics().get("embedded node count"));
            assertEquals("PARTITIONED", result.proofMetrics().get("cache mode"));
            assertEquals("1", result.proofMetrics().get("backup count"));

            long rawSecurityEvents = result.rawEvents().stream().filter(LogEvent::securityRelevant).count();
            long reducedSecurityEvents = result.reducedEvents().stream().filter(LogEvent::securityRelevant).count();
            assertEquals(rawSecurityEvents, reducedSecurityEvents);
        }
    }

    private static List<LogGenerator> windowGenerators() {
        return List.of(
                new WindowPatternGenerator(LogSourceType.FIREWALL),
                new WindowPatternGenerator(LogSourceType.DNS),
                new WindowPatternGenerator(LogSourceType.WINDOWS_AD),
                new WindowPatternGenerator(LogSourceType.CLOUD_ZERO_TRUST)
        );
    }

    private static final class WindowPatternGenerator implements LogGenerator {
        private final LogSourceType sourceType;

        private WindowPatternGenerator(LogSourceType sourceType) {
            this.sourceType = sourceType;
        }

        @Override
        public LogSourceType sourceType() {
            return sourceType;
        }

        @Override
        public List<LogEvent> generate(int count) {
            List<LogEvent> events = new ArrayList<>();
            Instant baseTime = Instant.parse("2026-06-22T13:00:00Z");
            for (int i = 0; i < count; i++) {
                boolean securityRelevant = i == 0;
                Instant timestamp = i < 2 ? baseTime.plusSeconds(i) : baseTime.plusSeconds(60 + i);
                events.add(LogEvent.observed(
                        sourceType,
                        timestamp,
                        securityRelevant ? "SECURITY" : "BENIGN",
                        securityRelevant ? "security payload" : "benign payload",
                        securityRelevant,
                        securityRelevant ? "security:" + sourceType.name() + ":" + i : "benign:" + sourceType.name()
                ));
            }
            return events;
        }
    }
}
