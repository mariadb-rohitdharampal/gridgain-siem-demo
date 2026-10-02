package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.generator.LogGenerator;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.reduction.ReductionService;
import com.gridgain.demo.siem.reduction.gridgain.GridGainReductionService;
import com.gridgain.demo.siem.reduction.inmemory.InMemoryReductionService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaSimulationPipelineTest {
    @Test
    void publishesRawEventsReducesAndPublishesCleanEventsByTopic() {
        List<LogGenerator> generators = patternGenerators();
        ReductionService reductionService = new InMemoryReductionService();

        PipelineResult result = new KafkaSimulationPipeline(generators).run(40, 40.0, reductionService);

        assertEquals(10, result.rawTopicCounts().get("raw-firewall"));
        assertEquals(10, result.rawTopicCounts().get("raw-dns"));
        assertEquals(10, result.rawTopicCounts().get("raw-windows-ad"));
        assertEquals(10, result.rawTopicCounts().get("raw-cloud-zero-trust"));

        assertEquals(6, result.cleanTopicCounts().get("clean-firewall"));
        assertEquals(6, result.cleanTopicCounts().get("clean-dns"));
        assertEquals(6, result.cleanTopicCounts().get("clean-windows-ad"));
        assertEquals(6, result.cleanTopicCounts().get("clean-cloud-zero-trust"));

        long rawSecurityEvents = result.rawEvents().stream().filter(LogEvent::securityRelevant).count();
        long cleanSecurityEvents = result.reducedEvents().stream().filter(LogEvent::securityRelevant).count();
        assertEquals(rawSecurityEvents, cleanSecurityEvents);
        assertEquals(8, cleanSecurityEvents);

        DemoMetrics.TopicMetrics firewallMetrics = result.topicMetrics().get(0);
        assertEquals("raw-firewall", firewallMetrics.rawTopicName());
        assertEquals("clean-firewall", firewallMetrics.cleanTopicName());
        assertEquals(10, firewallMetrics.rawTopicCount());
        assertEquals(6, firewallMetrics.cleanTopicCount());
        assertEquals(40.0, firewallMetrics.reductionPercentage());
    }

    @Test
    void kafkaSimulationWorksWithGridGainReducer() {
        try (GridGainReductionService reductionService = new GridGainReductionService()) {
            PipelineResult result = new KafkaSimulationPipeline(patternGenerators()).run(40, 40.0, reductionService);

            assertEquals(40, result.rawEvents().size());
            assertEquals(24, result.reducedEvents().size());
            assertEquals("3", result.proofMetrics().get("embedded node count"));
            assertEquals("PARTITIONED", result.proofMetrics().get("cache mode"));
            assertEquals("1", result.proofMetrics().get("backup count"));

            long rawSecurityEvents = result.rawEvents().stream().filter(LogEvent::securityRelevant).count();
            long cleanSecurityEvents = result.reducedEvents().stream().filter(LogEvent::securityRelevant).count();
            assertEquals(rawSecurityEvents, cleanSecurityEvents);
        }
    }

    private static List<LogGenerator> patternGenerators() {
        return List.of(
                new PatternGenerator(LogSourceType.FIREWALL),
                new PatternGenerator(LogSourceType.DNS),
                new PatternGenerator(LogSourceType.WINDOWS_AD),
                new PatternGenerator(LogSourceType.CLOUD_ZERO_TRUST)
        );
    }

    private static final class PatternGenerator implements LogGenerator {
        private final LogSourceType sourceType;

        private PatternGenerator(LogSourceType sourceType) {
            this.sourceType = sourceType;
        }

        @Override
        public LogSourceType sourceType() {
            return sourceType;
        }

        @Override
        public List<LogEvent> generate(int count) {
            List<LogEvent> events = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                boolean securityRelevant = i % 5 == 0;
                events.add(LogEvent.observed(
                        sourceType,
                        Instant.parse("2026-06-22T13:00:00Z").plusMillis(i),
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
