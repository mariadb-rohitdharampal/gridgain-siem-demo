package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.generator.LogGenerator;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import com.gridgain.demo.siem.topic.InMemoryTopic;
import com.gridgain.demo.siem.topic.Topic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KafkaSimulationPipeline {
    private final List<LogGenerator> generators;

    public KafkaSimulationPipeline(List<LogGenerator> generators) {
        this.generators = List.copyOf(generators);
    }

    public PipelineResult run(int totalEvents, double targetReductionPercentage, ReductionService reductionService) {
        Map<LogSourceType, Topic<LogEvent>> rawTopics = createTopics(true);
        Map<LogSourceType, Topic<LogEvent>> cleanTopics = createTopics(false);

        for (int i = 0; i < generators.size(); i++) {
            LogGenerator generator = generators.get(i);
            int eventCount = eventsForSource(totalEvents, generators.size(), i);
            rawTopics.get(generator.sourceType()).publishAll(generator.generate(eventCount));
        }

        Map<String, Integer> rawTopicCounts = topicCounts(rawTopics);
        List<LogEvent> rawEvents = drainTopics(rawTopics);

        long startNanos = System.nanoTime();
        ReductionResult reductionResult = reductionService.reduce(rawEvents, targetReductionPercentage);
        for (LogEvent event : reductionResult.reducedEvents()) {
            cleanTopics.get(event.sourceType()).publish(event);
        }
        Duration processingTime = Duration.ofNanos(System.nanoTime() - startNanos);

        Map<String, Integer> cleanTopicCounts = topicCounts(cleanTopics);
        return new PipelineResult(
                rawEvents,
                reductionResult.reducedEvents(),
                rawTopicCounts,
                cleanTopicCounts,
                topicMetrics(rawTopicCounts, cleanTopicCounts),
                processingTime,
                reductionService.reducerMode(),
                reductionService.proofMetrics()
        );
    }

    private static Map<LogSourceType, Topic<LogEvent>> createTopics(boolean raw) {
        Map<LogSourceType, Topic<LogEvent>> topics = new EnumMap<>(LogSourceType.class);
        for (LogSourceType sourceType : LogSourceType.values()) {
            String topicName = raw ? TopicNames.rawTopicName(sourceType) : TopicNames.cleanTopicName(sourceType);
            topics.put(sourceType, new InMemoryTopic<>(topicName));
        }
        return topics;
    }

    private static Map<String, Integer> topicCounts(Map<LogSourceType, Topic<LogEvent>> topics) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (LogSourceType sourceType : LogSourceType.values()) {
            Topic<LogEvent> topic = topics.get(sourceType);
            counts.put(topic.name(), topic.size());
        }
        return counts;
    }

    private static List<LogEvent> drainTopics(Map<LogSourceType, Topic<LogEvent>> topics) {
        List<LogEvent> events = new ArrayList<>();
        for (LogSourceType sourceType : LogSourceType.values()) {
            events.addAll(topics.get(sourceType).drain());
        }
        return events;
    }

    private static List<DemoMetrics.TopicMetrics> topicMetrics(
            Map<String, Integer> rawTopicCounts,
            Map<String, Integer> cleanTopicCounts
    ) {
        List<DemoMetrics.TopicMetrics> topicMetrics = new ArrayList<>();
        for (LogSourceType sourceType : LogSourceType.values()) {
            String rawTopicName = TopicNames.rawTopicName(sourceType);
            String cleanTopicName = TopicNames.cleanTopicName(sourceType);
            int rawCount = rawTopicCounts.getOrDefault(rawTopicName, 0);
            int cleanCount = cleanTopicCounts.getOrDefault(cleanTopicName, 0);
            topicMetrics.add(new DemoMetrics.TopicMetrics(
                    rawTopicName,
                    cleanTopicName,
                    rawCount,
                    cleanCount,
                    DemoMetrics.reductionPercentage(rawCount, cleanCount)
            ));
        }
        return topicMetrics;
    }

    private static int eventsForSource(int totalEvents, int sourceCount, int sourceIndex) {
        int baseEvents = totalEvents / sourceCount;
        int remainder = totalEvents % sourceCount;
        return baseEvents + (sourceIndex < remainder ? 1 : 0);
    }
}
