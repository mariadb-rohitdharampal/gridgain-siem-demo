package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.generator.LogGenerator;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import com.gridgain.demo.siem.streaming.TimeWindow;
import com.gridgain.demo.siem.streaming.WindowAssigner;
import com.gridgain.demo.siem.streaming.WindowMetrics;
import com.gridgain.demo.siem.topic.InMemoryTopic;
import com.gridgain.demo.siem.topic.Topic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WindowedKafkaSimulationPipeline {
    private final List<LogGenerator> generators;
    private final WindowAssigner windowAssigner;

    public WindowedKafkaSimulationPipeline(List<LogGenerator> generators, Duration windowSize) {
        this.generators = List.copyOf(generators);
        this.windowAssigner = new WindowAssigner(windowSize);
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
        List<LogEvent> orderedRawEvents = rawEvents.stream()
                .sorted(Comparator.comparing(LogEvent::timestamp))
                .toList();

        List<LogEvent> reducedEvents = new ArrayList<>();
        List<WindowMetrics> windowMetrics = new ArrayList<>();
        WindowBuffer currentWindowBuffer = null;
        long startNanos = System.nanoTime();

        for (LogEvent event : orderedRawEvents) {
            TimeWindow eventWindow = windowAssigner.assign(event.timestamp(), windowMetrics.size() + 1);
            if (currentWindowBuffer == null) {
                currentWindowBuffer = new WindowBuffer(eventWindow);
            } else if (!currentWindowBuffer.window().sameRange(eventWindow)) {
                windowMetrics.add(closeWindow(
                        currentWindowBuffer,
                        targetReductionPercentage,
                        reductionService,
                        cleanTopics,
                        reducedEvents
                ));
                currentWindowBuffer = new WindowBuffer(windowAssigner.assign(event.timestamp(), windowMetrics.size() + 1));
            }

            currentWindowBuffer.accept(event);
            if (event.securityRelevant()) {
                cleanTopics.get(event.sourceType()).publish(event);
                reducedEvents.add(event);
                currentWindowBuffer.recordSecurityPreserved();
            }
        }

        if (currentWindowBuffer != null) {
            windowMetrics.add(closeWindow(
                    currentWindowBuffer,
                    targetReductionPercentage,
                    reductionService,
                    cleanTopics,
                    reducedEvents
            ));
        }

        Duration processingTime = Duration.ofNanos(System.nanoTime() - startNanos);
        Map<String, Integer> cleanTopicCounts = topicCounts(cleanTopics);
        return new PipelineResult(
                rawEvents,
                reducedEvents,
                rawTopicCounts,
                cleanTopicCounts,
                topicMetrics(rawTopicCounts, cleanTopicCounts),
                processingTime,
                reductionService.reducerMode(),
                reductionService.proofMetrics(),
                windowMetrics
        );
    }

    private static WindowMetrics closeWindow(
            WindowBuffer windowBuffer,
            double targetReductionPercentage,
            ReductionService reductionService,
            Map<LogSourceType, Topic<LogEvent>> cleanTopics,
            List<LogEvent> reducedEvents
    ) {
        List<LogEvent> benignEvents = windowBuffer.benignEvents();
        List<LogEvent> reducedBenignEvents = List.of();
        if (!benignEvents.isEmpty()) {
            double effectiveBenignTarget = effectiveBenignTarget(
                    windowBuffer.rawEvents(),
                    benignEvents.size(),
                    targetReductionPercentage
            );
            ReductionResult reductionResult = reductionService.reduce(benignEvents, effectiveBenignTarget);
            reducedBenignEvents = reductionResult.reducedEvents();
        }

        for (LogEvent reducedEvent : reducedBenignEvents) {
            cleanTopics.get(reducedEvent.sourceType()).publish(reducedEvent);
            reducedEvents.add(reducedEvent);
        }

        long reducedWindowEvents = windowBuffer.securityEventsPreserved() + reducedBenignEvents.size();
        return WindowMetrics.from(
                windowBuffer.window(),
                windowBuffer.rawEvents(),
                reducedWindowEvents,
                windowBuffer.securityEventsObserved(),
                windowBuffer.securityEventsPreserved()
        );
    }

    private static double effectiveBenignTarget(long rawWindowEvents, long benignWindowEvents, double targetReductionPercentage) {
        if (benignWindowEvents == 0) {
            return 0.0;
        }

        long desiredWindowRemovals = Math.round(rawWindowEvents * (targetReductionPercentage / 100.0));
        double target = (desiredWindowRemovals * 100.0) / benignWindowEvents;
        return Math.max(0.0, Math.min(100.0, target));
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

    private static final class WindowBuffer {
        private final TimeWindow window;
        private final List<LogEvent> benignEvents = new ArrayList<>();
        private long rawEvents;
        private long securityEventsObserved;
        private long securityEventsPreserved;

        private WindowBuffer(TimeWindow window) {
            this.window = window;
        }

        private TimeWindow window() {
            return window;
        }

        private List<LogEvent> benignEvents() {
            return List.copyOf(benignEvents);
        }

        private long rawEvents() {
            return rawEvents;
        }

        private long securityEventsObserved() {
            return securityEventsObserved;
        }

        private long securityEventsPreserved() {
            return securityEventsPreserved;
        }

        private void accept(LogEvent event) {
            rawEvents++;
            if (event.securityRelevant()) {
                securityEventsObserved++;
            } else {
                benignEvents.add(event);
            }
        }

        private void recordSecurityPreserved() {
            securityEventsPreserved++;
        }
    }
}
