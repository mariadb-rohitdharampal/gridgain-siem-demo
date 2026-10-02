package com.gridgain.demo.siem.metrics;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.streaming.WindowMetrics;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record DemoMetrics(
        long rawEvents,
        long reducedEvents,
        double reductionPercentage,
        long securityEventsPreserved,
        long securityEventsObserved,
        Duration processingTime,
        Map<LogSourceType, SourceMetrics> perSourceMetrics,
        String reducerMode,
        Map<String, String> proofMetrics,
        List<TopicMetrics> topicMetrics,
        List<WindowMetrics> windowMetrics
) {
    public static DemoMetrics from(List<LogEvent> rawEvents, List<LogEvent> reducedEvents, Duration processingTime) {
        return from(rawEvents, reducedEvents, processingTime, "inmemory", Map.of());
    }

    public static DemoMetrics from(
            List<LogEvent> rawEvents,
            List<LogEvent> reducedEvents,
            Duration processingTime,
            String reducerMode,
            Map<String, String> proofMetrics
    ) {
        return from(rawEvents, reducedEvents, processingTime, reducerMode, proofMetrics, List.of());
    }

    public static DemoMetrics from(
            List<LogEvent> rawEvents,
            List<LogEvent> reducedEvents,
            Duration processingTime,
            String reducerMode,
            Map<String, String> proofMetrics,
            List<TopicMetrics> topicMetrics
    ) {
        return from(rawEvents, reducedEvents, processingTime, reducerMode, proofMetrics, topicMetrics, List.of());
    }

    public static DemoMetrics from(
            List<LogEvent> rawEvents,
            List<LogEvent> reducedEvents,
            Duration processingTime,
            String reducerMode,
            Map<String, String> proofMetrics,
            List<TopicMetrics> topicMetrics,
            List<WindowMetrics> windowMetrics
    ) {
        Map<LogSourceType, Long> rawBySource = rawEvents.stream()
                .collect(Collectors.groupingBy(LogEvent::sourceType, () -> new EnumMap<>(LogSourceType.class), Collectors.counting()));
        Map<LogSourceType, Long> reducedBySource = reducedEvents.stream()
                .collect(Collectors.groupingBy(LogEvent::sourceType, () -> new EnumMap<>(LogSourceType.class), Collectors.counting()));

        Map<LogSourceType, SourceMetrics> sourceMetrics = new EnumMap<>(LogSourceType.class);
        for (LogSourceType sourceType : LogSourceType.values()) {
            long rawCount = rawBySource.getOrDefault(sourceType, 0L);
            long reducedCount = reducedBySource.getOrDefault(sourceType, 0L);
            sourceMetrics.put(sourceType, new SourceMetrics(rawCount, reducedCount, reductionPercentage(rawCount, reducedCount)));
        }

        long rawCount = rawEvents.size();
        long reducedCount = reducedEvents.size();
        long observedSecurity = rawEvents.stream().filter(LogEvent::securityRelevant).count();
        long preservedSecurity = reducedEvents.stream().filter(LogEvent::securityRelevant).count();

        return new DemoMetrics(
                rawCount,
                reducedCount,
                reductionPercentage(rawCount, reducedCount),
                preservedSecurity,
                observedSecurity,
                processingTime,
                Map.copyOf(sourceMetrics),
                reducerMode,
                Collections.unmodifiableMap(new LinkedHashMap<>(proofMetrics)),
                List.copyOf(topicMetrics),
                List.copyOf(windowMetrics)
        );
    }

    public String toReport() {
        StringBuilder report = new StringBuilder();
        report.append("MariaDB GridGain SIEM Reduction Demo").append(System.lineSeparator());
        report.append("======================================").append(System.lineSeparator());
        report.append("Reducer mode: ").append(reducerMode).append(System.lineSeparator());
        report.append("Pipeline: pre-SIEM optimization simulation, no Kafka broker, no SQL, no persistence").append(System.lineSeparator());
        if (!proofMetrics.isEmpty()) {
            report.append("Ignite Proof Metrics").append(System.lineSeparator());
            proofMetrics.forEach((key, value) -> report.append("- ")
                    .append(key)
                    .append(": ")
                    .append(value)
                    .append(System.lineSeparator()));
        }
        if (!topicMetrics.isEmpty()) {
            report.append("Kafka Simulation Topic Metrics").append(System.lineSeparator());
            for (TopicMetrics topicMetric : topicMetrics) {
                report.append("- ")
                        .append(topicMetric.rawTopicName())
                        .append(" -> ")
                        .append(topicMetric.cleanTopicName())
                        .append(": raw=")
                        .append(topicMetric.rawTopicCount())
                        .append(", clean=")
                        .append(topicMetric.cleanTopicCount())
                        .append(", reduction=")
                        .append(formatPercentage(topicMetric.reductionPercentage()))
                        .append(System.lineSeparator());
            }
        }
        if (!windowMetrics.isEmpty()) {
            report.append("Window Metrics").append(System.lineSeparator());
            report.append("Windows processed: ").append(windowMetrics.size()).append(System.lineSeparator());
            for (WindowMetrics windowMetric : windowMetrics) {
                report.append("- window ")
                        .append(windowMetric.sequence())
                        .append(" [")
                        .append(windowMetric.startTime())
                        .append(" -> ")
                        .append(windowMetric.endTime())
                        .append("): raw=")
                        .append(windowMetric.rawEvents())
                        .append(", reduced=")
                        .append(windowMetric.reducedEvents())
                        .append(", reduction=")
                        .append(formatPercentage(windowMetric.reductionPercentage()))
                        .append(", security=")
                        .append(windowMetric.securityEventsPreserved())
                        .append(" / ")
                        .append(windowMetric.securityEventsObserved())
                        .append(System.lineSeparator());
            }
        }
        report.append(System.lineSeparator());
        report.append("Overall Metrics").append(System.lineSeparator());
        report.append("Raw events: ").append(rawEvents).append(System.lineSeparator());
        report.append("Reduced events: ").append(reducedEvents).append(System.lineSeparator());
        report.append("Reduction: ").append(formatPercentage(reductionPercentage)).append(System.lineSeparator());
        report.append("Security events preserved: ")
                .append(securityEventsPreserved)
                .append(" / ")
                .append(securityEventsObserved)
                .append(System.lineSeparator());
        if (securityEventsPreserved < securityEventsObserved) {
            report.append("WARNING: Security-relevant preserved count is less than generated security-relevant count.")
                    .append(System.lineSeparator());
        }
        report.append("Processing time: ").append(processingTime.toMillis()).append(" ms").append(System.lineSeparator());
        report.append(System.lineSeparator());
        report.append("Per-Source Metrics").append(System.lineSeparator());
        for (LogSourceType sourceType : LogSourceType.values()) {
            SourceMetrics source = perSourceMetrics.get(sourceType);
            report.append("- ")
                    .append(sourceType.displayName())
                    .append(": raw=")
                    .append(source.rawEvents())
                    .append(", reduced=")
                    .append(source.reducedEvents())
                    .append(", reduction=")
                    .append(formatPercentage(source.reductionPercentage()))
                    .append(System.lineSeparator());
        }
        return report.toString();
    }

    public static double reductionPercentage(long rawEvents, long reducedEvents) {
        if (rawEvents == 0) {
            return 0.0;
        }
        return ((rawEvents - reducedEvents) * 100.0) / rawEvents;
    }

    private static String formatPercentage(double value) {
        return "%.2f%%".formatted(value);
    }

    public record SourceMetrics(long rawEvents, long reducedEvents, double reductionPercentage) {
    }

    public record TopicMetrics(
            String rawTopicName,
            String cleanTopicName,
            long rawTopicCount,
            long cleanTopicCount,
            double reductionPercentage
    ) {
    }
}
