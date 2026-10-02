package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.streaming.WindowMetrics;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record KafkaReductionResult(
        Map<String, Integer> consumedRawTopicCounts,
        Map<String, Integer> producedCleanTopicCounts,
        long validConsumedEventCount,
        int malformedRecordCount,
        long securityEventsObserved,
        long securityEventsPreserved,
        Duration processingTime,
        String reducerMode,
        Map<String, String> proofMetrics,
        List<WindowMetrics> windowMetrics
) {
    public KafkaReductionResult {
        consumedRawTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(consumedRawTopicCounts));
        producedCleanTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(producedCleanTopicCounts));
        proofMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(proofMetrics));
        windowMetrics = List.copyOf(windowMetrics);
    }

    public long consumedEvents() {
        return validConsumedEventCount;
    }

    public long totalKafkaRecordsSeen() {
        return consumedRawTopicCounts.values().stream().mapToLong(Integer::longValue).sum();
    }

    public long producedEvents() {
        return producedCleanTopicCounts.values().stream().mapToLong(Integer::longValue).sum();
    }

    public double reductionPercentage() {
        return DemoMetrics.reductionPercentage(consumedEvents(), producedEvents());
    }

    public int windowsProcessed() {
        return windowMetrics.size();
    }

    public List<WindowMetrics> topWindowsByRawVolume(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return windowMetrics.stream()
                .sorted(Comparator.comparingLong(WindowMetrics::rawEvents)
                        .reversed()
                        .thenComparingInt(WindowMetrics::sequence))
                .limit(limit)
                .toList();
    }

    public String toReport() {
        return toReport(false);
    }

    public String toReport(boolean verboseWindows) {
        StringBuilder report = new StringBuilder();
        report.append("GridGain SIEM Real Kafka Reduction Demo").append(System.lineSeparator());
        report.append("==========================================").append(System.lineSeparator());
        report.append("Reducer mode: ").append(reducerMode).append(System.lineSeparator());
        report.append("Pipeline: real Kafka raw topics -> reducer -> real Kafka clean topics").append(System.lineSeparator());
        report.append("Offset strategy: manual synchronous commits, at-least-once output").append(System.lineSeparator());
        report.append(System.lineSeparator());
        report.append("Overall Metrics").append(System.lineSeparator());
        report.append("Kafka records seen: ").append(totalKafkaRecordsSeen()).append(System.lineSeparator());
        report.append("Valid consumed events: ").append(consumedEvents()).append(System.lineSeparator());
        report.append("Produced clean events: ").append(producedEvents()).append(System.lineSeparator());
        report.append("Reduction: ").append("%.2f%%".formatted(reductionPercentage())).append(System.lineSeparator());
        report.append("Malformed records: ").append(malformedRecordCount).append(System.lineSeparator());
        report.append("Security events preserved: ")
                .append(securityEventsPreserved)
                .append(" / ")
                .append(securityEventsObserved)
                .append(System.lineSeparator());
        if (securityEventsPreserved < securityEventsObserved) {
            report.append("WARNING: Security-relevant preserved count is less than consumed security-relevant count.")
                    .append(System.lineSeparator());
        }
        report.append("Processing time: ").append(processingTime.toMillis()).append(" ms").append(System.lineSeparator());
        report.append(System.lineSeparator());
        if (!proofMetrics.isEmpty()) {
            report.append("Ignite Proof Metrics").append(System.lineSeparator());
            proofMetrics.forEach((key, value) -> report.append("- ")
                    .append(key)
                    .append(": ")
                    .append(value)
                    .append(System.lineSeparator()));
            report.append(System.lineSeparator());
        }
        report.append("Kafka Topic Metrics").append(System.lineSeparator());
        consumedRawTopicCounts.forEach((topic, count) ->
                report.append("- consumed ").append(topic).append(": ").append(count).append(System.lineSeparator()));
        producedCleanTopicCounts.forEach((topic, count) ->
                report.append("- produced ").append(topic).append(": ").append(count).append(System.lineSeparator()));
        report.append(System.lineSeparator());
        report.append("Window Metrics").append(System.lineSeparator());
        report.append("Windows processed: ").append(windowsProcessed()).append(System.lineSeparator());
        report.append("Top 5 windows by raw volume").append(System.lineSeparator());
        for (WindowMetrics windowMetric : topWindowsByRawVolume(5)) {
            appendWindowLine(report, windowMetric);
        }

        if (verboseWindows) {
            report.append("All Window Details").append(System.lineSeparator());
            for (WindowMetrics windowMetric : windowMetrics) {
                appendWindowLine(report, windowMetric);
            }
        }
        return report.toString();
    }

    private static void appendWindowLine(StringBuilder report, WindowMetrics windowMetric) {
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
                .append("%.2f%%".formatted(windowMetric.reductionPercentage()))
                .append(", security=")
                .append(windowMetric.securityEventsPreserved())
                .append(" / ")
                .append(windowMetric.securityEventsObserved())
                .append(System.lineSeparator());
    }
}
