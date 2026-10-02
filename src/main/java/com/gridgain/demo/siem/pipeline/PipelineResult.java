package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.streaming.WindowMetrics;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record PipelineResult(
        List<LogEvent> rawEvents,
        List<LogEvent> reducedEvents,
        Map<String, Integer> rawTopicCounts,
        Map<String, Integer> cleanTopicCounts,
        List<DemoMetrics.TopicMetrics> topicMetrics,
        Duration processingTime,
        String reducerMode,
        Map<String, String> proofMetrics,
        List<WindowMetrics> windowMetrics
) {
    public PipelineResult(
            List<LogEvent> rawEvents,
            List<LogEvent> reducedEvents,
            Map<String, Integer> rawTopicCounts,
            Map<String, Integer> cleanTopicCounts,
            List<DemoMetrics.TopicMetrics> topicMetrics,
            Duration processingTime,
            String reducerMode,
            Map<String, String> proofMetrics
    ) {
        this(
                rawEvents,
                reducedEvents,
                rawTopicCounts,
                cleanTopicCounts,
                topicMetrics,
                processingTime,
                reducerMode,
                proofMetrics,
                List.of()
        );
    }

    public PipelineResult {
        rawEvents = List.copyOf(rawEvents);
        reducedEvents = List.copyOf(reducedEvents);
        rawTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(rawTopicCounts));
        cleanTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(cleanTopicCounts));
        topicMetrics = List.copyOf(topicMetrics);
        proofMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(proofMetrics));
        windowMetrics = List.copyOf(windowMetrics);
    }
}
