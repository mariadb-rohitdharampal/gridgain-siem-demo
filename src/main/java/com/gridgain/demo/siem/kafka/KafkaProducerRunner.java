package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.generator.LogGenerator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class KafkaProducerRunner {
    private final List<LogGenerator> generators;

    public KafkaProducerRunner(List<LogGenerator> generators) {
        this.generators = List.copyOf(generators);
    }

    public KafkaProduceResult run(int totalEvents, KafkaIntegrationConfig config) {
        return run(totalEvents, config, new KafkaTopicAdmin(), new KafkaRawEventProducer(config));
    }

    KafkaProduceResult run(
            int totalEvents,
            KafkaIntegrationConfig config,
            KafkaTopicProvisioner topicProvisioner,
            KafkaRawEventSink eventSink
    ) {
        if (config.createTopics()) {
            topicProvisioner.createRawTopics(config);
        }

        Map<String, Integer> perTopicProduced = emptyTopicCounts(config);
        int totalProduced = 0;

        try (eventSink) {
            for (int i = 0; i < generators.size(); i++) {
                LogGenerator generator = generators.get(i);
                int eventCount = eventsForSource(totalEvents, generators.size(), i);
                for (LogEvent event : generator.generate(eventCount)) {
                    eventSink.publish(event);
                    String topicName = config.rawTopicName(event.sourceType());
                    perTopicProduced.merge(topicName, 1, Integer::sum);
                    totalProduced++;
                }
            }
            eventSink.flush();
        }

        return new KafkaProduceResult(totalProduced, perTopicProduced);
    }

    private static Map<String, Integer> emptyTopicCounts(KafkaIntegrationConfig config) {
        Map<String, Integer> topicCounts = new LinkedHashMap<>();
        for (String topicName : config.rawTopicNames().values()) {
            topicCounts.putIfAbsent(topicName, 0);
        }
        return topicCounts;
    }

    private static int eventsForSource(int totalEvents, int sourceCount, int sourceIndex) {
        int baseEvents = totalEvents / sourceCount;
        int remainder = totalEvents % sourceCount;
        return baseEvents + (sourceIndex < remainder ? 1 : 0);
    }
}
