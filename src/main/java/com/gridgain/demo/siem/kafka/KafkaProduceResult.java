package com.gridgain.demo.siem.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record KafkaProduceResult(
        int totalProduced,
        Map<String, Integer> perTopicProduced
) {
    public KafkaProduceResult {
        perTopicProduced = Collections.unmodifiableMap(new LinkedHashMap<>(perTopicProduced));
    }

    public int producedForTopic(String topicName) {
        return perTopicProduced.getOrDefault(topicName, 0);
    }
}
