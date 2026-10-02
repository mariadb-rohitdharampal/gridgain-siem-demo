package com.gridgain.demo.siem.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record KafkaRawEventBatch(
        List<KafkaLogEventRecord> records,
        Map<String, Integer> consumedTopicCounts,
        int malformedRecordCount
) {
    public KafkaRawEventBatch {
        if (malformedRecordCount < 0) {
            throw new IllegalArgumentException("malformedRecordCount must not be negative");
        }
        records = List.copyOf(records);
        consumedTopicCounts = Collections.unmodifiableMap(new LinkedHashMap<>(consumedTopicCounts));
    }

    public static KafkaRawEventBatch empty() {
        return new KafkaRawEventBatch(List.of(), Map.of(), 0);
    }

    public int consumedRecordCount() {
        return consumedTopicCounts.values().stream().mapToInt(Integer::intValue).sum();
    }

    public int validRecordCount() {
        return records.size();
    }
}
