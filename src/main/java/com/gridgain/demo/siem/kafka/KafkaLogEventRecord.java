package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;

import java.util.Objects;

public record KafkaLogEventRecord(
        String topic,
        String key,
        int partition,
        long offset,
        LogEvent event
) {
    public KafkaLogEventRecord {
        topic = requireText(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        Objects.requireNonNull(event, "event");
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
