package com.gridgain.demo.siem.kafka;

@FunctionalInterface
public interface KafkaReductionProgressReporter {
    void report(KafkaReductionProgressSnapshot snapshot);

    static KafkaReductionProgressReporter noop() {
        return snapshot -> {
        };
    }
}
