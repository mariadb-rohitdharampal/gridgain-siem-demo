package com.gridgain.demo.siem.kafka;

import java.time.Instant;

interface KafkaReductionClock {
    Instant instant();

    long nanoTime();

    static KafkaReductionClock systemUtc() {
        return new KafkaReductionClock() {
            @Override
            public Instant instant() {
                return Instant.now();
            }

            @Override
            public long nanoTime() {
                return System.nanoTime();
            }
        };
    }
}
