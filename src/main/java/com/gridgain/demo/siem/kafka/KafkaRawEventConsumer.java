package com.gridgain.demo.siem.kafka;

import java.time.Duration;

public interface KafkaRawEventConsumer extends AutoCloseable {
    KafkaRawEventBatch poll(Duration timeout);

    void commit();

    default void wakeup() {
    }

    @Override
    void close();
}
