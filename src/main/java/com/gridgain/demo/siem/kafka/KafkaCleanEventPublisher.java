package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;

public interface KafkaCleanEventPublisher extends AutoCloseable {
    void publish(LogEvent event);

    void flush();

    @Override
    void close();
}
