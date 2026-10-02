package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;

interface KafkaRawEventSink extends AutoCloseable {
    void publish(LogEvent event);

    void flush();

    @Override
    void close();
}
