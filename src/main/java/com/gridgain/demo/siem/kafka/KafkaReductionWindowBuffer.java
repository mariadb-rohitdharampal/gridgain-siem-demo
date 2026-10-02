package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.streaming.TimeWindow;

import java.util.ArrayList;
import java.util.List;

final class KafkaReductionWindowBuffer {
    private final TimeWindow window;
    private final List<LogEvent> benignEvents = new ArrayList<>();
    private long rawEvents;
    private long securityEventsObserved;
    private long securityEventsPreserved;

    KafkaReductionWindowBuffer(TimeWindow window) {
        this.window = window;
    }

    TimeWindow window() {
        return window;
    }

    List<LogEvent> benignEvents() {
        return List.copyOf(benignEvents);
    }

    long rawEvents() {
        return rawEvents;
    }

    long benignEventCount() {
        return benignEvents.size();
    }

    long securityEventsObserved() {
        return securityEventsObserved;
    }

    long securityEventsPreserved() {
        return securityEventsPreserved;
    }

    void accept(LogEvent event) {
        rawEvents++;
        if (event.securityRelevant()) {
            securityEventsObserved++;
        } else {
            benignEvents.add(event);
        }
    }

    void recordSecurityPreserved() {
        securityEventsPreserved++;
    }
}
