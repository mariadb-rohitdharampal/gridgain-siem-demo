package com.gridgain.demo.siem.reduction;

import com.gridgain.demo.siem.event.LogEvent;

import java.util.List;

public record ReductionResult(List<LogEvent> rawEvents, List<LogEvent> reducedEvents) {
    public long rawEventCount() {
        return rawEvents.size();
    }

    public long reducedEventCount() {
        return reducedEvents.size();
    }

    public long rawSecurityEventCount() {
        return rawEvents.stream().filter(LogEvent::securityRelevant).count();
    }

    public long preservedSecurityEventCount() {
        return reducedEvents.stream().filter(LogEvent::securityRelevant).count();
    }
}
