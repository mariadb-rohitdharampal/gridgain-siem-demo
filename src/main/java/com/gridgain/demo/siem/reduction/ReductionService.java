package com.gridgain.demo.siem.reduction;

import com.gridgain.demo.siem.event.LogEvent;

import java.util.List;
import java.util.Map;

public interface ReductionService extends AutoCloseable {
    double DEFAULT_TARGET_REDUCTION_PERCENTAGE = 40.0;

    default ReductionResult reduce(List<LogEvent> rawEvents) {
        return reduce(rawEvents, DEFAULT_TARGET_REDUCTION_PERCENTAGE);
    }

    ReductionResult reduce(List<LogEvent> rawEvents, double targetReductionPercentage);

    default String reducerMode() {
        return "unknown";
    }

    default Map<String, String> proofMetrics() {
        return Map.of();
    }

    @Override
    default void close() {
    }
}
