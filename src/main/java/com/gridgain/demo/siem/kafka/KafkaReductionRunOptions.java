package com.gridgain.demo.siem.kafka;

import java.time.Duration;
import java.util.Objects;

public record KafkaReductionRunOptions(
        boolean continuous,
        Duration runDuration,
        Duration metricsInterval
) {
    public KafkaReductionRunOptions {
        runDuration = Objects.requireNonNull(runDuration, "runDuration");
        metricsInterval = Objects.requireNonNull(metricsInterval, "metricsInterval");
        if (!continuous && (runDuration.isZero() || runDuration.isNegative())) {
            throw new IllegalArgumentException("runDuration must be positive for bounded Kafka reduction");
        }
        if (continuous && (metricsInterval.isZero() || metricsInterval.isNegative())) {
            throw new IllegalArgumentException("metricsInterval must be positive for continuous Kafka reduction");
        }
    }

    public static KafkaReductionRunOptions bounded(Duration runDuration) {
        return new KafkaReductionRunOptions(false, runDuration, Duration.ZERO);
    }

    public static KafkaReductionRunOptions continuous(Duration metricsInterval) {
        if (metricsInterval.isZero() || metricsInterval.isNegative()) {
            throw new IllegalArgumentException("metricsInterval must be positive for continuous Kafka reduction");
        }
        return new KafkaReductionRunOptions(true, Duration.ZERO, metricsInterval);
    }
}
