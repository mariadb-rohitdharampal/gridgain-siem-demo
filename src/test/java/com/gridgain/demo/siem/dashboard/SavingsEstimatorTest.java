package com.gridgain.demo.siem.dashboard;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SavingsEstimatorTest {
    @Test
    void calculatesSavingsAndRetentionExtension() {
        DemoMetrics metrics = DemoMetrics.from(events(1_000), events(600), Duration.ZERO);

        SavingsEstimate estimate = new SavingsEstimator().estimate(metrics, 1_000, 500.0, 30);

        assertEquals(0.000001, estimate.rawTbPerDay(), 0.000000001);
        assertEquals(0.0000006, estimate.reducedTbPerDay(), 0.000000001);
        assertEquals(0.0000004, estimate.tbPerDaySaved(), 0.000000001);
        assertEquals(0.000012, estimate.tbPerMonthSaved(), 0.000000001);
        assertEquals(0.006, estimate.estimatedMonthlySavings(), 0.000000001);
        assertEquals(30, estimate.baselineRetentionDays());
        assertEquals(50.0, estimate.estimatedRetentionDays(), 0.000000001);
        assertEquals(20.0, estimate.retentionExtensionDays(), 0.000000001);
    }

    private static List<LogEvent> events(int count) {
        List<LogEvent> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(LogEvent.observed(
                    LogSourceType.FIREWALL,
                    Instant.parse("2026-06-22T13:00:00Z").plusMillis(i),
                    "BENIGN",
                    "payload",
                    false,
                    "key"
            ));
        }
        return events;
    }
}
