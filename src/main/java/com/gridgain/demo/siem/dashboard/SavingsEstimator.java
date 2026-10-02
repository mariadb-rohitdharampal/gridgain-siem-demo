package com.gridgain.demo.siem.dashboard;

import com.gridgain.demo.siem.metrics.DemoMetrics;

public class SavingsEstimator {
    private static final double BYTES_PER_TB = 1_000_000_000_000.0;
    private static final int DAYS_PER_MONTH = 30;

    public SavingsEstimate estimate(
            DemoMetrics metrics,
            long averageEventBytes,
            double costPerTb,
            int retentionDays
    ) {
        if (averageEventBytes <= 0) {
            throw new IllegalArgumentException("averageEventBytes must be positive");
        }
        if (costPerTb < 0.0) {
            throw new IllegalArgumentException("costPerTb must not be negative");
        }
        if (retentionDays <= 0) {
            throw new IllegalArgumentException("retentionDays must be positive");
        }

        double rawTbPerDay = (metrics.rawEvents() * averageEventBytes) / BYTES_PER_TB;
        double reducedTbPerDay = (metrics.reducedEvents() * averageEventBytes) / BYTES_PER_TB;
        double tbPerDaySaved = Math.max(0.0, rawTbPerDay - reducedTbPerDay);
        double tbPerMonthSaved = tbPerDaySaved * DAYS_PER_MONTH;
        double estimatedMonthlySavings = tbPerMonthSaved * costPerTb;
        double estimatedRetentionDays = metrics.reducedEvents() == 0
                ? retentionDays
                : retentionDays * (metrics.rawEvents() / (double) metrics.reducedEvents());
        double retentionExtensionDays = Math.max(0.0, estimatedRetentionDays - retentionDays);

        return new SavingsEstimate(
                rawTbPerDay,
                reducedTbPerDay,
                tbPerDaySaved,
                tbPerMonthSaved,
                estimatedMonthlySavings,
                retentionDays,
                estimatedRetentionDays,
                retentionExtensionDays
        );
    }
}
