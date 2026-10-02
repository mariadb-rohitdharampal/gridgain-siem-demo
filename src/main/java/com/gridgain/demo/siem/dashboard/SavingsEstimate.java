package com.gridgain.demo.siem.dashboard;

public record SavingsEstimate(
        double rawTbPerDay,
        double reducedTbPerDay,
        double tbPerDaySaved,
        double tbPerMonthSaved,
        double estimatedMonthlySavings,
        int baselineRetentionDays,
        double estimatedRetentionDays,
        double retentionExtensionDays
) {
}
