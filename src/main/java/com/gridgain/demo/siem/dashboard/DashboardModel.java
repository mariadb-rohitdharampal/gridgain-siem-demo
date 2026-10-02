package com.gridgain.demo.siem.dashboard;

import com.gridgain.demo.siem.metrics.DemoMetrics;

public record DashboardModel(
        DemoMetrics metrics,
        SavingsEstimate savingsEstimate,
        boolean kafkaSimulationMode,
        long averageEventBytes,
        double costPerTb
) {
}
