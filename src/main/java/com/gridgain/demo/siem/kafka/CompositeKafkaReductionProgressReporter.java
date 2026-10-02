package com.gridgain.demo.siem.kafka;

import java.util.List;

public final class CompositeKafkaReductionProgressReporter implements KafkaReductionProgressReporter {
    private final List<KafkaReductionProgressReporter> reporters;

    public CompositeKafkaReductionProgressReporter(List<KafkaReductionProgressReporter> reporters) {
        this.reporters = List.copyOf(reporters);
    }

    @Override
    public void report(KafkaReductionProgressSnapshot snapshot) {
        for (KafkaReductionProgressReporter reporter : reporters) {
            reporter.report(snapshot);
        }
    }
}
