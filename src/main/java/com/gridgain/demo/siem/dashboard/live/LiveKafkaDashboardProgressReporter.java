package com.gridgain.demo.siem.dashboard.live;

import com.gridgain.demo.siem.kafka.KafkaReductionProgressReporter;
import com.gridgain.demo.siem.kafka.KafkaReductionProgressSnapshot;

public final class LiveKafkaDashboardProgressReporter implements KafkaReductionProgressReporter {
    private final LiveKafkaDashboardState state;

    public LiveKafkaDashboardProgressReporter(LiveKafkaDashboardState state) {
        this.state = state;
    }

    @Override
    public void report(KafkaReductionProgressSnapshot snapshot) {
        state.update(snapshot);
    }
}
