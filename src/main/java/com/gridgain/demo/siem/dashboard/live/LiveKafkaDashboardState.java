package com.gridgain.demo.siem.dashboard.live;

import com.gridgain.demo.siem.kafka.KafkaReductionProgressSnapshot;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public final class LiveKafkaDashboardState {
    private final Instant startedAt;
    private final AtomicReference<KafkaReductionProgressSnapshot> latestSnapshot;
    private volatile Instant lastUpdatedAt;

    public LiveKafkaDashboardState() {
        this.startedAt = Instant.now();
        this.lastUpdatedAt = startedAt;
        this.latestSnapshot = new AtomicReference<>(new KafkaReductionProgressSnapshot(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                Duration.ZERO,
                Map.of(),
                Map.of(),
                Map.of()
        ));
    }

    public void update(KafkaReductionProgressSnapshot snapshot) {
        latestSnapshot.set(snapshot);
        lastUpdatedAt = Instant.now();
    }

    public KafkaReductionProgressSnapshot latestSnapshot() {
        return latestSnapshot.get();
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Instant lastUpdatedAt() {
        return lastUpdatedAt;
    }
}
