package com.gridgain.demo.siem.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompositeKafkaReductionProgressReporterTest {
    @Test
    void forwardsSnapshotsToAllReporters() {
        List<KafkaReductionProgressSnapshot> first = new ArrayList<>();
        List<KafkaReductionProgressSnapshot> second = new ArrayList<>();
        KafkaReductionProgressSnapshot snapshot = new KafkaReductionProgressSnapshot(
                10,
                10,
                6,
                0,
                2,
                2,
                1,
                0,
                Duration.ofSeconds(1),
                Map.of("raw-firewall", 10),
                Map.of("clean-firewall", 6),
                Map.of("embedded node count", "3")
        );

        new CompositeKafkaReductionProgressReporter(List.of(first::add, second::add)).report(snapshot);

        assertEquals(List.of(snapshot), first);
        assertEquals(List.of(snapshot), second);
    }
}
