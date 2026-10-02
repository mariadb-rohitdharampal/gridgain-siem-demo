package com.gridgain.demo.siem.kafka;

import java.io.PrintStream;

public final class ConsoleKafkaReductionProgressReporter implements KafkaReductionProgressReporter {
    private final PrintStream out;

    public ConsoleKafkaReductionProgressReporter() {
        this(System.out);
    }

    ConsoleKafkaReductionProgressReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(KafkaReductionProgressSnapshot snapshot) {
        out.println("Kafka reduce progress: recordsSeen=%d, valid=%d, clean=%d, reduction=%.2f%%, malformed=%d, security=%d/%d, windows=%d, openWindows=%d, primaryOwnerNodes=%s"
                .formatted(
                        snapshot.recordsSeen(),
                        snapshot.validConsumedEvents(),
                        snapshot.producedCleanEvents(),
                        snapshot.reductionPercentage(),
                        snapshot.malformedRecords(),
                        snapshot.securityEventsPreserved(),
                        snapshot.securityEventsObserved(),
                        snapshot.windowsProcessed(),
                        snapshot.openWindows(),
                        snapshot.primaryOwnerNodesObserved()
                ));
    }
}
