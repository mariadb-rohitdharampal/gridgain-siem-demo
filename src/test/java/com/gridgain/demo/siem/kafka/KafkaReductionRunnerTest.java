package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaReductionRunnerTest {
    @Test
    void securityEventsBypassReductionAndPublishImmediately() {
        LogEvent event = event(LogSourceType.FIREWALL, true, "security:firewall:block:203.0.113.10");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        List<String> actions = new ArrayList<>();
        RecordingConsumer consumer = new RecordingConsumer(clock, actions, batch("raw-firewall", event));
        RecordingPublisher publisher = new RecordingPublisher(actions);
        ThrowingReductionService reductionService = new ThrowingReductionService();

        KafkaReductionResult result = new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, reductionService, new RecordingProvisioner(), consumer, publisher);

        assertEquals(List.of(event), publisher.events);
        assertEquals(1, result.consumedRawTopicCounts().get("raw-firewall"));
        assertEquals(1, result.producedCleanTopicCounts().get("clean-firewall"));
        assertEquals(1, result.securityEventsObserved());
        assertEquals(1, result.securityEventsPreserved());
        assertEquals(1, result.windowsProcessed());
        assertEquals(1, consumer.commits);
        assertTrue(actions.contains("flush"));
        assertTrue(actions.contains("commit"));
        assertTrue(actions.indexOf("flush") < actions.indexOf("commit"));
    }

    @Test
    void benignEventsSummarizeWhenWindowCloses() {
        LogEvent first = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        LogEvent second = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        LogEvent third = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        RecordingConsumer consumer = new RecordingConsumer(clock, batch("raw-dns", first, second, third));
        RecordingPublisher publisher = new RecordingPublisher();

        KafkaReductionResult result = new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, new SummaryReductionService(), new RecordingProvisioner(), consumer, publisher);

        assertEquals(3, result.consumedRawTopicCounts().get("raw-dns"));
        assertEquals(1, result.producedCleanTopicCounts().get("clean-dns"));
        assertEquals(1, publisher.events.size());
        assertEquals("REDUCTION_SUMMARY", publisher.events.get(0).eventType());
        assertEquals(3, publisher.events.get(0).representedEventCount());
        assertEquals(1, result.windowsProcessed());
        assertEquals(66.66666666666667, result.reductionPercentage());
    }

    @Test
    void malformedRecordsAreCountedAndSkipped() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        KafkaRawEventBatch malformedBatch = new KafkaRawEventBatch(
                List.of(),
                Map.of("raw-dns", 1),
                1
        );
        RecordingConsumer consumer = new RecordingConsumer(clock, malformedBatch);
        RecordingPublisher publisher = new RecordingPublisher();

        KafkaReductionResult result = new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, new SummaryReductionService(), new RecordingProvisioner(), consumer, publisher);

        assertEquals(1, result.consumedRawTopicCounts().get("raw-dns"));
        assertEquals(1, result.malformedRecordCount());
        assertEquals(1, result.totalKafkaRecordsSeen());
        assertEquals(0, result.consumedEvents());
        assertEquals(0, result.producedEvents());
        assertEquals(0.0, result.reductionPercentage());
        assertEquals(1, consumer.commits);
    }

    @Test
    void commitDoesNotHappenWhenPublishFailsBeforeFlush() {
        LogEvent event = event(LogSourceType.WINDOWS_AD, true, "security:ad:group-change:admin");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        RecordingConsumer consumer = new RecordingConsumer(clock, batch("raw-windows-ad", event));
        FailingPublisher publisher = new FailingPublisher();

        assertThrows(IllegalStateException.class, () -> new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, new SummaryReductionService(), new RecordingProvisioner(), consumer, publisher));

        assertEquals(0, consumer.commits);
        assertEquals(0, publisher.flushes);
    }

    @Test
    void topicMetricsTrackConsumedRawAndProducedCleanCounts() {
        LogEvent security = event(LogSourceType.CLOUD_ZERO_TRUST, true, "security:cloud:policy-deny:user");
        LogEvent benign = event(LogSourceType.CLOUD_ZERO_TRUST, false, "cloud-zt:allow:mission-dashboard");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        RecordingConsumer consumer = new RecordingConsumer(clock, batch("raw-cloud-zero-trust", security, benign));
        RecordingPublisher publisher = new RecordingPublisher();

        KafkaReductionResult result = new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, new SummaryReductionService(), new RecordingProvisioner(), consumer, publisher);

        assertEquals(2, result.consumedRawTopicCounts().get("raw-cloud-zero-trust"));
        assertEquals(2, result.producedCleanTopicCounts().get("clean-cloud-zero-trust"));
        assertEquals(1, result.securityEventsObserved());
        assertEquals(1, result.securityEventsPreserved());
    }

    @Test
    void boundedModeStillExitsAfterConfiguredDuration() {
        LogEvent first = event(LogSourceType.DNS, true, "security:dns:first");
        LogEvent second = event(LogSourceType.DNS, true, "security:dns:second");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        RecordingConsumer consumer = new RecordingConsumer(
                clock,
                batch("raw-dns", first),
                batch("raw-dns", second)
        );
        RecordingPublisher publisher = new RecordingPublisher();

        KafkaReductionResult result = new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, new ThrowingReductionService(), new RecordingProvisioner(), consumer, publisher);

        assertEquals(1, result.consumedRawTopicCounts().get("raw-dns"));
        assertEquals(List.of(first), publisher.events);
    }

    @Test
    void continuousModeStopsOnRequestAndClosesOpenWindowsBeforeCommit() {
        LogEvent first = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        LogEvent second = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        KafkaReductionRunner runner = new KafkaReductionRunner(Duration.ofSeconds(60), clock);
        List<String> actions = new ArrayList<>();
        RecordingConsumer consumer = new RecordingConsumer(
                clock,
                actions,
                runner::requestStop,
                batch("raw-dns", first, second)
        );
        RecordingPublisher publisher = new RecordingPublisher(actions);

        KafkaReductionResult result = runner.run(
                config(),
                40.0,
                new SummaryReductionService(),
                KafkaReductionRunOptions.continuous(Duration.ofSeconds(1)),
                KafkaReductionProgressReporter.noop(),
                new RecordingProvisioner(),
                consumer,
                publisher
        );

        assertEquals(1, consumer.wakeups);
        assertEquals(1, consumer.commits);
        assertEquals(1, result.windowsProcessed());
        assertEquals(1, publisher.events.size());
        assertEquals("REDUCTION_SUMMARY", publisher.events.get(0).eventType());
        assertTrue(actions.contains("wakeup"));
        assertTrue(actions.contains("flush"));
        assertTrue(actions.contains("commit"));
        assertTrue(actions.indexOf("flush") < actions.indexOf("commit"));
    }

    @Test
    void stopRequestPathHandlesWakeupExceptionWithoutSurfacing() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        KafkaReductionRunner runner = new KafkaReductionRunner(Duration.ofSeconds(60), clock);
        StopThenWakeupConsumer consumer = new StopThenWakeupConsumer(clock, runner::requestStop);
        RecordingPublisher publisher = new RecordingPublisher();

        KafkaReductionResult result = runner.run(
                config(),
                40.0,
                new SummaryReductionService(),
                KafkaReductionRunOptions.continuous(Duration.ofSeconds(1)),
                KafkaReductionProgressReporter.noop(),
                new RecordingProvisioner(),
                consumer,
                publisher
        );

        assertTrue(runner.stopRequested());
        assertEquals(1, consumer.wakeups);
        assertEquals(0, result.totalKafkaRecordsSeen());
        assertEquals(0, result.producedEvents());
    }

    @Test
    void wakeupDuringFinalCommitAndCloseIsSuppressedDuringShutdown() {
        LogEvent first = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        LogEvent second = event(LogSourceType.DNS, false, "dns:query:A:intranet.example.internal");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        KafkaReductionRunner runner = new KafkaReductionRunner(Duration.ofSeconds(60), clock);
        CommitCloseWakeupConsumer consumer = new CommitCloseWakeupConsumer(
                clock,
                runner::requestStop,
                batch("raw-dns", first, second)
        );
        RecordingPublisher publisher = new RecordingPublisher();

        KafkaReductionResult result = runner.run(
                config(),
                40.0,
                new SummaryReductionService(),
                KafkaReductionRunOptions.continuous(Duration.ofSeconds(1)),
                KafkaReductionProgressReporter.noop(),
                new RecordingProvisioner(),
                consumer,
                publisher
        );

        assertEquals(1, consumer.wakeups);
        assertEquals(1, consumer.commitAttempts);
        assertEquals(1, consumer.closeAttempts);
        assertEquals(1, result.windowsProcessed());
        assertEquals(1, publisher.events.size());
    }

    @Test
    void unexpectedWakeupWithoutStopRequestStillFails() {
        LogEvent event = event(LogSourceType.FIREWALL, true, "security:firewall:block:203.0.113.10");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        CommitCloseWakeupConsumer consumer = new CommitCloseWakeupConsumer(
                clock,
                () -> {
                },
                batch("raw-firewall", event)
        );
        RecordingPublisher publisher = new RecordingPublisher();

        assertThrows(WakeupException.class, () -> new KafkaReductionRunner(Duration.ofSeconds(60), clock)
                .run(config(), 40.0, new SummaryReductionService(), new RecordingProvisioner(), consumer, publisher));
    }

    @Test
    void periodicReporterReceivesSnapshotsInContinuousMode() {
        LogEvent event = event(LogSourceType.FIREWALL, true, "security:firewall:block:203.0.113.10");
        MutableClock clock = new MutableClock(Instant.parse("2026-06-22T13:00:00Z"));
        KafkaReductionRunner runner = new KafkaReductionRunner(Duration.ofSeconds(60), clock);
        RecordingConsumer consumer = new RecordingConsumer(clock, runner::requestStop, batch("raw-firewall", event));
        RecordingPublisher publisher = new RecordingPublisher();
        List<KafkaReductionProgressSnapshot> snapshots = new ArrayList<>();

        runner.run(
                config(),
                40.0,
                new SummaryReductionService(),
                KafkaReductionRunOptions.continuous(Duration.ofSeconds(1)),
                snapshots::add,
                new RecordingProvisioner(),
                consumer,
                publisher
        );

        assertEquals(1, snapshots.size());
        KafkaReductionProgressSnapshot snapshot = snapshots.get(0);
        assertEquals(1, snapshot.recordsSeen());
        assertEquals(1, snapshot.validConsumedEvents());
        assertEquals(1, snapshot.producedCleanEvents());
        assertEquals(1, snapshot.consumedRawTopicCounts().get("raw-firewall"));
        assertEquals(1, snapshot.producedCleanTopicCounts().get("clean-firewall"));
        assertEquals("3", snapshot.proofMetrics().get("embedded node count"));
    }

    @Test
    void progressReporterDoesNotPrintVerboseWindowDetails() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ConsoleKafkaReductionProgressReporter reporter = new ConsoleKafkaReductionProgressReporter(
                new PrintStream(output, true, StandardCharsets.UTF_8)
        );

        reporter.report(new KafkaReductionProgressSnapshot(
                10,
                10,
                6,
                0,
                2,
                2,
                4,
                1,
                Duration.ofSeconds(5),
                Map.of("primary owner nodes observed", "3 / 3")
        ));

        String report = output.toString(StandardCharsets.UTF_8);
        assertTrue(report.contains("Kafka reduce progress: recordsSeen=10"));
        assertTrue(report.contains("primaryOwnerNodes=3 / 3"));
        assertTrue(report.contains("windows=4"));
        assertTrue(report.contains("openWindows=1"));
        assertTrue(!report.contains("Top 5 windows"));
        assertTrue(!report.contains("- window "));
    }

    private static KafkaIntegrationConfig config() {
        KafkaIntegrationConfig defaults = KafkaIntegrationConfig.defaults();
        return new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                100,
                1,
                false,
                defaults.rawTopicNames(),
                defaults.cleanTopicNames(),
                defaults.topicPartitions(),
                defaults.topicReplicationFactor()
        );
    }

    private static KafkaRawEventBatch batch(String topicName, LogEvent... events) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put(topicName, events.length);
        List<KafkaLogEventRecord> records = new ArrayList<>();
        for (int i = 0; i < events.length; i++) {
            records.add(new KafkaLogEventRecord(topicName, "key-" + i, 0, i, events[i]));
        }
        return new KafkaRawEventBatch(records, counts, 0);
    }

    private static LogEvent event(LogSourceType sourceType, boolean securityRelevant, String reductionKey) {
        return LogEvent.observed(
                sourceType,
                Instant.parse("2026-06-22T13:00:00Z"),
                securityRelevant ? "SECURITY_EVENT" : "BENIGN_EVENT",
                "payload",
                securityRelevant,
                reductionKey
        );
    }

    private static final class MutableClock implements KafkaReductionClock {
        private Instant instant;
        private long nanoTime;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        @Override
        public long nanoTime() {
            return nanoTime;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
            nanoTime += duration.toNanos();
        }
    }

    private static final class RecordingConsumer implements KafkaRawEventConsumer {
        private final MutableClock clock;
        private final List<KafkaRawEventBatch> batches;
        private final List<String> actions = new ArrayList<>();
        private final Runnable afterPoll;
        private int index;
        private int commits;
        private int wakeups;

        private RecordingConsumer(MutableClock clock, KafkaRawEventBatch... batches) {
            this(clock, () -> {
            }, batches);
        }

        private RecordingConsumer(MutableClock clock, Runnable afterPoll, KafkaRawEventBatch... batches) {
            this.clock = clock;
            this.afterPoll = afterPoll;
            this.batches = List.of(batches);
        }

        private RecordingConsumer(MutableClock clock, List<String> actions, KafkaRawEventBatch... batches) {
            this(clock, actions, () -> {
            }, batches);
        }

        private RecordingConsumer(
                MutableClock clock,
                List<String> actions,
                Runnable afterPoll,
                KafkaRawEventBatch... batches
        ) {
            this.clock = clock;
            this.afterPoll = afterPoll;
            this.batches = List.of(batches);
            this.actions.addAll(actions);
            actions.clear();
            this.sharedActions = actions;
        }

        private List<String> sharedActions = actions;

        @Override
        public KafkaRawEventBatch poll(Duration timeout) {
            clock.advance(Duration.ofSeconds(2));
            if (index < batches.size()) {
                KafkaRawEventBatch batch = batches.get(index++);
                afterPoll.run();
                return batch;
            }
            return KafkaRawEventBatch.empty();
        }

        @Override
        public void commit() {
            sharedActions.add("commit");
            commits++;
        }

        @Override
        public void wakeup() {
            sharedActions.add("wakeup");
            wakeups++;
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingPublisher implements KafkaCleanEventPublisher {
        private final List<LogEvent> events = new ArrayList<>();
        private final List<String> actions = new ArrayList<>();

        private RecordingPublisher() {
        }

        private RecordingPublisher(List<String> actions) {
            this.actions.addAll(actions);
            actions.clear();
            this.sharedActions = actions;
        }

        private List<String> sharedActions = actions;

        @Override
        public void publish(LogEvent event) {
            sharedActions.add("publish");
            events.add(event);
        }

        @Override
        public void flush() {
            sharedActions.add("flush");
        }

        @Override
        public void close() {
        }
    }

    private static final class StopThenWakeupConsumer implements KafkaRawEventConsumer {
        private final MutableClock clock;
        private final Runnable stopRequest;
        private int wakeups;

        private StopThenWakeupConsumer(MutableClock clock, Runnable stopRequest) {
            this.clock = clock;
            this.stopRequest = stopRequest;
        }

        @Override
        public KafkaRawEventBatch poll(Duration timeout) {
            clock.advance(Duration.ofSeconds(1));
            stopRequest.run();
            throw new WakeupException();
        }

        @Override
        public void commit() {
            throw new AssertionError("No commit should be attempted without consumed records.");
        }

        @Override
        public void wakeup() {
            wakeups++;
        }

        @Override
        public void close() {
        }
    }

    private static final class CommitCloseWakeupConsumer implements KafkaRawEventConsumer {
        private final MutableClock clock;
        private final Runnable afterPoll;
        private final KafkaRawEventBatch batch;
        private boolean polled;
        private int wakeups;
        private int commitAttempts;
        private int closeAttempts;

        private CommitCloseWakeupConsumer(MutableClock clock, Runnable afterPoll, KafkaRawEventBatch batch) {
            this.clock = clock;
            this.afterPoll = afterPoll;
            this.batch = batch;
        }

        @Override
        public KafkaRawEventBatch poll(Duration timeout) {
            clock.advance(Duration.ofSeconds(1));
            if (polled) {
                return KafkaRawEventBatch.empty();
            }
            polled = true;
            afterPoll.run();
            return batch;
        }

        @Override
        public void commit() {
            commitAttempts++;
            throw new WakeupException();
        }

        @Override
        public void wakeup() {
            wakeups++;
        }

        @Override
        public void close() {
            closeAttempts++;
            throw new WakeupException();
        }
    }

    private static final class FailingPublisher implements KafkaCleanEventPublisher {
        private int flushes;

        @Override
        public void publish(LogEvent event) {
            throw new IllegalStateException("publish failed");
        }

        @Override
        public void flush() {
            flushes++;
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingProvisioner implements KafkaTopicProvisioner {
        @Override
        public void createRawTopics(KafkaIntegrationConfig config) {
        }

        @Override
        public void createRawAndCleanTopics(KafkaIntegrationConfig config) {
        }
    }

    private static final class ThrowingReductionService implements ReductionService {
        @Override
        public ReductionResult reduce(List<LogEvent> rawEvents, double targetReductionPercentage) {
            throw new AssertionError("security events should bypass reduction");
        }
    }

    private static final class SummaryReductionService implements ReductionService {
        @Override
        public ReductionResult reduce(List<LogEvent> rawEvents, double targetReductionPercentage) {
            if (rawEvents.isEmpty()) {
                return new ReductionResult(rawEvents, List.of());
            }
            LogEvent first = rawEvents.get(0);
            return new ReductionResult(rawEvents, List.of(LogEvent.summary(
                    first.sourceType(),
                    first.timestamp(),
                    first.reductionKey(),
                    rawEvents.size()
            )));
        }

        @Override
        public String reducerMode() {
            return "test";
        }

        @Override
        public Map<String, String> proofMetrics() {
            return Map.of("embedded node count", "3");
        }
    }
}
