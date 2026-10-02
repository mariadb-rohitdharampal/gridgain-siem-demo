package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import com.gridgain.demo.siem.streaming.TimeWindow;
import com.gridgain.demo.siem.streaming.WindowAssigner;
import com.gridgain.demo.siem.streaming.WindowMetrics;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class KafkaReductionRunner {
    private final WindowAssigner windowAssigner;
    private final KafkaReductionClock clock;
    private volatile boolean stopRequested;
    private volatile KafkaRawEventConsumer activeConsumer;

    public KafkaReductionRunner(Duration windowSize) {
        this(windowSize, KafkaReductionClock.systemUtc());
    }

    KafkaReductionRunner(Duration windowSize, KafkaReductionClock clock) {
        this.windowAssigner = new WindowAssigner(windowSize);
        this.clock = clock;
    }

    public KafkaReductionResult run(
            KafkaIntegrationConfig config,
            double targetReductionPercentage,
            ReductionService reductionService
    ) {
        KafkaReductionRunOptions options = config.runSeconds() == 0
                ? KafkaReductionRunOptions.continuous(Duration.ofSeconds(10))
                : KafkaReductionRunOptions.bounded(Duration.ofSeconds(config.runSeconds()));
        return run(config, targetReductionPercentage, reductionService, options, KafkaReductionProgressReporter.noop());
    }

    public KafkaReductionResult run(
            KafkaIntegrationConfig config,
            double targetReductionPercentage,
            ReductionService reductionService,
            KafkaReductionRunOptions options,
            KafkaReductionProgressReporter progressReporter
    ) {
        return run(
                config,
                targetReductionPercentage,
                reductionService,
                options,
                progressReporter,
                new KafkaTopicAdmin(),
                new KafkaRawTopicConsumer(config),
                new KafkaCleanTopicPublisher(config)
        );
    }

    public void requestStop() {
        stopRequested = true;
        KafkaRawEventConsumer consumer = activeConsumer;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    public boolean stopRequested() {
        return stopRequested;
    }

    KafkaReductionResult run(
            KafkaIntegrationConfig config,
            double targetReductionPercentage,
            ReductionService reductionService,
            KafkaTopicProvisioner topicProvisioner,
            KafkaRawEventConsumer consumer,
            KafkaCleanEventPublisher publisher
    ) {
        return run(
                config,
                targetReductionPercentage,
                reductionService,
                KafkaReductionRunOptions.bounded(Duration.ofSeconds(config.runSeconds())),
                KafkaReductionProgressReporter.noop(),
                topicProvisioner,
                consumer,
                publisher
        );
    }

    KafkaReductionResult run(
            KafkaIntegrationConfig config,
            double targetReductionPercentage,
            ReductionService reductionService,
            KafkaReductionRunOptions options,
            KafkaReductionProgressReporter progressReporter,
            KafkaTopicProvisioner topicProvisioner,
            KafkaRawEventConsumer consumer,
            KafkaCleanEventPublisher publisher
    ) {
        if (config.createTopics()) {
            topicProvisioner.createRawAndCleanTopics(config);
        }

        Map<String, Integer> consumedRawTopicCounts = emptyRawTopicCounts(config);
        Map<String, Integer> producedCleanTopicCounts = emptyCleanTopicCounts(config);
        Map<String, KafkaReductionWindowBuffer> openWindows = new LinkedHashMap<>();
        List<WindowMetrics> windowMetrics = new ArrayList<>();
        int[] nextWindowSequence = {1};
        int malformedRecords = 0;
        long validConsumedEvents = 0;
        long securityObserved = 0;
        long securityPreserved = 0;
        boolean consumedSinceLastCommit = false;
        long startNanos = clock.nanoTime();
        long nextMetricsNanos = options.continuous()
                ? startNanos + options.metricsInterval().toNanos()
                : Long.MAX_VALUE;

        activeConsumer = consumer;
        Throwable failure = null;
        try {
            while (shouldContinue(startNanos, options)) {
                KafkaRawEventBatch batch;
                try {
                    batch = consumer.poll(Duration.ofMillis(config.pollMs()));
                } catch (WakeupException ex) {
                    if (stopRequested) {
                        break;
                    }
                    throw ex;
                }

                mergeCounts(consumedRawTopicCounts, batch.consumedTopicCounts());
                malformedRecords += batch.malformedRecordCount();
                validConsumedEvents += batch.validRecordCount();
                consumedSinceLastCommit = consumedSinceLastCommit || batch.consumedRecordCount() > 0;

                for (KafkaLogEventRecord record : batch.records()) {
                    LogEvent event = record.event();
                    KafkaReductionWindowBuffer windowBuffer = windowBufferFor(openWindows, event, nextWindowSequence);
                    windowBuffer.accept(event);
                    if (event.securityRelevant()) {
                        publisher.publish(event);
                        increment(producedCleanTopicCounts, config.cleanTopicName(event.sourceType()));
                        windowBuffer.recordSecurityPreserved();
                        securityObserved++;
                        securityPreserved++;
                    }
                }

                int closedWindows = closeExpiredWindows(
                        openWindows,
                        clock.instant(),
                        targetReductionPercentage,
                        reductionService,
                        publisher,
                        config,
                        producedCleanTopicCounts,
                        windowMetrics
                );

                if (batch.consumedRecordCount() > 0 || batch.malformedRecordCount() > 0 || closedWindows > 0) {
                    boolean flushed = flushPublisher(publisher);
                    if (flushed && consumedSinceLastCommit && openWindows.isEmpty()) {
                        if (commitConsumer(consumer)) {
                            consumedSinceLastCommit = false;
                        }
                    }
                }

                if (options.continuous() && clock.nanoTime() >= nextMetricsNanos) {
                    progressReporter.report(snapshot(
                            consumedRawTopicCounts,
                            producedCleanTopicCounts,
                            validConsumedEvents,
                            malformedRecords,
                            securityObserved,
                            securityPreserved,
                            windowMetrics,
                            openWindows,
                            startNanos,
                            reductionService
                    ));
                    while (clock.nanoTime() >= nextMetricsNanos) {
                        nextMetricsNanos += options.metricsInterval().toNanos();
                    }
                }
            }

            if (!openWindows.isEmpty()) {
                closeAllWindows(
                        openWindows,
                        targetReductionPercentage,
                        reductionService,
                        publisher,
                        config,
                        producedCleanTopicCounts,
                        windowMetrics
                );
                boolean flushed = flushPublisher(publisher);
                if (flushed && consumedSinceLastCommit) {
                    if (commitConsumer(consumer)) {
                        consumedSinceLastCommit = false;
                    }
                }
            } else if (consumedSinceLastCommit) {
                boolean flushed = flushPublisher(publisher);
                if (flushed && commitConsumer(consumer)) {
                    consumedSinceLastCommit = false;
                }
            }
        } catch (RuntimeException | Error ex) {
            failure = ex;
            throw ex;
        } finally {
            activeConsumer = null;
            Throwable closeFailure = null;
            closeFailure = closeResource(publisher, failure, closeFailure);
            closeFailure = closeResource(consumer, failure, closeFailure);
            if (failure == null && closeFailure != null) {
                throwUnchecked(closeFailure);
            }
        }

        Duration processingTime = Duration.ofNanos(clock.nanoTime() - startNanos);
        return new KafkaReductionResult(
                consumedRawTopicCounts,
                producedCleanTopicCounts,
                validConsumedEvents,
                malformedRecords,
                securityObserved,
                securityPreserved,
                processingTime,
                reductionService.reducerMode(),
                reductionService.proofMetrics(),
                windowMetrics
        );
    }

    private boolean shouldContinue(long startNanos, KafkaReductionRunOptions options) {
        if (stopRequested) {
            return false;
        }
        if (options.continuous()) {
            return true;
        }
        return clock.nanoTime() < startNanos + options.runDuration().toNanos();
    }

    private boolean flushPublisher(KafkaCleanEventPublisher publisher) {
        try {
            publisher.flush();
            return true;
        } catch (WakeupException ex) {
            if (stopRequested) {
                return false;
            }
            throw ex;
        } catch (RuntimeException ex) {
            preserveInterruptFlagIfNeeded(ex);
            throw ex;
        }
    }

    private boolean commitConsumer(KafkaRawEventConsumer consumer) {
        try {
            consumer.commit();
            return true;
        } catch (WakeupException ex) {
            if (stopRequested) {
                return false;
            }
            throw ex;
        } catch (RuntimeException ex) {
            preserveInterruptFlagIfNeeded(ex);
            throw ex;
        }
    }

    private Throwable closeResource(AutoCloseable resource, Throwable primaryFailure, Throwable closeFailure) {
        try {
            resource.close();
            return closeFailure;
        } catch (WakeupException ex) {
            if (stopRequested) {
                return closeFailure;
            }
            return recordCloseFailure(ex, primaryFailure, closeFailure);
        } catch (Exception ex) {
            preserveInterruptFlagIfNeeded(ex);
            return recordCloseFailure(ex, primaryFailure, closeFailure);
        }
    }

    private static Throwable recordCloseFailure(Throwable ex, Throwable primaryFailure, Throwable closeFailure) {
        if (primaryFailure != null) {
            primaryFailure.addSuppressed(ex);
            return closeFailure;
        }
        if (closeFailure != null) {
            closeFailure.addSuppressed(ex);
            return closeFailure;
        }
        return ex;
    }

    private static void preserveInterruptFlagIfNeeded(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
            current = current.getCause();
        }
    }

    private static void throwUnchecked(Throwable ex) {
        if (ex instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (ex instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException(ex);
    }

    private KafkaReductionWindowBuffer windowBufferFor(
            Map<String, KafkaReductionWindowBuffer> openWindows,
            LogEvent event,
            int[] nextWindowSequence
    ) {
        TimeWindow range = windowAssigner.assign(event.timestamp(), 1);
        String key = windowKey(range);
        KafkaReductionWindowBuffer existing = openWindows.get(key);
        if (existing != null) {
            return existing;
        }

        TimeWindow sequencedWindow = new TimeWindow(nextWindowSequence[0]++, range.startInclusive(), range.endExclusive());
        KafkaReductionWindowBuffer created = new KafkaReductionWindowBuffer(sequencedWindow);
        openWindows.put(key, created);
        return created;
    }

    private static String windowKey(TimeWindow window) {
        return window.startInclusive() + "|" + window.endExclusive();
    }

    private int closeExpiredWindows(
            Map<String, KafkaReductionWindowBuffer> openWindows,
            Instant now,
            double targetReductionPercentage,
            ReductionService reductionService,
            KafkaCleanEventPublisher publisher,
            KafkaIntegrationConfig config,
            Map<String, Integer> producedCleanTopicCounts,
            List<WindowMetrics> windowMetrics
    ) {
        List<String> expiredKeys = openWindows.entrySet()
                .stream()
                .filter(entry -> !entry.getValue().window().endExclusive().isAfter(now))
                .sorted(Comparator.comparing(entry -> entry.getValue().window().startInclusive()))
                .map(Map.Entry::getKey)
                .toList();

        for (String key : expiredKeys) {
            closeWindow(
                    openWindows.remove(key),
                    targetReductionPercentage,
                    reductionService,
                    publisher,
                    config,
                    producedCleanTopicCounts,
                    windowMetrics
            );
        }

        return expiredKeys.size();
    }

    private static void closeAllWindows(
            Map<String, KafkaReductionWindowBuffer> openWindows,
            double targetReductionPercentage,
            ReductionService reductionService,
            KafkaCleanEventPublisher publisher,
            KafkaIntegrationConfig config,
            Map<String, Integer> producedCleanTopicCounts,
            List<WindowMetrics> windowMetrics
    ) {
        List<KafkaReductionWindowBuffer> windows = openWindows.values()
                .stream()
                .sorted(Comparator.comparing(buffer -> buffer.window().startInclusive()))
                .toList();
        openWindows.clear();
        for (KafkaReductionWindowBuffer window : windows) {
            closeWindow(
                    window,
                    targetReductionPercentage,
                    reductionService,
                    publisher,
                    config,
                    producedCleanTopicCounts,
                    windowMetrics
            );
        }
    }

    private static void closeWindow(
            KafkaReductionWindowBuffer window,
            double targetReductionPercentage,
            ReductionService reductionService,
            KafkaCleanEventPublisher publisher,
            KafkaIntegrationConfig config,
            Map<String, Integer> producedCleanTopicCounts,
            List<WindowMetrics> windowMetrics
    ) {
        List<LogEvent> reducedBenignEvents = List.of();
        if (!window.benignEvents().isEmpty()) {
            ReductionResult reductionResult = reductionService.reduce(
                    window.benignEvents(),
                    effectiveBenignTarget(window.rawEvents(), window.benignEventCount(), targetReductionPercentage)
            );
            reducedBenignEvents = reductionResult.reducedEvents();
        }

        for (LogEvent reducedEvent : reducedBenignEvents) {
            publisher.publish(reducedEvent);
            increment(producedCleanTopicCounts, config.cleanTopicName(reducedEvent.sourceType()));
        }

        windowMetrics.add(WindowMetrics.from(
                window.window(),
                window.rawEvents(),
                window.securityEventsPreserved() + reducedBenignEvents.size(),
                window.securityEventsObserved(),
                window.securityEventsPreserved()
        ));
    }

    private static double effectiveBenignTarget(long rawWindowEvents, long benignWindowEvents, double targetReductionPercentage) {
        if (benignWindowEvents == 0) {
            return 0.0;
        }

        long desiredWindowRemovals = Math.round(rawWindowEvents * (targetReductionPercentage / 100.0));
        double target = (desiredWindowRemovals * 100.0) / benignWindowEvents;
        return Math.max(0.0, Math.min(100.0, target));
    }

    private static Map<String, Integer> emptyRawTopicCounts(KafkaIntegrationConfig config) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (LogSourceType sourceType : LogSourceType.values()) {
            counts.put(config.rawTopicName(sourceType), 0);
        }
        return counts;
    }

    private static Map<String, Integer> emptyCleanTopicCounts(KafkaIntegrationConfig config) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (LogSourceType sourceType : LogSourceType.values()) {
            counts.put(config.cleanTopicName(sourceType), 0);
        }
        return counts;
    }

    private static void mergeCounts(Map<String, Integer> target, Map<String, Integer> source) {
        source.forEach((topic, count) -> target.merge(topic, count, Integer::sum));
    }

    private static void increment(Map<String, Integer> counts, String topicName) {
        counts.merge(topicName, 1, Integer::sum);
    }

    private KafkaReductionProgressSnapshot snapshot(
            Map<String, Integer> consumedRawTopicCounts,
            Map<String, Integer> producedCleanTopicCounts,
            long validConsumedEvents,
            int malformedRecords,
            long securityObserved,
            long securityPreserved,
            List<WindowMetrics> windowMetrics,
            Map<String, KafkaReductionWindowBuffer> openWindows,
            long startNanos,
            ReductionService reductionService
    ) {
        return new KafkaReductionProgressSnapshot(
                sumCounts(consumedRawTopicCounts),
                validConsumedEvents,
                sumCounts(producedCleanTopicCounts),
                malformedRecords,
                securityObserved,
                securityPreserved,
                windowMetrics.size(),
                openWindows.size(),
                Duration.ofNanos(clock.nanoTime() - startNanos),
                consumedRawTopicCounts,
                producedCleanTopicCounts,
                reductionService.proofMetrics()
        );
    }

    private static long sumCounts(Map<String, Integer> counts) {
        return counts.values().stream().mapToLong(Integer::longValue).sum();
    }
}
