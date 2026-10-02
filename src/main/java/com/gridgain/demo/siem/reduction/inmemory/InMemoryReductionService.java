package com.gridgain.demo.siem.reduction.inmemory;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMemoryReductionService implements ReductionService {
    @Override
    public String reducerMode() {
        return "inmemory";
    }

    @Override
    public ReductionResult reduce(List<LogEvent> rawEvents, double targetReductionPercentage) {
        if (targetReductionPercentage < 0.0 || targetReductionPercentage > 100.0) {
            throw new IllegalArgumentException("targetReductionPercentage must be between 0 and 100");
        }

        List<LogEvent> securityEvents = new ArrayList<>();
        Map<BenignFingerprint, BenignGroup> benignGroups = new LinkedHashMap<>();

        for (LogEvent event : rawEvents) {
            if (event.securityRelevant()) {
                securityEvents.add(event);
            } else {
                BenignFingerprint fingerprint = new BenignFingerprint(event.sourceType().name(), event.reductionKey());
                benignGroups.computeIfAbsent(fingerprint, ignored -> new BenignGroup(event)).add(event);
            }
        }

        long desiredRemovals = Math.round(rawEvents.size() * (targetReductionPercentage / 100.0));
        long maxSafeRemovals = benignGroups.values().stream()
                .mapToLong(BenignGroup::maxRemovableEvents)
                .sum();
        long removalsToApply = Math.min(desiredRemovals, maxSafeRemovals);
        Map<BenignGroup, Integer> removalsByGroup = allocateRemovals(benignGroups, removalsToApply, maxSafeRemovals);

        List<LogEvent> reducedEvents = new ArrayList<>((int) Math.max(0, rawEvents.size() - removalsToApply));
        reducedEvents.addAll(securityEvents);
        for (BenignGroup group : benignGroups.values()) {
            reducedEvents.addAll(group.toReducedEvents(removalsByGroup.getOrDefault(group, 0)));
        }

        return new ReductionResult(rawEvents, reducedEvents);
    }

    private Map<BenignGroup, Integer> allocateRemovals(
            Map<BenignFingerprint, BenignGroup> benignGroups,
            long removalsToApply,
            long maxSafeRemovals
    ) {
        Map<BenignGroup, Integer> removalsByGroup = new LinkedHashMap<>();
        if (removalsToApply == 0 || maxSafeRemovals == 0) {
            return removalsByGroup;
        }

        long allocated = 0;
        for (BenignGroup group : benignGroups.values()) {
            int groupMax = group.maxRemovableEvents();
            int groupRemovals = (int) Math.min(groupMax, (groupMax * removalsToApply) / maxSafeRemovals);
            if (groupRemovals > 0) {
                removalsByGroup.put(group, groupRemovals);
                allocated += groupRemovals;
            }
        }

        long remaining = removalsToApply - allocated;
        while (remaining > 0) {
            boolean madeProgress = false;
            for (BenignGroup group : benignGroups.values()) {
                if (remaining == 0) {
                    break;
                }
                int current = removalsByGroup.getOrDefault(group, 0);
                if (current < group.maxRemovableEvents()) {
                    removalsByGroup.put(group, current + 1);
                    remaining--;
                    madeProgress = true;
                }
            }
            if (!madeProgress) {
                break;
            }
        }

        return removalsByGroup;
    }

    private record BenignFingerprint(String sourceTypeName, String reductionKey) {
    }

    private static final class BenignGroup {
        private final List<LogEvent> events = new ArrayList<>();
        private Instant lastSeen;

        private BenignGroup(LogEvent firstEvent) {
            this.lastSeen = firstEvent.timestamp();
        }

        private void add(LogEvent event) {
            events.add(event);
            if (event.timestamp().isAfter(lastSeen)) {
                lastSeen = event.timestamp();
            }
        }

        private int maxRemovableEvents() {
            return Math.max(0, events.size() - 1);
        }

        private List<LogEvent> toReducedEvents(int removals) {
            if (removals <= 0) {
                return List.copyOf(events);
            }

            int summarizedEventCount = removals + 1;
            int eventsToKeep = events.size() - summarizedEventCount;
            List<LogEvent> reduced = new ArrayList<>(eventsToKeep + 1);
            for (int i = 0; i < eventsToKeep; i++) {
                reduced.add(events.get(i));
            }

            LogEvent firstSummarizedEvent = events.get(eventsToKeep);
            reduced.add(LogEvent.summary(
                    firstSummarizedEvent.sourceType(),
                    firstSummarizedEvent.timestamp(),
                    lastSeen,
                    firstSummarizedEvent.reductionKey(),
                    summarizedEventCount
            ));
            return reduced;
        }
    }
}
