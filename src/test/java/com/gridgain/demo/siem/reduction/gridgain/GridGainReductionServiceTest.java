package com.gridgain.demo.siem.reduction.gridgain;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import com.gridgain.demo.siem.reduction.inmemory.InMemoryReductionService;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GridGainReductionServiceTest {
    @Test
    void startsThreeNodesByDefaultAndMatchesInMemoryReducerCountsForDeterministicInput() {
        List<LogEvent> rawEvents = deterministicEvents();
        ReductionService inMemoryService = new InMemoryReductionService();

        ReductionResult inMemoryResult = inMemoryService.reduce(rawEvents, 40.0);
        ReductionResult gridGainResult;
        try (GridGainReductionService gridGainService = new GridGainReductionService()) {
            gridGainResult = gridGainService.reduce(rawEvents, 40.0);

            Map<String, String> proofMetrics = gridGainService.proofMetrics();
            assertEquals("gridgain / embedded Apache Ignite", gridGainService.reducerMode());
            assertEquals("siem-reduction-state", proofMetrics.get("cache name"));
            assertEquals("3", proofMetrics.get("reduction state bucket count"));
            assertEquals("3", proofMetrics.get("embedded node count"));
            assertEquals("PARTITIONED", proofMetrics.get("cache mode"));
            assertEquals("ATOMIC", proofMetrics.get("cache atomicity"));
            assertEquals("1", proofMetrics.get("backup count"));
            assertTrue(gridGainResult.preservedSecurityEventCount() > 0);
        }

        assertEquals(inMemoryResult.rawEventCount(), gridGainResult.rawEventCount());
        assertEquals(inMemoryResult.reducedEventCount(), gridGainResult.reducedEventCount());
        assertEquals(inMemoryResult.rawSecurityEventCount(), gridGainResult.rawSecurityEventCount());
        assertEquals(inMemoryResult.preservedSecurityEventCount(), gridGainResult.preservedSecurityEventCount());
    }

    @Test
    void oneNodeDebugModeUsesZeroBackups() {
        try (GridGainReductionService gridGainService = new GridGainReductionService(1)) {
            ReductionResult result = gridGainService.reduce(deterministicEvents(), 40.0);
            Map<String, String> proofMetrics = gridGainService.proofMetrics();

            assertEquals("1", proofMetrics.get("embedded node count"));
            assertEquals("PARTITIONED", proofMetrics.get("cache mode"));
            assertEquals("ATOMIC", proofMetrics.get("cache atomicity"));
            assertEquals("0", proofMetrics.get("backup count"));
            assertEquals("1 / 1", proofMetrics.get("latest primary owner nodes"));
            assertEquals("0 / 1", proofMetrics.get("latest backup owner nodes"));
            assertEquals("1 / 1", proofMetrics.get("primary owner nodes observed"));
            assertEquals("0 / 1", proofMetrics.get("backup owner nodes observed"));
            assertTrue(proofMetrics.get("primary reduction state ownership by node").contains("node-1="));
            assertEquals("node-1=0", proofMetrics.get("backup reduction state ownership by node"));
            assertEquals(result.rawSecurityEventCount(), result.preservedSecurityEventCount());
        }
    }

    @Test
    void fingerprintBucketsDistributeAcrossMoreThanOneNode() {
        try (GridGainReductionService gridGainService = new GridGainReductionService()) {
            gridGainService.reduce(distributionEvents(), 10.0);

            assertTrue(gridGainService.lastPrimaryOwnerNodeCount() > 1);
            assertTrue(gridGainService.observedPrimaryOwnerNodeCount() > 1);
            assertTrue(gridGainService.observedBackupOwnerNodeCount() > 1);
            Map<String, String> proofMetrics = gridGainService.proofMetrics();
            assertEquals("3", gridGainService.proofMetrics().get("embedded node count"));
            assertEquals("PARTITIONED", proofMetrics.get("cache mode"));
            assertEquals("ATOMIC", proofMetrics.get("cache atomicity"));
            assertEquals("1", proofMetrics.get("backup count"));
            assertTrue(ratioNumerator(proofMetrics.get("primary owner nodes observed")) > 1);
            assertTrue(ratioNumerator(proofMetrics.get("backup owner nodes observed")) > 1);
            assertTrue(proofMetrics.get("primary reduction state ownership by node").contains("node-1="));
            assertTrue(proofMetrics.get("primary reduction state ownership by node").contains("node-2="));
            assertTrue(proofMetrics.get("primary reduction state ownership by node").contains("node-3="));
            assertTrue(proofMetrics.get("backup reduction state ownership by node").contains("node-1="));
            assertTrue(proofMetrics.get("local primary entries by node").contains("node-"));
            assertTrue(proofMetrics.get("local backup entries by node").contains("node-"));
            assertTrue(Integer.parseInt(proofMetrics.get("partitions touched")) > 1);
            assertTrue(Integer.parseInt(proofMetrics.get("partitions touched observed")) > 1);
            assertTrue(proofMetrics.values().stream().noneMatch(value -> value.contains("distributed-fingerprint")));
        }
    }

    @Test
    void closesAllServiceOwnedNodes() {
        GridGainReductionService gridGainService = new GridGainReductionService();
        List<String> igniteInstanceNames = gridGainService.serviceOwnedIgniteInstanceNames();

        gridGainService.close();

        assertEquals(3, igniteInstanceNames.size());
        for (String igniteInstanceName : igniteInstanceNames) {
            assertThrows(IgniteIllegalStateException.class, () -> Ignition.ignite(igniteInstanceName));
        }
    }

    private static List<LogEvent> deterministicEvents() {
        List<LogEvent> events = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            events.add(benignEvent(LogSourceType.FIREWALL, "firewall:allow:443:portal", i));
        }
        for (int i = 0; i < 25; i++) {
            events.add(benignEvent(LogSourceType.DNS, "dns:query:A:intranet.example.internal", i));
        }
        for (int i = 0; i < 10; i++) {
            events.add(benignEvent(LogSourceType.WINDOWS_AD, "windows-ad:4624:success:WS-101", i));
        }
        for (int i = 0; i < 5; i++) {
            events.add(securityEvent(LogSourceType.CLOUD_ZERO_TRUST, "security:cloud:policy:" + i, i));
        }
        return events;
    }

    private static List<LogEvent> distributionEvents() {
        List<LogEvent> events = new ArrayList<>();
        LogSourceType[] sourceTypes = LogSourceType.values();
        for (int i = 0; i < 90; i++) {
            LogSourceType sourceType = sourceTypes[i % sourceTypes.length];
            events.add(benignEvent(sourceType, "distributed-fingerprint:" + i, i));
            events.add(benignEvent(sourceType, "distributed-fingerprint:" + i, i + 1_000));
        }
        for (int i = 0; i < 10; i++) {
            events.add(securityEvent(LogSourceType.CLOUD_ZERO_TRUST, "security:distributed:" + i, i));
        }
        return events;
    }

    private static LogEvent benignEvent(LogSourceType sourceType, String reductionKey, int index) {
        return LogEvent.observed(
                sourceType,
                Instant.parse("2026-06-22T13:00:00Z").plusMillis(index),
                "BENIGN",
                "benign payload",
                false,
                reductionKey
        );
    }

    private static LogEvent securityEvent(LogSourceType sourceType, String reductionKey, int index) {
        return LogEvent.observed(
                sourceType,
                Instant.parse("2026-06-22T13:00:00Z").plusMillis(index),
                "SECURITY",
                "security payload",
                true,
                reductionKey
        );
    }

    private static int ratioNumerator(String ratio) {
        return Integer.parseInt(ratio.split("/")[0].trim());
    }
}
