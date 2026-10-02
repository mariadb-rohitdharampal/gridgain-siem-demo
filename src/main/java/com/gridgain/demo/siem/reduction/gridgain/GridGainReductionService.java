package com.gridgain.demo.siem.reduction.gridgain;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class GridGainReductionService implements ReductionService {
    private static final String REDUCTION_CACHE_NAME = "siem-reduction-state";
    private static final int DEFAULT_IGNITE_NODE_COUNT = 3;
    private static final int MAX_LOCAL_IGNITE_NODE_COUNT = 3;
    private static final int DISCOVERY_PORT_START = 47500;
    private static final int DISCOVERY_PORT_END = 47999;
    private static final Duration CLUSTER_START_TIMEOUT = Duration.ofSeconds(20);

    private final List<Ignite> nodes;
    private final Ignite ignite;
    private final IgniteCache<String, ReductionBucket> reductionCache;
    private final int backupCount;
    private int lastFingerprintBucketCount;
    private int lastEmbeddedNodeCount;
    private int lastPrimaryOwnerNodeCount;
    private IgniteOwnershipSnapshot latestOwnershipSnapshot;
    private IgniteOwnershipSnapshot observedOwnershipSnapshot;
    private boolean closed;

    public GridGainReductionService() {
        this(DEFAULT_IGNITE_NODE_COUNT);
    }

    public GridGainReductionService(int igniteNodeCount) {
        this(startEmbeddedIgniteCluster(validateIgniteNodeCount(igniteNodeCount)));
    }

    private GridGainReductionService(StartedCluster cluster) {
        this.nodes = cluster.nodes();
        this.ignite = nodes.get(0);
        this.backupCount = backupCountFor(nodes.size());
        this.reductionCache = ignite.getOrCreateCache(cacheConfiguration(backupCount));
        this.lastEmbeddedNodeCount = serverNodeCount();
        this.latestOwnershipSnapshot = IgniteOwnershipSnapshot.empty(nodeNames());
        this.observedOwnershipSnapshot = IgniteOwnershipSnapshot.empty(nodeNames());
        System.out.println("Embedded Apache Ignite started: nodes=%d, cache=%s, discovery=%s"
                .formatted(lastEmbeddedNodeCount, REDUCTION_CACHE_NAME, String.join(",", cluster.discoveryAddresses())));
    }

    @Override
    public String reducerMode() {
        return "gridgain / embedded Apache Ignite";
    }

    @Override
    public Map<String, String> proofMetrics() {
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("cache name", REDUCTION_CACHE_NAME);
        metrics.put("reduction state bucket count", Integer.toString(lastFingerprintBucketCount));
        metrics.put("embedded node count", Integer.toString(lastEmbeddedNodeCount));
        metrics.put("cache mode", CacheMode.PARTITIONED.name());
        metrics.put("cache atomicity", CacheAtomicityMode.ATOMIC.name());
        metrics.put("backup count", Integer.toString(backupCount));
        metrics.put("latest primary owner nodes", latestOwnershipSnapshot.primaryOwnerRatio());
        metrics.put("latest backup owner nodes", latestOwnershipSnapshot.backupOwnerRatio());
        metrics.put("primary owner nodes observed", observedOwnershipSnapshot.primaryOwnerRatio());
        metrics.put("backup owner nodes observed", observedOwnershipSnapshot.backupOwnerRatio());
        metrics.put("primary reduction state ownership by node", latestOwnershipSnapshot.primaryFingerprintOwnershipByNode());
        metrics.put("backup reduction state ownership by node", latestOwnershipSnapshot.backupFingerprintOwnershipByNode());
        metrics.put("observed primary reduction state ownership by node", observedOwnershipSnapshot.primaryFingerprintOwnershipByNode());
        metrics.put("observed backup reduction state ownership by node", observedOwnershipSnapshot.backupFingerprintOwnershipByNode());
        metrics.put("partitions touched", Integer.toString(latestOwnershipSnapshot.partitionsTouchedCount()));
        metrics.put("partitions touched observed", Integer.toString(observedOwnershipSnapshot.partitionsTouchedCount()));
        metrics.put("primary partitions by node", latestOwnershipSnapshot.primaryPartitionsByNode());
        metrics.put("backup partitions by node", latestOwnershipSnapshot.backupPartitionsByNode());
        metrics.put("local primary entries by node", latestOwnershipSnapshot.localPrimaryEntriesByNode());
        metrics.put("local backup entries by node", latestOwnershipSnapshot.localBackupEntriesByNode());
        return metrics;
    }

    @Override
    public ReductionResult reduce(List<LogEvent> rawEvents, double targetReductionPercentage) {
        if (targetReductionPercentage < 0.0 || targetReductionPercentage > 100.0) {
            throw new IllegalArgumentException("targetReductionPercentage must be between 0 and 100");
        }

        reductionCache.clear();

        List<LogEvent> securityEvents = new ArrayList<>();
        Map<String, List<LogEvent>> localBenignGroups = new LinkedHashMap<>();

        for (LogEvent event : rawEvents) {
            if (event.securityRelevant()) {
                securityEvents.add(event);
            } else {
                String cacheKey = cacheKey(event);
                localBenignGroups.computeIfAbsent(cacheKey, ignored -> new ArrayList<>()).add(event);
                reductionCache.invoke(
                        cacheKey,
                        new ReductionBucketUpdater(event.sourceType().name(), event.reductionKey(), event.timestamp().toEpochMilli())
                );
            }
        }

        Map<String, ReductionBucket> buckets = new LinkedHashMap<>();
        for (String cacheKey : localBenignGroups.keySet()) {
            buckets.put(cacheKey, reductionCache.get(cacheKey));
        }
        lastFingerprintBucketCount = buckets.size();
        lastEmbeddedNodeCount = serverNodeCount();
        latestOwnershipSnapshot = ownershipSnapshot(localBenignGroups.keySet());
        observedOwnershipSnapshot = observedOwnershipSnapshot.mergeObserved(latestOwnershipSnapshot);
        lastPrimaryOwnerNodeCount = latestOwnershipSnapshot.primaryOwnerNodeCount();

        long desiredRemovals = Math.round(rawEvents.size() * (targetReductionPercentage / 100.0));
        long maxSafeRemovals = buckets.values().stream()
                .mapToLong(ReductionBucket::maxRemovableEvents)
                .sum();
        long removalsToApply = Math.min(desiredRemovals, maxSafeRemovals);
        Map<String, Integer> removalsByKey = allocateRemovals(buckets, removalsToApply, maxSafeRemovals);

        List<LogEvent> reducedEvents = new ArrayList<>((int) Math.max(0, rawEvents.size() - removalsToApply));
        reducedEvents.addAll(securityEvents);
        for (Map.Entry<String, List<LogEvent>> entry : localBenignGroups.entrySet()) {
            ReductionBucket bucket = buckets.get(entry.getKey());
            int removals = removalsByKey.getOrDefault(entry.getKey(), 0);
            reducedEvents.addAll(toReducedEvents(entry.getValue(), bucket, removals));
        }

        return new ReductionResult(rawEvents, reducedEvents);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        RuntimeException failure = null;
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).close();
            } catch (RuntimeException ex) {
                if (failure == null) {
                    failure = ex;
                } else {
                    failure.addSuppressed(ex);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    int lastPrimaryOwnerNodeCount() {
        return lastPrimaryOwnerNodeCount;
    }

    int observedPrimaryOwnerNodeCount() {
        return observedOwnershipSnapshot.primaryOwnerNodeCount();
    }

    int observedBackupOwnerNodeCount() {
        return observedOwnershipSnapshot.backupOwnerNodeCount();
    }

    List<String> serviceOwnedIgniteInstanceNames() {
        return nodes.stream().map(Ignite::name).toList();
    }

    private static StartedCluster startEmbeddedIgniteCluster(int igniteNodeCount) {
        String clusterId = UUID.randomUUID().toString();
        int basePort = findAvailableBasePort(igniteNodeCount);
        List<String> discoveryAddresses = discoveryAddresses(basePort, igniteNodeCount);
        List<Ignite> startedNodes = new ArrayList<>();

        try {
            for (int nodeIndex = 0; nodeIndex < igniteNodeCount; nodeIndex++) {
                IgniteConfiguration configuration = igniteConfiguration(
                        clusterId,
                        nodeIndex,
                        basePort + nodeIndex,
                        discoveryAddresses
                );
                startedNodes.add(Ignition.start(configuration));
            }
            waitForClusterSize(startedNodes.get(0), igniteNodeCount);
            return new StartedCluster(List.copyOf(startedNodes), discoveryAddresses);
        } catch (RuntimeException | Error ex) {
            closeQuietly(startedNodes);
            throw ex;
        }
    }

    private static IgniteConfiguration igniteConfiguration(
            String clusterId,
            int nodeIndex,
            int localDiscoveryPort,
            List<String> discoveryAddresses
    ) {
        String nodeName = "gridgain-demo-" + clusterId + "-node-" + (nodeIndex + 1);
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setIgniteInstanceName(nodeName);
        configuration.setConsistentId(nodeName);
        configuration.setLocalHost("127.0.0.1");
        configuration.setClientMode(false);
        configuration.setPeerClassLoadingEnabled(false);
        configuration.setWorkDirectory(Path.of("target", "ignite-work", clusterId, "node-" + (nodeIndex + 1))
                .toAbsolutePath()
                .toString());
        configuration.setGridLogger(new NullLogger());

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(false);
        ipFinder.setAddresses(discoveryAddresses);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);
        discoverySpi.setLocalPort(localDiscoveryPort);
        discoverySpi.setLocalPortRange(0);
        configuration.setDiscoverySpi(discoverySpi);

        return configuration;
    }

    private static CacheConfiguration<String, ReductionBucket> cacheConfiguration(int backupCount) {
        CacheConfiguration<String, ReductionBucket> cacheConfiguration = new CacheConfiguration<>(REDUCTION_CACHE_NAME);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheConfiguration.setBackups(backupCount);
        return cacheConfiguration;
    }

    private int serverNodeCount() {
        return ignite.cluster().forServers().nodes().size();
    }

    private IgniteOwnershipSnapshot ownershipSnapshot(Set<String> cacheKeys) {
        Map<UUID, String> aliasesByNodeId = nodeAliasesById();
        Map<String, MutableNodeOwnership> mutableOwnership = new LinkedHashMap<>();
        for (String nodeName : aliasesByNodeId.values()) {
            mutableOwnership.put(nodeName, new MutableNodeOwnership(nodeName));
        }

        Set<Integer> partitionsTouched = new LinkedHashSet<>();
        Affinity<String> affinity = ignite.affinity(REDUCTION_CACHE_NAME);
        for (String cacheKey : cacheKeys) {
            partitionsTouched.add(affinity.partition(cacheKey));
            ClusterNode primaryNode = affinity.mapKeyToNode(cacheKey);
            if (primaryNode != null) {
                MutableNodeOwnership primaryOwnership = mutableOwnership.get(aliasesByNodeId.get(primaryNode.id()));
                if (primaryOwnership != null) {
                    primaryOwnership.primaryFingerprintCount++;
                }
            }

            for (ClusterNode ownerNode : affinity.mapKeyToPrimaryAndBackups(cacheKey)) {
                if (primaryNode != null && ownerNode.id().equals(primaryNode.id())) {
                    continue;
                }
                MutableNodeOwnership backupOwnership = mutableOwnership.get(aliasesByNodeId.get(ownerNode.id()));
                if (backupOwnership != null) {
                    backupOwnership.backupFingerprintCount++;
                }
            }
        }

        for (Ignite node : nodes) {
            ClusterNode localNode = node.cluster().localNode();
            String nodeName = aliasesByNodeId.get(localNode.id());
            MutableNodeOwnership ownership = mutableOwnership.get(nodeName);
            if (ownership == null) {
                continue;
            }

            ownership.primaryPartitionCount = affinity.primaryPartitions(localNode).length;
            ownership.backupPartitionCount = affinity.backupPartitions(localNode).length;

            IgniteCache<String, ReductionBucket> localCache = node.cache(REDUCTION_CACHE_NAME);
            if (localCache != null) {
                ownership.localPrimaryEntries = localCache.localSize(CachePeekMode.PRIMARY);
                ownership.localBackupEntries = localCache.localSize(CachePeekMode.BACKUP);
            }
        }

        return new IgniteOwnershipSnapshot(
                nodes.size(),
                cacheKeys.size(),
                partitionsTouched,
                mutableOwnership.values().stream().map(MutableNodeOwnership::toImmutable).toList()
        );
    }

    private List<String> nodeNames() {
        List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            nodeNames.add("node-" + (i + 1));
        }
        return nodeNames;
    }

    private Map<UUID, String> nodeAliasesById() {
        Map<UUID, String> aliases = new LinkedHashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            aliases.put(nodes.get(i).cluster().localNode().id(), "node-" + (i + 1));
        }
        return aliases;
    }

    private static int validateIgniteNodeCount(int igniteNodeCount) {
        if (igniteNodeCount < 1 || igniteNodeCount > MAX_LOCAL_IGNITE_NODE_COUNT) {
            throw new IllegalArgumentException("--ignite-nodes must be between 1 and " + MAX_LOCAL_IGNITE_NODE_COUNT + ".");
        }
        return igniteNodeCount;
    }

    private static int backupCountFor(int igniteNodeCount) {
        return igniteNodeCount > 1 ? 1 : 0;
    }

    private static void waitForClusterSize(Ignite coordinator, int expectedNodeCount) {
        long deadline = System.nanoTime() + CLUSTER_START_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (coordinator.cluster().forServers().nodes().size() == expectedNodeCount) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for embedded Ignite cluster startup.", ex);
            }
        }

        throw new IllegalStateException("Timed out waiting for embedded Ignite cluster to reach "
                + expectedNodeCount + " server nodes.");
    }

    private static List<String> discoveryAddresses(int basePort, int igniteNodeCount) {
        List<String> addresses = new ArrayList<>();
        for (int i = 0; i < igniteNodeCount; i++) {
            addresses.add("127.0.0.1:" + (basePort + i));
        }
        return List.copyOf(addresses);
    }

    private static int findAvailableBasePort(int igniteNodeCount) {
        for (int basePort = DISCOVERY_PORT_START; basePort <= DISCOVERY_PORT_END - igniteNodeCount + 1; basePort++) {
            if (portsAvailable(basePort, igniteNodeCount)) {
                return basePort;
            }
        }

        throw new IllegalStateException("No available local Ignite discovery port block found.");
    }

    private static boolean portsAvailable(int basePort, int igniteNodeCount) {
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            InetAddress loopback = InetAddress.getByName("127.0.0.1");
            for (int i = 0; i < igniteNodeCount; i++) {
                ServerSocket socket = new ServerSocket(basePort + i, 1, loopback);
                socket.setReuseAddress(false);
                sockets.add(socket);
            }
            return true;
        } catch (IOException ex) {
            return false;
        } finally {
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Best effort release before the Ignite nodes bind the selected ports.
                }
            }
        }
    }

    private static void closeQuietly(List<Ignite> nodes) {
        List<Ignite> reverseNodes = new ArrayList<>(nodes);
        Collections.reverse(reverseNodes);
        for (Ignite node : reverseNodes) {
            try {
                node.close();
            } catch (RuntimeException ignored) {
                // Startup is already failing; preserve the original exception.
            }
        }
    }

    private static String cacheKey(LogEvent event) {
        return event.sourceType().name() + "|" + event.reductionKey();
    }

    private static Map<String, Integer> allocateRemovals(
            Map<String, ReductionBucket> buckets,
            long removalsToApply,
            long maxSafeRemovals
    ) {
        Map<String, Integer> removalsByKey = new LinkedHashMap<>();
        if (removalsToApply == 0 || maxSafeRemovals == 0) {
            return removalsByKey;
        }

        long allocated = 0;
        for (Map.Entry<String, ReductionBucket> entry : buckets.entrySet()) {
            int groupMax = entry.getValue().maxRemovableEvents();
            int groupRemovals = (int) Math.min(groupMax, (groupMax * removalsToApply) / maxSafeRemovals);
            if (groupRemovals > 0) {
                removalsByKey.put(entry.getKey(), groupRemovals);
                allocated += groupRemovals;
            }
        }

        long remaining = removalsToApply - allocated;
        while (remaining > 0) {
            boolean madeProgress = false;
            for (Map.Entry<String, ReductionBucket> entry : buckets.entrySet()) {
                if (remaining == 0) {
                    break;
                }
                int current = removalsByKey.getOrDefault(entry.getKey(), 0);
                if (current < entry.getValue().maxRemovableEvents()) {
                    removalsByKey.put(entry.getKey(), current + 1);
                    remaining--;
                    madeProgress = true;
                }
            }
            if (!madeProgress) {
                break;
            }
        }

        return removalsByKey;
    }

    private static List<LogEvent> toReducedEvents(List<LogEvent> events, ReductionBucket bucket, int removals) {
        if (removals <= 0) {
            return List.copyOf(events);
        }

        int summarizedEventCount = removals + 1;
        int eventsToKeep = events.size() - summarizedEventCount;
        List<LogEvent> reduced = new ArrayList<>(eventsToKeep + 1);
        for (int i = 0; i < eventsToKeep; i++) {
            reduced.add(events.get(i));
        }

        reduced.add(LogEvent.summary(
                LogSourceType.valueOf(bucket.sourceTypeName()),
                events.get(eventsToKeep).timestamp(),
                bucket.lastSeen(),
                bucket.reductionKey(),
                summarizedEventCount
        ));
        return reduced;
    }

    private record StartedCluster(List<Ignite> nodes, List<String> discoveryAddresses) {
    }

    private static final class MutableNodeOwnership {
        private final String nodeName;
        private int primaryFingerprintCount;
        private int backupFingerprintCount;
        private int primaryPartitionCount;
        private int backupPartitionCount;
        private int localPrimaryEntries;
        private int localBackupEntries;

        private MutableNodeOwnership(String nodeName) {
            this.nodeName = nodeName;
        }

        private IgniteNodeOwnership toImmutable() {
            return new IgniteNodeOwnership(
                    nodeName,
                    primaryFingerprintCount,
                    backupFingerprintCount,
                    primaryPartitionCount,
                    backupPartitionCount,
                    localPrimaryEntries,
                    localBackupEntries
            );
        }
    }
}
