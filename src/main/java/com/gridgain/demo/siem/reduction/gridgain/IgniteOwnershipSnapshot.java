package com.gridgain.demo.siem.reduction.gridgain;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

record IgniteOwnershipSnapshot(
        int totalNodeCount,
        int fingerprintBucketCount,
        Set<Integer> partitionsTouched,
        List<IgniteNodeOwnership> nodeOwnership
) {
    IgniteOwnershipSnapshot {
        partitionsTouched = Set.copyOf(partitionsTouched);
        nodeOwnership = List.copyOf(nodeOwnership);
    }

    static IgniteOwnershipSnapshot empty(List<String> nodeNames) {
        List<IgniteNodeOwnership> ownership = new ArrayList<>();
        for (String nodeName : nodeNames) {
            ownership.add(new IgniteNodeOwnership(nodeName, 0, 0, 0, 0, 0, 0));
        }
        return new IgniteOwnershipSnapshot(nodeNames.size(), 0, Set.of(), ownership);
    }

    IgniteOwnershipSnapshot mergeObserved(IgniteOwnershipSnapshot latest) {
        List<IgniteNodeOwnership> mergedOwnership = new ArrayList<>();
        for (int i = 0; i < nodeOwnership.size(); i++) {
            mergedOwnership.add(nodeOwnership.get(i).mergeObserved(latest.nodeOwnership.get(i)));
        }

        Set<Integer> mergedPartitions = new LinkedHashSet<>(partitionsTouched);
        mergedPartitions.addAll(latest.partitionsTouched);

        return new IgniteOwnershipSnapshot(
                totalNodeCount,
                fingerprintBucketCount + latest.fingerprintBucketCount,
                mergedPartitions,
                mergedOwnership
        );
    }

    int primaryOwnerNodeCount() {
        return countNodesWith(IgniteNodeOwnership::primaryFingerprintCount);
    }

    int backupOwnerNodeCount() {
        return countNodesWith(IgniteNodeOwnership::backupFingerprintCount);
    }

    String primaryOwnerRatio() {
        return ownerRatio(primaryOwnerNodeCount());
    }

    String backupOwnerRatio() {
        return ownerRatio(backupOwnerNodeCount());
    }

    String primaryFingerprintOwnershipByNode() {
        return formatByNode(IgniteNodeOwnership::primaryFingerprintCount);
    }

    String backupFingerprintOwnershipByNode() {
        return formatByNode(IgniteNodeOwnership::backupFingerprintCount);
    }

    String primaryPartitionsByNode() {
        return formatByNode(IgniteNodeOwnership::primaryPartitionCount);
    }

    String backupPartitionsByNode() {
        return formatByNode(IgniteNodeOwnership::backupPartitionCount);
    }

    String localPrimaryEntriesByNode() {
        return formatByNode(IgniteNodeOwnership::localPrimaryEntries);
    }

    String localBackupEntriesByNode() {
        return formatByNode(IgniteNodeOwnership::localBackupEntries);
    }

    int partitionsTouchedCount() {
        return partitionsTouched.size();
    }

    private int countNodesWith(ToIntFunction<IgniteNodeOwnership> valueExtractor) {
        int count = 0;
        for (IgniteNodeOwnership ownership : nodeOwnership) {
            if (valueExtractor.applyAsInt(ownership) > 0) {
                count++;
            }
        }
        return count;
    }

    private String ownerRatio(int ownerNodeCount) {
        return ownerNodeCount + " / " + totalNodeCount;
    }

    private String formatByNode(ToIntFunction<IgniteNodeOwnership> valueExtractor) {
        StringBuilder formatted = new StringBuilder();
        for (IgniteNodeOwnership ownership : nodeOwnership) {
            if (!formatted.isEmpty()) {
                formatted.append(", ");
            }
            formatted.append(ownership.nodeName())
                    .append("=")
                    .append(valueExtractor.applyAsInt(ownership));
        }
        return formatted.toString();
    }
}
