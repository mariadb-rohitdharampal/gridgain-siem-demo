package com.gridgain.demo.siem.reduction.gridgain;

record IgniteNodeOwnership(
        String nodeName,
        int primaryFingerprintCount,
        int backupFingerprintCount,
        int primaryPartitionCount,
        int backupPartitionCount,
        int localPrimaryEntries,
        int localBackupEntries
) {
    IgniteNodeOwnership mergeObserved(IgniteNodeOwnership other) {
        return new IgniteNodeOwnership(
                nodeName,
                primaryFingerprintCount + other.primaryFingerprintCount,
                backupFingerprintCount + other.backupFingerprintCount,
                Math.max(primaryPartitionCount, other.primaryPartitionCount),
                Math.max(backupPartitionCount, other.backupPartitionCount),
                Math.max(localPrimaryEntries, other.localPrimaryEntries),
                Math.max(localBackupEntries, other.localBackupEntries)
        );
    }
}
