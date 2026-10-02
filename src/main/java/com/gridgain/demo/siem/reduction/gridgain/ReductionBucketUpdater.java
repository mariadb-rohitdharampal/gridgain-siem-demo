package com.gridgain.demo.siem.reduction.gridgain;

import java.io.Serializable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

final class ReductionBucketUpdater implements EntryProcessor<String, ReductionBucket, Void>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String sourceTypeName;
    private final String reductionKey;
    private final long timestampEpochMillis;

    ReductionBucketUpdater(String sourceTypeName, String reductionKey, long timestampEpochMillis) {
        this.sourceTypeName = sourceTypeName;
        this.reductionKey = reductionKey;
        this.timestampEpochMillis = timestampEpochMillis;
    }

    @Override
    public Void process(MutableEntry<String, ReductionBucket> entry, Object... arguments) throws EntryProcessorException {
        ReductionBucket current = entry.exists() ? entry.getValue() : null;
        if (current == null) {
            entry.setValue(new ReductionBucket(sourceTypeName, reductionKey, 1, timestampEpochMillis));
        } else {
            entry.setValue(current.increment(timestampEpochMillis));
        }
        return null;
    }
}
