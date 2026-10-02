package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.pipeline.TopicNames;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

public record KafkaIntegrationConfig(
        String bootstrapServers,
        long pollMs,
        long runSeconds,
        boolean createTopics,
        Map<LogSourceType, String> rawTopicNames,
        Map<LogSourceType, String> cleanTopicNames,
        int topicPartitions,
        short topicReplicationFactor,
        String consumerGroupId,
        String autoOffsetReset,
        KafkaCommitStrategy commitStrategy
) {
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final long DEFAULT_POLL_MS = 1_000L;
    public static final long DEFAULT_RUN_SECONDS = 60L;
    public static final boolean DEFAULT_CREATE_TOPICS = false;
    public static final int DEFAULT_TOPIC_PARTITIONS = 3;
    public static final short DEFAULT_TOPIC_REPLICATION_FACTOR = 1;
    public static final String DEFAULT_CONSUMER_GROUP_ID = "gridgain-siem-reducer";
    public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
    public static final KafkaCommitStrategy DEFAULT_COMMIT_STRATEGY = KafkaCommitStrategy.MANUAL_SYNC;

    public KafkaIntegrationConfig(
            String bootstrapServers,
            long pollMs,
            long runSeconds,
            boolean createTopics,
            Map<LogSourceType, String> rawTopicNames,
            Map<LogSourceType, String> cleanTopicNames
    ) {
        this(
                bootstrapServers,
                pollMs,
                runSeconds,
                createTopics,
                rawTopicNames,
                cleanTopicNames,
                DEFAULT_TOPIC_PARTITIONS,
                DEFAULT_TOPIC_REPLICATION_FACTOR,
                DEFAULT_CONSUMER_GROUP_ID,
                DEFAULT_AUTO_OFFSET_RESET,
                DEFAULT_COMMIT_STRATEGY
        );
    }

    public KafkaIntegrationConfig(
            String bootstrapServers,
            long pollMs,
            long runSeconds,
            boolean createTopics,
            Map<LogSourceType, String> rawTopicNames,
            Map<LogSourceType, String> cleanTopicNames,
            int topicPartitions,
            short topicReplicationFactor
    ) {
        this(
                bootstrapServers,
                pollMs,
                runSeconds,
                createTopics,
                rawTopicNames,
                cleanTopicNames,
                topicPartitions,
                topicReplicationFactor,
                DEFAULT_CONSUMER_GROUP_ID,
                DEFAULT_AUTO_OFFSET_RESET,
                DEFAULT_COMMIT_STRATEGY
        );
    }

    public KafkaIntegrationConfig {
        bootstrapServers = requireText(bootstrapServers, "bootstrapServers");
        if (pollMs <= 0) {
            throw new IllegalArgumentException("pollMs must be greater than zero");
        }
        if (runSeconds < 0) {
            throw new IllegalArgumentException("runSeconds must not be negative");
        }
        rawTopicNames = copyTopicNames(rawTopicNames, "rawTopicNames");
        cleanTopicNames = copyTopicNames(cleanTopicNames, "cleanTopicNames");
        if (topicPartitions <= 0) {
            throw new IllegalArgumentException("topicPartitions must be greater than zero");
        }
        if (topicReplicationFactor <= 0) {
            throw new IllegalArgumentException("topicReplicationFactor must be greater than zero");
        }
        consumerGroupId = requireText(consumerGroupId, "consumerGroupId");
        autoOffsetReset = requireText(autoOffsetReset, "autoOffsetReset").toLowerCase();
        if (!autoOffsetReset.equals("earliest") && !autoOffsetReset.equals("latest") && !autoOffsetReset.equals("none")) {
            throw new IllegalArgumentException("autoOffsetReset must be earliest, latest, or none");
        }
        commitStrategy = Objects.requireNonNull(commitStrategy, "commitStrategy");
    }

    public static KafkaIntegrationConfig defaults() {
        return new KafkaIntegrationConfig(
                DEFAULT_BOOTSTRAP_SERVERS,
                DEFAULT_POLL_MS,
                DEFAULT_RUN_SECONDS,
                DEFAULT_CREATE_TOPICS,
                defaultRawTopicNames(),
                defaultCleanTopicNames(),
                DEFAULT_TOPIC_PARTITIONS,
                DEFAULT_TOPIC_REPLICATION_FACTOR,
                DEFAULT_CONSUMER_GROUP_ID,
                DEFAULT_AUTO_OFFSET_RESET,
                DEFAULT_COMMIT_STRATEGY
        );
    }

    public String rawTopicName(LogSourceType sourceType) {
        return rawTopicNames.get(sourceType);
    }

    public String cleanTopicName(LogSourceType sourceType) {
        return cleanTopicNames.get(sourceType);
    }

    private static Map<LogSourceType, String> defaultRawTopicNames() {
        EnumMap<LogSourceType, String> topicNames = new EnumMap<>(LogSourceType.class);
        for (LogSourceType sourceType : LogSourceType.values()) {
            topicNames.put(sourceType, TopicNames.rawTopicName(sourceType));
        }
        return topicNames;
    }

    private static Map<LogSourceType, String> defaultCleanTopicNames() {
        EnumMap<LogSourceType, String> topicNames = new EnumMap<>(LogSourceType.class);
        for (LogSourceType sourceType : LogSourceType.values()) {
            topicNames.put(sourceType, TopicNames.cleanTopicName(sourceType));
        }
        return topicNames;
    }

    private static Map<LogSourceType, String> copyTopicNames(
            Map<LogSourceType, String> topicNames,
            String fieldName
    ) {
        if (topicNames == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }

        EnumMap<LogSourceType, String> copy = new EnumMap<>(LogSourceType.class);
        for (LogSourceType sourceType : LogSourceType.values()) {
            copy.put(sourceType, requireText(topicNames.get(sourceType), fieldName + "." + sourceType.name()));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
