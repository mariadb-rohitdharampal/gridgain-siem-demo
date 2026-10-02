package com.gridgain.demo.siem.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class KafkaTopicAdmin implements KafkaTopicProvisioner {
    @Override
    public void createRawTopics(KafkaIntegrationConfig config) {
        createTopics(config, rawTopicDefinitions(config));
    }

    @Override
    public void createRawAndCleanTopics(KafkaIntegrationConfig config) {
        createTopics(config, rawAndCleanTopicDefinitions(config));
    }

    private static void createTopics(KafkaIntegrationConfig config, List<NewTopic> topics) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {
            CreateTopicsResult result = adminClient.createTopics(topics);
            for (Map.Entry<String, KafkaFuture<Void>> topicResult : result.values().entrySet()) {
                waitForTopic(topicResult);
            }
        }
    }

    public static List<NewTopic> rawTopicDefinitions(KafkaIntegrationConfig config) {
        return config.rawTopicNames()
                .values()
                .stream()
                .distinct()
                .map(topicName -> new NewTopic(
                        topicName,
                        config.topicPartitions(),
                        config.topicReplicationFactor()
                ))
                .toList();
    }

    public static List<NewTopic> cleanTopicDefinitions(KafkaIntegrationConfig config) {
        return config.cleanTopicNames()
                .values()
                .stream()
                .distinct()
                .map(topicName -> new NewTopic(
                        topicName,
                        config.topicPartitions(),
                        config.topicReplicationFactor()
                ))
                .toList();
    }

    public static List<NewTopic> rawAndCleanTopicDefinitions(KafkaIntegrationConfig config) {
        List<NewTopic> rawTopics = rawTopicDefinitions(config);
        List<NewTopic> cleanTopics = cleanTopicDefinitions(config);
        java.util.ArrayList<NewTopic> topics = new java.util.ArrayList<>(rawTopics.size() + cleanTopics.size());
        topics.addAll(rawTopics);
        topics.addAll(cleanTopics);
        return List.copyOf(topics);
    }

    private static void waitForTopic(Map.Entry<String, KafkaFuture<Void>> topicResult) {
        try {
            topicResult.getValue().get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating Kafka topic " + topicResult.getKey(), ex);
        } catch (Exception ex) {
            if (ex.getCause() instanceof TopicExistsException) {
                return;
            }
            throw new IllegalStateException("Failed to create Kafka topic " + topicResult.getKey(), ex);
        }
    }
}
