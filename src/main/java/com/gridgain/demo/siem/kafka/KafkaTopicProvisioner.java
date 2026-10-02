package com.gridgain.demo.siem.kafka;

interface KafkaTopicProvisioner {
    void createRawTopics(KafkaIntegrationConfig config);

    default void createRawAndCleanTopics(KafkaIntegrationConfig config) {
        createRawTopics(config);
    }
}
