package com.gridgain.demo.siem.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaTopicAdminTest {
    @Test
    void createsDefinitionsForRawTopicsOnly() {
        KafkaIntegrationConfig config = KafkaIntegrationConfig.defaults();

        Map<String, NewTopic> topics = KafkaTopicAdmin.rawTopicDefinitions(config)
                .stream()
                .collect(Collectors.toMap(NewTopic::name, Function.identity()));

        assertEquals(4, topics.size());
        assertTrue(topics.containsKey("raw-firewall"));
        assertTrue(topics.containsKey("raw-dns"));
        assertTrue(topics.containsKey("raw-windows-ad"));
        assertTrue(topics.containsKey("raw-cloud-zero-trust"));
        assertEquals(3, topics.get("raw-firewall").numPartitions());
        assertEquals((short) 1, topics.get("raw-firewall").replicationFactor());
    }

    @Test
    void createsDefinitionsForRawAndCleanTopics() {
        KafkaIntegrationConfig config = KafkaIntegrationConfig.defaults();

        Map<String, NewTopic> topics = KafkaTopicAdmin.rawAndCleanTopicDefinitions(config)
                .stream()
                .collect(Collectors.toMap(NewTopic::name, Function.identity()));

        assertEquals(8, topics.size());
        assertTrue(topics.containsKey("raw-firewall"));
        assertTrue(topics.containsKey("raw-dns"));
        assertTrue(topics.containsKey("raw-windows-ad"));
        assertTrue(topics.containsKey("raw-cloud-zero-trust"));
        assertTrue(topics.containsKey("clean-firewall"));
        assertTrue(topics.containsKey("clean-dns"));
        assertTrue(topics.containsKey("clean-windows-ad"));
        assertTrue(topics.containsKey("clean-cloud-zero-trust"));
    }
}
