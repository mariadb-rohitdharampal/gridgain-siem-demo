package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogSourceType;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaIntegrationConfigTest {
    @Test
    void defaultsUseSimulationTopicNames() {
        KafkaIntegrationConfig config = KafkaIntegrationConfig.defaults();

        assertEquals("localhost:9092", config.bootstrapServers());
        assertEquals(1_000L, config.pollMs());
        assertEquals(60L, config.runSeconds());
        assertEquals("gridgain-siem-reducer", config.consumerGroupId());
        assertEquals("earliest", config.autoOffsetReset());
        assertEquals(KafkaCommitStrategy.MANUAL_SYNC, config.commitStrategy());
        assertEquals(3, config.topicPartitions());
        assertEquals((short) 1, config.topicReplicationFactor());
        assertEquals("raw-firewall", config.rawTopicName(LogSourceType.FIREWALL));
        assertEquals("raw-dns", config.rawTopicName(LogSourceType.DNS));
        assertEquals("raw-windows-ad", config.rawTopicName(LogSourceType.WINDOWS_AD));
        assertEquals("raw-cloud-zero-trust", config.rawTopicName(LogSourceType.CLOUD_ZERO_TRUST));
        assertEquals("clean-firewall", config.cleanTopicName(LogSourceType.FIREWALL));
        assertEquals("clean-dns", config.cleanTopicName(LogSourceType.DNS));
        assertEquals("clean-windows-ad", config.cleanTopicName(LogSourceType.WINDOWS_AD));
        assertEquals("clean-cloud-zero-trust", config.cleanTopicName(LogSourceType.CLOUD_ZERO_TRUST));
    }

    @Test
    void rejectsInvalidTimingValues() {
        KafkaIntegrationConfig defaults = KafkaIntegrationConfig.defaults();

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                0,
                defaults.runSeconds(),
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames()
        ));

        KafkaIntegrationConfig continuousAliasConfig = new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                0,
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames()
        );
        assertEquals(0, continuousAliasConfig.runSeconds());

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                -1,
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames()
        ));
    }

    @Test
    void rejectsMissingTopicNames() {
        KafkaIntegrationConfig defaults = KafkaIntegrationConfig.defaults();
        EnumMap<LogSourceType, String> rawTopics = new EnumMap<>(defaults.rawTopicNames());
        rawTopics.remove(LogSourceType.DNS);

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                defaults.runSeconds(),
                defaults.createTopics(),
                rawTopics,
                defaults.cleanTopicNames()
        ));
    }

    @Test
    void rejectsInvalidTopicSettings() {
        KafkaIntegrationConfig defaults = KafkaIntegrationConfig.defaults();

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                defaults.runSeconds(),
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames(),
                0,
                defaults.topicReplicationFactor()
        ));

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                defaults.runSeconds(),
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames(),
                defaults.topicPartitions(),
                (short) 0
        ));
    }

    @Test
    void rejectsInvalidConsumerSettings() {
        KafkaIntegrationConfig defaults = KafkaIntegrationConfig.defaults();

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                defaults.runSeconds(),
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames(),
                defaults.topicPartitions(),
                defaults.topicReplicationFactor(),
                " ",
                defaults.autoOffsetReset(),
                defaults.commitStrategy()
        ));

        assertThrows(IllegalArgumentException.class, () -> new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                defaults.runSeconds(),
                defaults.createTopics(),
                defaults.rawTopicNames(),
                defaults.cleanTopicNames(),
                defaults.topicPartitions(),
                defaults.topicReplicationFactor(),
                defaults.consumerGroupId(),
                "middle",
                defaults.commitStrategy()
        ));
    }
}
