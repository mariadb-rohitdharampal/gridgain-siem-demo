package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogSourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopicNamesTest {
    @Test
    void mapsSourcesToRawTopics() {
        assertEquals("raw-firewall", TopicNames.rawTopicName(LogSourceType.FIREWALL));
        assertEquals("raw-dns", TopicNames.rawTopicName(LogSourceType.DNS));
        assertEquals("raw-windows-ad", TopicNames.rawTopicName(LogSourceType.WINDOWS_AD));
        assertEquals("raw-cloud-zero-trust", TopicNames.rawTopicName(LogSourceType.CLOUD_ZERO_TRUST));
    }

    @Test
    void mapsSourcesToCleanTopics() {
        assertEquals("clean-firewall", TopicNames.cleanTopicName(LogSourceType.FIREWALL));
        assertEquals("clean-dns", TopicNames.cleanTopicName(LogSourceType.DNS));
        assertEquals("clean-windows-ad", TopicNames.cleanTopicName(LogSourceType.WINDOWS_AD));
        assertEquals("clean-cloud-zero-trust", TopicNames.cleanTopicName(LogSourceType.CLOUD_ZERO_TRUST));
    }
}
