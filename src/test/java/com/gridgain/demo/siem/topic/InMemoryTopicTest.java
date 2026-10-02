package com.gridgain.demo.siem.topic;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryTopicTest {
    @Test
    void publishesAndDrainsMessages() {
        Topic<String> topic = new InMemoryTopic<>("raw-firewall");

        topic.publish("one");
        topic.publishAll(List.of("two", "three"));

        assertEquals("raw-firewall", topic.name());
        assertEquals(3, topic.size());
        assertEquals(List.of("one", "two", "three"), topic.drain());
        assertEquals(0, topic.size());
    }
}
