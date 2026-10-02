package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.generator.ActiveDirectoryLogGenerator;
import com.gridgain.demo.siem.generator.CloudZeroTrustJsonLogGenerator;
import com.gridgain.demo.siem.generator.DnsSyslogLogGenerator;
import com.gridgain.demo.siem.generator.FirewallCefLogGenerator;
import com.gridgain.demo.siem.generator.LogGenerator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProducerRunnerTest {
    @Test
    void routesGeneratedEventsToRawTopicCountsWithoutKafkaBroker() {
        KafkaIntegrationConfig defaults = KafkaIntegrationConfig.defaults();
        KafkaIntegrationConfig config = new KafkaIntegrationConfig(
                defaults.bootstrapServers(),
                defaults.pollMs(),
                defaults.runSeconds(),
                true,
                defaults.rawTopicNames(),
                defaults.cleanTopicNames(),
                defaults.topicPartitions(),
                defaults.topicReplicationFactor()
        );
        RecordingProvisioner provisioner = new RecordingProvisioner();
        RecordingSink sink = new RecordingSink();

        KafkaProduceResult result = new KafkaProducerRunner(generators())
                .run(8, config, provisioner, sink);

        assertTrue(provisioner.createRawTopicsCalled);
        assertTrue(sink.flushed);
        assertTrue(sink.closed);
        assertEquals(8, result.totalProduced());
        assertEquals(8, sink.events.size());
        assertEquals(2, result.producedForTopic("raw-firewall"));
        assertEquals(2, result.producedForTopic("raw-dns"));
        assertEquals(2, result.producedForTopic("raw-windows-ad"));
        assertEquals(2, result.producedForTopic("raw-cloud-zero-trust"));
    }

    @Test
    void skipsTopicCreationWhenDisabled() {
        KafkaIntegrationConfig config = KafkaIntegrationConfig.defaults();
        RecordingProvisioner provisioner = new RecordingProvisioner();
        RecordingSink sink = new RecordingSink();

        new KafkaProducerRunner(generators()).run(4, config, provisioner, sink);

        assertEquals(false, provisioner.createRawTopicsCalled);
        assertEquals(4, sink.events.size());
    }

    private static List<LogGenerator> generators() {
        return List.of(
                new FirewallCefLogGenerator(),
                new DnsSyslogLogGenerator(),
                new ActiveDirectoryLogGenerator(),
                new CloudZeroTrustJsonLogGenerator()
        );
    }

    private static final class RecordingProvisioner implements KafkaTopicProvisioner {
        private boolean createRawTopicsCalled;

        @Override
        public void createRawTopics(KafkaIntegrationConfig config) {
            createRawTopicsCalled = true;
        }
    }

    private static final class RecordingSink implements KafkaRawEventSink {
        private final List<LogEvent> events = new ArrayList<>();
        private boolean flushed;
        private boolean closed;

        @Override
        public void publish(LogEvent event) {
            events.add(event);
        }

        @Override
        public void flush() {
            flushed = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
