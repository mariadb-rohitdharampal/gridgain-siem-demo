package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRawEventProducerTest {
    @Test
    void buildsProducerRecordForMatchingRawTopic() {
        LogEvent event = LogEvent.observed(
                LogSourceType.DNS,
                Instant.parse("2026-06-22T13:02:00Z"),
                "DNS_QUERY_ALLOWED",
                "dns payload",
                false,
                "dns:query:A:intranet.example.internal"
        );

        ProducerRecord<String, String> record = KafkaRawEventProducer.toProducerRecord(
                event,
                KafkaIntegrationConfig.defaults(),
                new KafkaLogEventJsonCodec()
        );

        assertEquals("raw-dns", record.topic());
        assertEquals("DNS|dns:query:A:intranet.example.internal", record.key());
        assertTrue(record.value().contains("\"source\":\"DNS\""));
        assertTrue(record.value().contains("\"payload\":\"dns payload\""));
    }
}
