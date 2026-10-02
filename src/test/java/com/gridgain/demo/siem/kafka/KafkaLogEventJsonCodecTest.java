package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaLogEventJsonCodecTest {
    @Test
    void serializesRequiredFieldsAsCompactJson() {
        LogEvent event = LogEvent.observed(
                LogSourceType.FIREWALL,
                Instant.parse("2026-06-22T13:00:00Z"),
                "FIREWALL_ALLOW",
                "CEF payload \"quoted\" path=C:\\\\Temp\nnext",
                false,
                "firewall:allow:tcp:443:172.16.20.10"
        );

        String json = new KafkaLogEventJsonCodec().toJson(event);

        assertTrue(json.startsWith("{\"schemaVersion\":1,"));
        assertTrue(json.contains("\"timestamp\":\"2026-06-22T13:00:00Z\""));
        assertTrue(json.contains("\"source\":\"FIREWALL\""));
        assertTrue(json.contains("\"sourceDisplayName\":\"Firewall\""));
        assertTrue(json.contains("\"eventType\":\"FIREWALL_ALLOW\""));
        assertTrue(json.contains("\"type\":\"FIREWALL_ALLOW\""));
        assertTrue(json.contains("\"reductionKey\":\"firewall:allow:tcp:443:172.16.20.10\""));
        assertFalse(json.contains("\"fingerprint\""));
        assertTrue(json.contains("\"severity\":\"INFO\""));
        assertTrue(json.contains("\"securityRelevant\":false"));
        assertTrue(json.contains("\"representedEventCount\":1"));
        assertTrue(json.contains("CEF payload \\\"quoted\\\" path=C:\\\\\\\\Temp\\nnext"));
        assertFalse(json.contains(System.lineSeparator()));
    }

    @Test
    void marksSecurityRelevantEventsWithSecuritySeverity() {
        LogEvent event = LogEvent.observed(
                LogSourceType.DNS,
                Instant.parse("2026-06-22T13:02:00Z"),
                "DNS_THREAT_INTEL_MATCH",
                "threat payload",
                true,
                "security:dns:threat:example"
        );

        String json = new KafkaLogEventJsonCodec().toJson(event);

        assertTrue(json.contains("\"severity\":\"SECURITY\""));
        assertTrue(json.contains("\"securityRelevant\":true"));
    }

    @Test
    void roundTripsGeneratedJsonToLogEvent() {
        LogEvent event = new LogEvent(
                "event-1",
                LogSourceType.CLOUD_ZERO_TRUST,
                Instant.parse("2026-06-22T13:06:00Z"),
                "ZT_POLICY_DENY",
                "{\"decision\":\"deny\",\"reason\":\"quote=\\\"test\\\"\"}\nnext",
                true,
                "security:cloud:policy-deny:user:1",
                1
        );
        KafkaLogEventJsonCodec codec = new KafkaLogEventJsonCodec();

        LogEvent decoded = codec.fromJson(codec.toJson(event));

        assertEquals(event, decoded);
    }

    @Test
    void preservesSecurityRelevantFlagThroughRoundTrip() {
        LogEvent event = new LogEvent(
                "event-2",
                LogSourceType.WINDOWS_AD,
                Instant.parse("2026-06-22T13:04:00Z"),
                "AD_PRIVILEGE_GROUP_CHANGE",
                "WinEventLog: Security EventCode=4728",
                true,
                "security:ad:group-change:admin",
                1
        );
        KafkaLogEventJsonCodec codec = new KafkaLogEventJsonCodec();

        LogEvent decoded = codec.fromJson(codec.toJson(event));

        assertTrue(decoded.securityRelevant());
        assertEquals(event.reductionKey(), decoded.reductionKey());
    }

    @Test
    void reportsMalformedJson() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaLogEventJsonCodec().fromJson("{not json")
        );

        assertTrue(exception.getMessage().contains("Malformed Kafka log event JSON"));
    }

    @Test
    void reportsUnsupportedSchemaVersion() {
        String json = """
                {"schemaVersion":2,"id":"event-1","timestamp":"2026-06-22T13:00:00Z","source":"DNS","eventType":"DNS_QUERY_ALLOWED","reductionKey":"dns:query","securityRelevant":false,"representedEventCount":1,"payload":"payload"}
                """;

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaLogEventJsonCodec().fromJson(json)
        );

        assertTrue(exception.getMessage().contains("Unsupported Kafka log event schemaVersion: 2"));
    }
}
