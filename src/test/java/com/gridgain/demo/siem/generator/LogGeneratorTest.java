package com.gridgain.demo.siem.generator;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogGeneratorTest {
    @Test
    void firewallGeneratorCreatesCefEvents() {
        List<LogEvent> events = new FirewallCefLogGenerator().generate(250);

        assertEquals(250, events.size());
        assertTrue(events.stream().allMatch(event -> event.sourceType() == LogSourceType.FIREWALL));
        assertTrue(events.stream().anyMatch(event -> event.rawPayload().startsWith("CEF:0|")));
        assertTrue(events.stream().anyMatch(LogEvent::securityRelevant));
        assertFalse(events.stream().allMatch(LogEvent::securityRelevant));
    }

    @Test
    void dnsGeneratorCreatesSyslogEvents() {
        List<LogEvent> events = new DnsSyslogLogGenerator().generate(250);

        assertEquals(250, events.size());
        assertTrue(events.stream().allMatch(event -> event.sourceType() == LogSourceType.DNS));
        assertTrue(events.stream().anyMatch(event -> event.rawPayload().startsWith("<")));
        assertTrue(events.stream().anyMatch(LogEvent::securityRelevant));
        assertFalse(events.stream().allMatch(LogEvent::securityRelevant));
    }

    @Test
    void activeDirectoryGeneratorCreatesWindowsEvents() {
        List<LogEvent> events = new ActiveDirectoryLogGenerator().generate(250);

        assertEquals(250, events.size());
        assertTrue(events.stream().allMatch(event -> event.sourceType() == LogSourceType.WINDOWS_AD));
        assertTrue(events.stream().anyMatch(event -> event.rawPayload().contains("WinEventLog: Security")));
        assertTrue(events.stream().anyMatch(LogEvent::securityRelevant));
        assertFalse(events.stream().allMatch(LogEvent::securityRelevant));
    }

    @Test
    void cloudGeneratorCreatesJsonEvents() {
        List<LogEvent> events = new CloudZeroTrustJsonLogGenerator().generate(250);

        assertEquals(250, events.size());
        assertTrue(events.stream().allMatch(event -> event.sourceType() == LogSourceType.CLOUD_ZERO_TRUST));
        assertTrue(events.stream().anyMatch(event -> event.rawPayload().startsWith("{")));
        assertTrue(events.stream().anyMatch(LogEvent::securityRelevant));
        assertFalse(events.stream().allMatch(LogEvent::securityRelevant));
    }
}
