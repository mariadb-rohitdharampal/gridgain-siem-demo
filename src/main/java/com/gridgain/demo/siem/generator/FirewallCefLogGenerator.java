package com.gridgain.demo.siem.generator;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FirewallCefLogGenerator implements LogGenerator {
    private static final Instant BASE_TIME = Instant.parse("2026-06-22T13:00:00Z");
    private static final String[] SOURCES = {"10.10.4.11", "10.10.4.12", "10.10.5.21", "10.10.5.22"};
    private static final String[] DESTINATIONS = {"172.16.20.10", "172.16.20.11", "172.16.21.15", "172.16.21.16"};
    private static final int[] PORTS = {443, 443, 443, 53, 80, 8080};

    @Override
    public LogSourceType sourceType() {
        return LogSourceType.FIREWALL;
    }

    @Override
    public List<LogEvent> generate(int count) {
        List<LogEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Instant timestamp = BASE_TIME.plusMillis(i * 25L);
            if (i % 97 == 0) {
                events.add(blockedOutbound(timestamp, i));
            } else if (i % 211 == 0) {
                events.add(portScan(timestamp, i));
            } else {
                events.add(allowedTraffic(timestamp, i));
            }
        }
        return events;
    }

    private LogEvent allowedTraffic(Instant timestamp, int index) {
        String source = SOURCES[index % SOURCES.length];
        String destination = DESTINATIONS[index % DESTINATIONS.length];
        int port = PORTS[index % PORTS.length];
        String key = "firewall:allow:tcp:%d:%s".formatted(port, destination);
        String payload = "CEF:0|Acme|Firewall|1.0|100|Allowed connection|3|src=%s dst=%s spt=%d dpt=%d proto=TCP act=allow"
                .formatted(source, destination, 49_152 + (index % 2048), port);
        return LogEvent.observed(sourceType(), timestamp, "FIREWALL_ALLOW", payload, false, key);
    }

    private LogEvent blockedOutbound(Instant timestamp, int index) {
        String destination = "203.0.113.%d".formatted(10 + (index % 20));
        String payload = "CEF:0|Acme|Firewall|1.0|900|Blocked outbound suspicious destination|8|src=10.10.9.%d dst=%s dpt=4444 proto=TCP act=blocked cs1Label=threat cs1=command-and-control"
                .formatted(20 + (index % 40), destination);
        return LogEvent.observed(sourceType(), timestamp, "FIREWALL_BLOCK", payload, true, "security:firewall:block:" + destination);
    }

    private LogEvent portScan(Instant timestamp, int index) {
        String source = "198.51.100.%d".formatted(1 + (index % 40));
        String payload = "CEF:0|Acme|Firewall|1.0|901|Inbound port scan|9|src=%s dst=172.16.20.10 dpt=%d proto=TCP act=blocked cs1Label=threat cs1=reconnaissance"
                .formatted(source, 20 + (index % 600));
        return LogEvent.observed(sourceType(), timestamp, "FIREWALL_PORT_SCAN", payload, true, "security:firewall:scan:" + source);
    }
}
