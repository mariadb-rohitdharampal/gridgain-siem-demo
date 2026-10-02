package com.gridgain.demo.siem.generator;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DnsSyslogLogGenerator implements LogGenerator {
    private static final Instant BASE_TIME = Instant.parse("2026-06-22T13:02:00Z");
    private static final String[] CLIENTS = {"10.20.1.15", "10.20.1.16", "10.20.2.22", "10.20.2.23"};
    private static final String[] BENIGN_DOMAINS = {
            "intranet.example.internal",
            "updates.example.com",
            "time.windows.com",
            "service.portal.example"
    };
    private static final String[] SUSPICIOUS_DOMAINS = {
            "exfil-drop.example",
            "dga-7391.example",
            "credential-harvest.example"
    };

    @Override
    public LogSourceType sourceType() {
        return LogSourceType.DNS;
    }

    @Override
    public List<LogEvent> generate(int count) {
        List<LogEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Instant timestamp = BASE_TIME.plusMillis(i * 20L);
            if (i % 89 == 0) {
                events.add(suspiciousLookup(timestamp, i));
            } else if (i % 157 == 0) {
                events.add(highEntropyLookup(timestamp, i));
            } else {
                events.add(benignLookup(timestamp, i));
            }
        }
        return events;
    }

    private LogEvent benignLookup(Instant timestamp, int index) {
        String client = CLIENTS[index % CLIENTS.length];
        String domain = BENIGN_DOMAINS[index % BENIGN_DOMAINS.length];
        String key = "dns:query:A:" + domain;
        String payload = "<134>1 %s dns01 example-corp named - - client=%s query=%s type=A rcode=NOERROR action=allow"
                .formatted(timestamp, client, domain);
        return LogEvent.observed(sourceType(), timestamp, "DNS_QUERY_ALLOWED", payload, false, key);
    }

    private LogEvent suspiciousLookup(Instant timestamp, int index) {
        String client = CLIENTS[index % CLIENTS.length];
        String domain = SUSPICIOUS_DOMAINS[index % SUSPICIOUS_DOMAINS.length];
        String payload = "<132>1 %s dns01 example-corp named - - client=%s query=%s type=A rcode=NXDOMAIN action=alert reason=threat-intel-match"
                .formatted(timestamp, client, domain);
        return LogEvent.observed(sourceType(), timestamp, "DNS_THREAT_INTEL_MATCH", payload, true, "security:dns:threat:" + domain);
    }

    private LogEvent highEntropyLookup(Instant timestamp, int index) {
        String client = CLIENTS[index % CLIENTS.length];
        String domain = "xj%d-kq%d-zp%d.example".formatted(index, index * 7, index * 11);
        String payload = "<132>1 %s dns01 example-corp named - - client=%s query=%s type=TXT rcode=NOERROR action=alert reason=high-entropy-domain"
                .formatted(timestamp, client, domain);
        return LogEvent.observed(sourceType(), timestamp, "DNS_HIGH_ENTROPY", payload, true, "security:dns:entropy:" + domain);
    }
}
