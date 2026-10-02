package com.gridgain.demo.siem.generator;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CloudZeroTrustJsonLogGenerator implements LogGenerator {
    private static final Instant BASE_TIME = Instant.parse("2026-06-22T13:06:00Z");
    private static final String[] USERS = {"analyst01@example.com", "analyst02@example.com", "operator01@example.com"};
    private static final String[] APPS = {"case-management", "mission-dashboard", "artifact-store", "ticketing"};

    @Override
    public LogSourceType sourceType() {
        return LogSourceType.CLOUD_ZERO_TRUST;
    }

    @Override
    public List<LogEvent> generate(int count) {
        List<LogEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Instant timestamp = BASE_TIME.plusMillis(i * 35L);
            if (i % 101 == 0) {
                events.add(impossibleTravel(timestamp, i));
            } else if (i % 173 == 0) {
                events.add(policyViolation(timestamp, i));
            } else {
                events.add(allowedApplicationAccess(timestamp, i));
            }
        }
        return events;
    }

    private LogEvent allowedApplicationAccess(Instant timestamp, int index) {
        String user = USERS[index % USERS.length];
        String app = APPS[index % APPS.length];
        String device = "managed-%02d".formatted(index % 12);
        String key = "cloud-zt:allow:" + app + ":managed-device";
        String payload = """
                {"time":"%s","source":"zero-trust","user":"%s","application":"%s","device":"%s","decision":"allow","riskScore":12}
                """.formatted(timestamp, user, app, device).trim();
        return LogEvent.observed(sourceType(), timestamp, "ZT_ACCESS_ALLOWED", payload, false, key);
    }

    private LogEvent impossibleTravel(Instant timestamp, int index) {
        String user = USERS[index % USERS.length];
        String payload = """
                {"time":"%s","source":"zero-trust","user":"%s","application":"mission-dashboard","decision":"challenge","riskScore":91,"reason":"impossible-travel"}
                """.formatted(timestamp, user).trim();
        return LogEvent.observed(sourceType(), timestamp, "ZT_IMPOSSIBLE_TRAVEL", payload, true, "security:cloud:impossible-travel:" + user + ":" + index);
    }

    private LogEvent policyViolation(Instant timestamp, int index) {
        String user = USERS[index % USERS.length];
        String payload = """
                {"time":"%s","source":"zero-trust","user":"%s","application":"artifact-store","decision":"deny","riskScore":88,"reason":"unmanaged-device-sensitive-app"}
                """.formatted(timestamp, user).trim();
        return LogEvent.observed(sourceType(), timestamp, "ZT_POLICY_DENY", payload, true, "security:cloud:policy-deny:" + user + ":" + index);
    }
}
