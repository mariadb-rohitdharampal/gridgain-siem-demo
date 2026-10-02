package com.gridgain.demo.siem.generator;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ActiveDirectoryLogGenerator implements LogGenerator {
    private static final Instant BASE_TIME = Instant.parse("2026-06-22T13:04:00Z");
    private static final String[] USERS = {"svc_patch", "analyst01", "analyst02", "operator01", "operator02"};
    private static final String[] WORKSTATIONS = {"WS-101", "WS-102", "WS-201", "WS-202"};

    @Override
    public LogSourceType sourceType() {
        return LogSourceType.WINDOWS_AD;
    }

    @Override
    public List<LogEvent> generate(int count) {
        List<LogEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Instant timestamp = BASE_TIME.plusMillis(i * 30L);
            if (i % 83 == 0) {
                events.add(privilegeEscalation(timestamp, i));
            } else if (i % 131 == 0) {
                events.add(repeatedFailure(timestamp, i));
            } else {
                events.add(successfulLogon(timestamp, i));
            }
        }
        return events;
    }

    private LogEvent successfulLogon(Instant timestamp, int index) {
        String user = USERS[index % USERS.length];
        String workstation = WORKSTATIONS[index % WORKSTATIONS.length];
        String key = "windows-ad:4624:success:interactive:" + workstation;
        String payload = "WinEventLog: Security EventCode=4624 TimeGenerated=%s AccountName=%s WorkstationName=%s LogonType=2 Status=Success"
                .formatted(timestamp, user, workstation);
        return LogEvent.observed(sourceType(), timestamp, "AD_LOGON_SUCCESS", payload, false, key);
    }

    private LogEvent privilegeEscalation(Instant timestamp, int index) {
        String user = "admin-candidate-%02d".formatted(index % 20);
        String payload = "WinEventLog: Security EventCode=4728 TimeGenerated=%s AccountName=%s GroupName=Domain Admins Action=MemberAdded"
                .formatted(timestamp, user);
        return LogEvent.observed(sourceType(), timestamp, "AD_PRIVILEGE_GROUP_CHANGE", payload, true, "security:ad:group-change:" + user);
    }

    private LogEvent repeatedFailure(Instant timestamp, int index) {
        String user = USERS[index % USERS.length];
        String source = "10.30.40.%d".formatted(10 + (index % 60));
        String payload = "WinEventLog: Security EventCode=4625 TimeGenerated=%s AccountName=%s SourceNetworkAddress=%s Status=Failed SubStatus=BadPassword"
                .formatted(timestamp, user, source);
        return LogEvent.observed(sourceType(), timestamp, "AD_LOGON_FAILURE_SPIKE", payload, true, "security:ad:failed-logon:" + user + ":" + source);
    }
}
