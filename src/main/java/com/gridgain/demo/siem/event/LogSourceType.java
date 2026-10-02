package com.gridgain.demo.siem.event;

public enum LogSourceType {
    FIREWALL("Firewall"),
    DNS("DNS"),
    WINDOWS_AD("Windows AD"),
    CLOUD_ZERO_TRUST("Cloud / Zero Trust");

    private final String displayName;

    LogSourceType(String displayName) {
        this.displayName = displayName;
    }

    public String displayName() {
        return displayName;
    }
}
