package com.gridgain.demo.siem.pipeline;

import com.gridgain.demo.siem.event.LogSourceType;

public final class TopicNames {
    private TopicNames() {
    }

    public static String rawTopicName(LogSourceType sourceType) {
        return switch (sourceType) {
            case FIREWALL -> "raw-firewall";
            case DNS -> "raw-dns";
            case WINDOWS_AD -> "raw-windows-ad";
            case CLOUD_ZERO_TRUST -> "raw-cloud-zero-trust";
        };
    }

    public static String cleanTopicName(LogSourceType sourceType) {
        return switch (sourceType) {
            case FIREWALL -> "clean-firewall";
            case DNS -> "clean-dns";
            case WINDOWS_AD -> "clean-windows-ad";
            case CLOUD_ZERO_TRUST -> "clean-cloud-zero-trust";
        };
    }
}
