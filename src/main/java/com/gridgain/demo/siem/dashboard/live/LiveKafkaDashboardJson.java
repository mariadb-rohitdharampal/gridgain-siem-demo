package com.gridgain.demo.siem.dashboard.live;

import com.gridgain.demo.siem.kafka.KafkaReductionProgressSnapshot;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;

public final class LiveKafkaDashboardJson {
    public String json(LiveKafkaDashboardState state) {
        return json(state.latestSnapshot(), state.startedAt(), state.lastUpdatedAt());
    }

    String json(KafkaReductionProgressSnapshot snapshot, Instant startedAt, Instant lastUpdatedAt) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"startedAt\":").append(jsonString(startedAt.toString())).append(",");
        json.append("\"lastUpdatedAt\":").append(jsonString(lastUpdatedAt.toString())).append(",");
        json.append("\"recordsSeen\":").append(snapshot.recordsSeen()).append(",");
        json.append("\"validConsumedEvents\":").append(snapshot.validConsumedEvents()).append(",");
        json.append("\"producedCleanEvents\":").append(snapshot.producedCleanEvents()).append(",");
        json.append("\"reductionPercentage\":").append(jsonNumber(snapshot.reductionPercentage())).append(",");
        json.append("\"malformedRecords\":").append(snapshot.malformedRecords()).append(",");
        json.append("\"securityEventsObserved\":").append(snapshot.securityEventsObserved()).append(",");
        json.append("\"securityEventsPreserved\":").append(snapshot.securityEventsPreserved()).append(",");
        json.append("\"windowsProcessed\":").append(snapshot.windowsProcessed()).append(",");
        json.append("\"openWindows\":").append(snapshot.openWindows()).append(",");
        json.append("\"elapsedMillis\":").append(snapshot.elapsed().toMillis()).append(",");
        json.append("\"consumedRawTopicCounts\":").append(numberMapJson(snapshot.consumedRawTopicCounts())).append(",");
        json.append("\"producedCleanTopicCounts\":").append(numberMapJson(snapshot.producedCleanTopicCounts())).append(",");
        json.append("\"proofMetrics\":").append(stringMapJson(snapshot.proofMetrics()));
        json.append("}");
        return json.toString();
    }

    private static String numberMapJson(Map<String, Integer> values) {
        StringBuilder json = new StringBuilder("{");
        int index = 0;
        for (Map.Entry<String, Integer> entry : values.entrySet()) {
            if (index > 0) {
                json.append(",");
            }
            json.append(jsonString(entry.getKey())).append(":").append(entry.getValue());
            index++;
        }
        json.append("}");
        return json.toString();
    }

    private static String stringMapJson(Map<String, String> values) {
        StringBuilder json = new StringBuilder("{");
        int index = 0;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (index > 0) {
                json.append(",");
            }
            json.append(jsonString(entry.getKey())).append(":").append(jsonString(entry.getValue()));
            index++;
        }
        json.append("}");
        return json.toString();
    }

    private static String jsonString(String value) {
        StringBuilder escaped = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                case '<' -> escaped.append("\\u003c");
                case '>' -> escaped.append("\\u003e");
                case '&' -> escaped.append("\\u0026");
                default -> {
                    if (character < 0x20) {
                        escaped.append(String.format(Locale.ROOT, "\\u%04x", (int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        escaped.append("\"");
        return escaped.toString();
    }

    private static String jsonNumber(double value) {
        return String.format(Locale.ROOT, "%.6f", value);
    }
}
