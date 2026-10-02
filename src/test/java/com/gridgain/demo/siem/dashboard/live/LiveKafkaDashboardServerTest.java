package com.gridgain.demo.siem.dashboard.live;

import com.gridgain.demo.siem.kafka.KafkaReductionProgressSnapshot;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveKafkaDashboardServerTest {
    @Test
    void healthReturnsOkFromEphemeralPort() throws Exception {
        LiveKafkaDashboardState state = new LiveKafkaDashboardState();
        try (LiveKafkaDashboardServer server = new LiveKafkaDashboardServer(state, 0).start()) {
            HttpResponse<String> response = get(server, "/health");

            assertEquals(200, response.statusCode());
            assertEquals("ok", response.body());
            assertTrue(server.port() > 0);
        }
    }

    @Test
    void apiProgressJsonIncludesAggregateTopicAndProofMetrics() throws Exception {
        LiveKafkaDashboardState state = new LiveKafkaDashboardState();
        Map<String, Integer> rawCounts = new LinkedHashMap<>();
        rawCounts.put("raw-firewall", 7);
        rawCounts.put("raw-dns", 5);
        Map<String, Integer> cleanCounts = new LinkedHashMap<>();
        cleanCounts.put("clean-firewall", 4);
        cleanCounts.put("clean-dns", 2);
        Map<String, String> proofMetrics = new LinkedHashMap<>();
        proofMetrics.put("embedded node count", "3");
        proofMetrics.put("primary owner nodes observed", "3 / 3");
        state.update(new KafkaReductionProgressSnapshot(
                12,
                10,
                6,
                2,
                3,
                3,
                4,
                1,
                Duration.ofSeconds(5),
                rawCounts,
                cleanCounts,
                proofMetrics
        ));

        try (LiveKafkaDashboardServer server = new LiveKafkaDashboardServer(state, 0).start()) {
            HttpResponse<String> response = get(server, "/api/progress");

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"recordsSeen\":12"));
            assertTrue(response.body().contains("\"validConsumedEvents\":10"));
            assertTrue(response.body().contains("\"producedCleanEvents\":6"));
            assertTrue(response.body().contains("\"reductionPercentage\":40.000000"));
            assertTrue(response.body().contains("\"malformedRecords\":2"));
            assertTrue(response.body().contains("\"consumedRawTopicCounts\":{\"raw-firewall\":7,\"raw-dns\":5}"));
            assertTrue(response.body().contains("\"producedCleanTopicCounts\":{\"clean-firewall\":4,\"clean-dns\":2}"));
            assertTrue(response.body().contains("\"embedded node count\":\"3\""));
            assertTrue(response.body().contains("\"primary owner nodes observed\":\"3 / 3\""));
        }
    }

    @Test
    void liveDashboardHtmlHasNoExternalJsOrCssReferences() throws Exception {
        LiveKafkaDashboardState state = new LiveKafkaDashboardState();
        try (LiveKafkaDashboardServer server = new LiveKafkaDashboardServer(state, 0).start()) {
            HttpResponse<String> response = get(server, "/");

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("GridGain SIEM Live Kafka Dashboard"));
            assertTrue(response.body().contains("fetch('/api/progress'"));
            assertFalse(response.body().contains("<script src="));
            assertFalse(response.body().contains("<link rel=\"stylesheet\""));
            assertFalse(response.body().contains("https://"));
            assertFalse(response.body().contains("http://"));
        }
    }

    private static HttpResponse<String> get(LiveKafkaDashboardServer server, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:%d%s".formatted(server.port(), path)))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
}
