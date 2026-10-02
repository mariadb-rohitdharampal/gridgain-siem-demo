package com.gridgain.demo.siem.dashboard;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.streaming.WindowMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DashboardRendererTest {
    @TempDir
    Path tempDir;

    @Test
    void generatesDirectModeDashboardFileWithKeyMetrics() throws Exception {
        DemoMetrics metrics = DemoMetrics.from(
                List.of(event(false), event(false), event(true)),
                List.of(LogEvent.summary(LogSourceType.FIREWALL, Instant.parse("2026-06-22T13:00:00Z"), "key", 2), event(true)),
                Duration.ofMillis(5),
                "inmemory",
                Map.of()
        );
        SavingsEstimate estimate = new SavingsEstimator().estimate(metrics, 1_200, 500.0, 30);
        DashboardModel model = new DashboardModel(metrics, estimate, false, 1_200, 500.0);

        Path output = new DashboardRenderer().render(model, tempDir.resolve("direct-dashboard.html"));

        String html = Files.readString(output);
        assertTrue(html.contains("Direct mode: generated events reduced without simulated topics"));
        assertTrue(html.contains("Raw events"));
        assertTrue(html.contains("Reduced events"));
        assertTrue(html.contains("Reduction"));
        assertTrue(html.contains("Security events preserved"));
        assertTrue(html.contains("TB/day saved"));
        assertTrue(html.contains("Estimated monthly savings"));
        assertTrue(html.contains("Retention extension"));
        assertTrue(html.contains("Architecture Flow"));
        assertTrue(html.contains("<script type=\"application/json\" id=\"dashboard-data\">"));
        assertTrue(html.contains("document.querySelectorAll('[data-dashboard-toggle]')"));
        assertTrue(html.contains("data-dashboard-toggle=\"business-impact\" checked"));
        assertTrue(html.contains("data-dashboard-toggle=\"topic-metrics\" disabled"));
        assertTrue(html.contains("data-dashboard-toggle=\"window-metrics\" disabled"));
        assertTrue(html.contains("style=\"width:100.00%\""));
        assertTrue(html.contains("style=\"width:66.67%\""));
        assertFalse(html.contains("id=\"topic-metrics\" data-dashboard-section"));
        assertFalse(html.contains("id=\"window-metrics\" data-dashboard-section"));
        assertSelfContained(html);
    }

    @Test
    void generatesKafkaSimulationDashboardWithTopicAndIgniteMetrics() throws Exception {
        DemoMetrics metrics = DemoMetrics.from(
                List.of(event(false), event(false), event(true)),
                List.of(LogEvent.summary(LogSourceType.FIREWALL, Instant.parse("2026-06-22T13:00:00Z"), "key", 2), event(true)),
                Duration.ofMillis(5),
                "gridgain / embedded Apache Ignite",
                Map.of(
                        "cache name", "siem-reduction-state",
                        "reduction state bucket count", "1",
                        "embedded node count", "3",
                        "cache mode", "PARTITIONED",
                        "backup count", "1"
                ),
                List.of(new DemoMetrics.TopicMetrics("raw-firewall", "clean-firewall", 3, 2, 33.3333)),
                List.of(new WindowMetrics(
                        1,
                        Instant.parse("2026-06-22T13:00:00Z"),
                        Instant.parse("2026-06-22T13:01:00Z"),
                        3,
                        2,
                        33.3333,
                        1,
                        1
                ))
        );
        SavingsEstimate estimate = new SavingsEstimator().estimate(metrics, 1_200, 500.0, 30);
        DashboardModel model = new DashboardModel(metrics, estimate, true, 1_200, 500.0);

        Path output = new DashboardRenderer().render(model, tempDir.resolve("kafka-dashboard.html"));

        String html = Files.readString(output);
        assertTrue(html.contains("Kafka simulation mode: simulated topics, no Kafka broker"));
        assertTrue(html.contains("gridgain / embedded Apache Ignite"));
        assertTrue(html.contains("View Controls"));
        assertTrue(html.contains("data-dashboard-toggle=\"topic-metrics\" checked"));
        assertTrue(html.contains("data-dashboard-toggle=\"window-metrics\" checked"));
        assertTrue(html.contains("data-dashboard-toggle=\"ignite-proof\" checked"));
        assertTrue(html.contains("data-dashboard-toggle=\"business-impact\" checked"));
        assertTrue(html.contains("Kafka Simulation Topic Metrics"));
        assertTrue(html.contains("id=\"topic-metrics\" data-dashboard-section"));
        assertTrue(html.contains("raw-firewall"));
        assertTrue(html.contains("clean-firewall"));
        assertTrue(html.contains("Ignite Proof Metrics"));
        assertTrue(html.contains("id=\"ignite-proof\" data-dashboard-section"));
        assertTrue(html.contains("siem-reduction-state"));
        assertTrue(html.contains("PARTITIONED"));
        assertTrue(html.contains("backup count"));
        assertTrue(html.contains("Window Metrics"));
        assertTrue(html.contains("id=\"window-metrics\" data-dashboard-section"));
        assertTrue(html.contains("Windows processed"));
        assertTrue(html.contains("2026-06-22T13:00:00Z"));
        assertTrue(html.contains("Infrastructure Estimate"));
        assertTrue(html.contains("\"topicMetrics\":["));
        assertTrue(html.contains("\"windowMetrics\":["));
        assertTrue(html.contains("\"proofMetrics\":{"));
        assertTrue(html.contains("bar-fill reduction"));
        assertTrue(html.contains("style=\"width:33.33%\""));
        assertSelfContained(html);
    }

    private static LogEvent event(boolean securityRelevant) {
        return LogEvent.observed(
                LogSourceType.FIREWALL,
                Instant.parse("2026-06-22T13:00:00Z"),
                securityRelevant ? "SECURITY" : "BENIGN",
                "payload",
                securityRelevant,
                securityRelevant ? "security:key" : "key"
        );
    }

    private static void assertSelfContained(String html) {
        assertFalse(html.contains("<script src="));
        assertFalse(html.contains("<link"));
        assertFalse(html.contains("href="));
        assertFalse(html.contains("http://"));
        assertFalse(html.contains("https://"));
    }
}
