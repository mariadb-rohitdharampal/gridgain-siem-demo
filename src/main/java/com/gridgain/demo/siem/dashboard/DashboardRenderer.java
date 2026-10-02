package com.gridgain.demo.siem.dashboard;

import com.gridgain.demo.siem.event.LogSourceType;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.streaming.WindowMetrics;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

public class DashboardRenderer {
    public Path render(DashboardModel model, Path outputPath) throws IOException {
        Path parent = outputPath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(outputPath, html(model), StandardCharsets.UTF_8);
        return outputPath;
    }

    String html(DashboardModel model) {
        DemoMetrics metrics = model.metrics();
        SavingsEstimate savings = model.savingsEstimate();
        String modeLabel = model.kafkaSimulationMode()
                ? "Kafka simulation mode: simulated topics, no Kafka broker"
                : "Direct mode: generated events reduced without simulated topics";

        return """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>GridGain SIEM SIEM Reduction Dashboard</title>
                  <style>
                    :root {
                      color-scheme: light;
                      --ink: #17202a;
                      --muted: #5c6670;
                      --line: #d8dde3;
                      --accent: #00838F;
                      --ignite: #d44718;
                      --panel: #ffffff;
                      --bg: #f5f7f9;
                      --bar-bg: #dbe2ea;
                    }
                    * { box-sizing: border-box; }
                    body {
                      margin: 0;
                      background: var(--bg);
                      color: var(--ink);
                      font-family: Arial, Helvetica, sans-serif;
                      line-height: 1.45;
                    }
                    header {
                      background: #101820;
                      color: white;
                      padding: 28px 36px;
                      border-bottom: 5px solid var(--accent);
                    }
                    header h1 { margin: 0 0 8px; font-size: 28px; }
                    header p { margin: 0; color: #d9e2ea; }
                    main { padding: 28px 36px 40px; }
                    section { margin-bottom: 28px; }
                    section[hidden] { display: none; }
                    h2 { margin: 0 0 12px; font-size: 20px; }
                    .grid {
                      display: grid;
                      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
                      gap: 14px;
                    }
                    .card {
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 8px;
                      padding: 16px;
                    }
                    .label {
                      color: var(--muted);
                      font-size: 12px;
                      text-transform: uppercase;
                      letter-spacing: 0.04em;
                    }
                    .value {
                      margin-top: 6px;
                      font-size: 28px;
                      font-weight: 700;
                    }
                    .subtle { color: var(--muted); font-size: 13px; }
                    table {
                      width: 100%%;
                      border-collapse: collapse;
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 8px;
                      overflow: hidden;
                    }
                    th, td {
                      padding: 11px 12px;
                      border-bottom: 1px solid var(--line);
                      text-align: left;
                    }
                    th { background: #eef2f5; font-size: 12px; text-transform: uppercase; }
                    tr:last-child td { border-bottom: 0; }
                    .flow {
                      display: grid;
                      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
                      gap: 10px;
                    }
                    .flow .step {
                      border-left: 5px solid var(--accent);
                      min-height: 86px;
                    }
                    .flow .step:nth-child(2) { border-left-color: var(--ignite); }
                    .pill {
                      display: inline-block;
                      padding: 4px 8px;
                      border-radius: 999px;
                      background: #e8f7f3;
                      color: #006b54;
                      font-size: 12px;
                      font-weight: 700;
                    }
                    .controls {
                      display: flex;
                      flex-wrap: wrap;
                      gap: 12px;
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 8px;
                      padding: 14px;
                    }
                    .toggle {
                      display: inline-flex;
                      align-items: center;
                      gap: 8px;
                      font-size: 14px;
                    }
                    .toggle input { inline-size: 18px; block-size: 18px; }
                    .toggle input:disabled + span { color: var(--muted); }
                    .bars {
                      display: grid;
                      gap: 10px;
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 8px;
                      padding: 14px;
                      margin-top: 14px;
                    }
                    .bar-row {
                      display: grid;
                      grid-template-columns: minmax(110px, 180px) 1fr minmax(72px, auto);
                      gap: 10px;
                      align-items: center;
                    }
                    .bar-track {
                      background: var(--bar-bg);
                      border-radius: 999px;
                      height: 14px;
                      overflow: hidden;
                    }
                    .bar-fill {
                      height: 100%%;
                      min-width: 2px;
                      border-radius: 999px;
                      background: var(--accent);
                    }
                    .bar-fill.reduced { background: #4f7cac; }
                    .bar-fill.reduction { background: var(--ignite); }
                    .bar-value {
                      color: var(--muted);
                      font-size: 13px;
                      text-align: right;
                    }
                  </style>
                </head>
                <body>
                  <header>
                    <h1>GridGain SIEM SIEM Reduction Dashboard</h1>
                    <p>%s</p>
                  </header>
                  <main>
                    <section>
                      <span class="pill">%s</span>
                    </section>
                    <section>
                      <h2>Architecture Flow</h2>
                      <div class="flow">
                        %s
                      </div>
                    </section>
                    <section>
                      <h2>View Controls</h2>
                      %s
                    </section>
                    <section>
                      <h2>Executive Metrics</h2>
                      <div class="grid">
                        %s
                      </div>
                      %s
                    </section>
                    <section id="business-impact" data-dashboard-section>
                      <h2>Infrastructure Estimate</h2>
                      <div class="grid">
                        %s
                      </div>
                      <p class="subtle">Assumptions: %,d bytes per event, $%.2f per TB, %d baseline retention days.</p>
                    </section>
                    %s
                    %s
                    %s
                    <section>
                      <h2>Per-Source Metrics</h2>
                      %s
                    </section>
                  </main>
                  %s
                  %s
                </body>
                </html>
                """.formatted(
                escape(modeLabel),
                escape(metrics.reducerMode()),
                architectureFlow(model),
                viewControls(metrics),
                executiveCards(metrics),
                rawReducedBars(metrics),
                savingsCards(savings),
                model.averageEventBytes(),
                model.costPerTb(),
                savings.baselineRetentionDays(),
                topicMetricsSection(metrics),
                igniteProofSection(metrics),
                windowMetricsSection(metrics),
                sourceMetricsTable(metrics),
                dashboardDataScript(model),
                toggleScript()
        );
    }

    private static String executiveCards(DemoMetrics metrics) {
        return card("Raw events", formatLong(metrics.rawEvents()))
                + card("Reduced events", formatLong(metrics.reducedEvents()))
                + card("Reduction", formatPercent(metrics.reductionPercentage()))
                + card("Security events preserved", "%s / %s".formatted(
                formatLong(metrics.securityEventsPreserved()),
                formatLong(metrics.securityEventsObserved())
        ));
    }

    private static String savingsCards(SavingsEstimate savings) {
        return card("TB/day saved", formatDecimal(savings.tbPerDaySaved()))
                + card("TB/month saved", formatDecimal(savings.tbPerMonthSaved()))
                + card("Estimated monthly savings", "$" + formatDecimal(savings.estimatedMonthlySavings()))
                + card("Retention extension", "%.1f days (+%.1f)".formatted(
                savings.estimatedRetentionDays(),
                savings.retentionExtensionDays()
        ));
    }

    private static String rawReducedBars(DemoMetrics metrics) {
        double reducedWidth = metrics.rawEvents() == 0
                ? 0.0
                : (metrics.reducedEvents() * 100.0) / metrics.rawEvents();
        return """
                <div class="bars" aria-label="Raw vs reduced event volume">
                  %s
                  %s
                </div>
                """.formatted(
                barRow("Raw events", 100.0, "raw", formatLong(metrics.rawEvents())),
                barRow("Reduced events", reducedWidth, "reduced", formatLong(metrics.reducedEvents()))
        );
    }

    private static String viewControls(DemoMetrics metrics) {
        return """
                <div class="controls" aria-label="Dashboard view controls">
                  %s
                  %s
                  %s
                  %s
                </div>
                """.formatted(
                toggle("topic-metrics", "Topic metrics", !metrics.topicMetrics().isEmpty()),
                toggle("window-metrics", "Window metrics", !metrics.windowMetrics().isEmpty()),
                toggle("ignite-proof", "Ignite proof metrics", !metrics.proofMetrics().isEmpty()),
                toggle("business-impact", "Business impact", true)
        );
    }

    private static String toggle(String sectionId, String label, boolean enabled) {
        String checked = enabled ? " checked" : "";
        String disabled = enabled ? "" : " disabled";
        return """
                <label class="toggle">
                  <input type="checkbox" data-dashboard-toggle="%s"%s%s>
                  <span>%s</span>
                </label>
                """.formatted(escape(sectionId), checked, disabled, escape(label));
    }

    private static String architectureFlow(DashboardModel model) {
        if (model.kafkaSimulationMode()) {
            return flowStep("1", "Raw Kafka topics", "Simulated raw-firewall, raw-dns, raw-windows-ad, raw-cloud-zero-trust")
                    + flowStep("2", "Reduction layer", escape(model.metrics().reducerMode()))
                    + flowStep("3", "Clean Kafka topics", "Simulated clean topics carry reduced SIEM-bound events")
                    + flowStep("4", "Downstream SIEM", "Splunk or Elastic receives lower-volume security telemetry");
        }
        return flowStep("1", "Synthetic telemetry", "Firewall, DNS, Windows AD, and Cloud / Zero Trust events")
                + flowStep("2", "Reduction layer", escape(model.metrics().reducerMode()))
                + flowStep("3", "Clean SIEM-bound stream", "Reduced events and preserved security events")
                + flowStep("4", "Downstream SIEM", "Splunk or Elastic receives lower-volume security telemetry");
    }

    private static String topicMetricsSection(DemoMetrics metrics) {
        if (metrics.topicMetrics().isEmpty()) {
            return "";
        }

        StringBuilder rows = new StringBuilder();
        for (DemoMetrics.TopicMetrics topicMetric : metrics.topicMetrics()) {
            rows.append("<tr><td>")
                    .append(escape(topicMetric.rawTopicName()))
                    .append("</td><td>")
                    .append(escape(topicMetric.cleanTopicName()))
                    .append("</td><td>")
                    .append(formatLong(topicMetric.rawTopicCount()))
                    .append("</td><td>")
                    .append(formatLong(topicMetric.cleanTopicCount()))
                    .append("</td><td>")
                    .append(formatPercent(topicMetric.reductionPercentage()))
                    .append("</td><td>")
                    .append(reductionBar(topicMetric.reductionPercentage()))
                    .append("</td></tr>");
        }

        return """
                <section id="topic-metrics" data-dashboard-section>
                  <h2>Kafka Simulation Topic Metrics</h2>
                  <table>
                    <thead><tr><th>Raw topic</th><th>Clean topic</th><th>Raw count</th><th>Clean count</th><th>Reduction</th><th>Visual</th></tr></thead>
                    <tbody>%s</tbody>
                  </table>
                </section>
                """.formatted(rows);
    }

    private static String igniteProofSection(DemoMetrics metrics) {
        if (metrics.proofMetrics().isEmpty()) {
            return "";
        }

        StringBuilder rows = new StringBuilder();
        for (Map.Entry<String, String> entry : metrics.proofMetrics().entrySet()) {
            rows.append("<tr><td>")
                    .append(escape(entry.getKey()))
                    .append("</td><td>")
                    .append(escape(entry.getValue()))
                    .append("</td></tr>");
        }

        return """
                <section id="ignite-proof" data-dashboard-section>
                  <h2>Ignite Proof Metrics</h2>
                  <table>
                    <thead><tr><th>Metric</th><th>Value</th></tr></thead>
                    <tbody>%s</tbody>
                  </table>
                </section>
                """.formatted(rows);
    }

    private static String windowMetricsSection(DemoMetrics metrics) {
        if (metrics.windowMetrics().isEmpty()) {
            return "";
        }

        StringBuilder rows = new StringBuilder();
        for (WindowMetrics windowMetric : metrics.windowMetrics()) {
            rows.append("<tr><td>")
                    .append(windowMetric.sequence())
                    .append("</td><td>")
                    .append(escape(windowMetric.startTime().toString()))
                    .append("</td><td>")
                    .append(escape(windowMetric.endTime().toString()))
                    .append("</td><td>")
                    .append(formatLong(windowMetric.rawEvents()))
                    .append("</td><td>")
                    .append(formatLong(windowMetric.reducedEvents()))
                    .append("</td><td>")
                    .append(formatPercent(windowMetric.reductionPercentage()))
                    .append("</td><td>")
                    .append(reductionBar(windowMetric.reductionPercentage()))
                    .append("</td><td>")
                    .append(formatLong(windowMetric.securityEventsPreserved()))
                    .append(" / ")
                    .append(formatLong(windowMetric.securityEventsObserved()))
                    .append("</td></tr>");
        }

        return """
                <section id="window-metrics" data-dashboard-section>
                  <h2>Window Metrics</h2>
                  <div class="grid">
                    %s
                  </div>
                  <table>
                    <thead><tr><th>Window</th><th>Start</th><th>End</th><th>Raw events</th><th>Reduced events</th><th>Reduction</th><th>Visual</th><th>Security preserved</th></tr></thead>
                    <tbody>%s</tbody>
                  </table>
                </section>
                """.formatted(card("Windows processed", formatLong(metrics.windowMetrics().size())), rows);
    }

    private static String sourceMetricsTable(DemoMetrics metrics) {
        StringBuilder rows = new StringBuilder();
        for (LogSourceType sourceType : LogSourceType.values()) {
            DemoMetrics.SourceMetrics source = metrics.perSourceMetrics().get(sourceType);
            rows.append("<tr><td>")
                    .append(escape(sourceType.displayName()))
                    .append("</td><td>")
                    .append(formatLong(source.rawEvents()))
                    .append("</td><td>")
                    .append(formatLong(source.reducedEvents()))
                    .append("</td><td>")
                    .append(formatPercent(source.reductionPercentage()))
                    .append("</td></tr>");
        }
        return """
                <table>
                  <thead><tr><th>Source</th><th>Raw events</th><th>Reduced events</th><th>Reduction</th></tr></thead>
                  <tbody>%s</tbody>
                </table>
                """.formatted(rows);
    }

    private static String card(String label, String value) {
        return """
                <div class="card">
                  <div class="label">%s</div>
                  <div class="value">%s</div>
                </div>
                """.formatted(escape(label), escape(value));
    }

    private static String flowStep(String number, String title, String description) {
        return """
                <div class="card step">
                  <div class="label">Step %s</div>
                  <div class="value" style="font-size:18px">%s</div>
                  <div class="subtle">%s</div>
                </div>
                """.formatted(escape(number), escape(title), escape(description));
    }

    private static String reductionBar(double percentage) {
        return """
                <div class="bar-track" aria-label="Reduction %s">
                  <div class="bar-fill reduction" style="width:%s"></div>
                </div>
                """.formatted(formatPercent(percentage), formatBarWidth(percentage));
    }

    private static String barRow(String label, double widthPercentage, String className, String value) {
        return """
                <div class="bar-row">
                  <div>%s</div>
                  <div class="bar-track">
                    <div class="bar-fill %s" style="width:%s"></div>
                  </div>
                  <div class="bar-value">%s</div>
                </div>
                """.formatted(escape(label), escape(className), formatBarWidth(widthPercentage), escape(value));
    }

    private static String formatLong(long value) {
        return "%,d".formatted(value);
    }

    private static String formatPercent(double value) {
        return "%.2f%%".formatted(value);
    }

    private static String formatDecimal(double value) {
        return "%,.2f".formatted(value);
    }

    private static String formatBarWidth(double value) {
        double clamped = Math.max(0.0, Math.min(100.0, value));
        return String.format(Locale.ROOT, "%.2f%%", clamped);
    }

    private static String dashboardDataScript(DashboardModel model) {
        return """
                  <script type="application/json" id="dashboard-data">
                  %s
                  </script>
                """.formatted(dashboardDataJson(model));
    }

    private static String dashboardDataJson(DashboardModel model) {
        DemoMetrics metrics = model.metrics();
        SavingsEstimate savings = model.savingsEstimate();
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"modeLabel\":").append(jsonString(model.kafkaSimulationMode()
                ? "Kafka simulation mode: simulated topics, no Kafka broker"
                : "Direct mode: generated events reduced without simulated topics")).append(",");
        json.append("\"reducerMode\":").append(jsonString(metrics.reducerMode())).append(",");
        json.append("\"rawEvents\":").append(metrics.rawEvents()).append(",");
        json.append("\"reducedEvents\":").append(metrics.reducedEvents()).append(",");
        json.append("\"reductionPercentage\":").append(jsonNumber(metrics.reductionPercentage())).append(",");
        json.append("\"securityEventsPreserved\":").append(metrics.securityEventsPreserved()).append(",");
        json.append("\"securityEventsObserved\":").append(metrics.securityEventsObserved()).append(",");
        json.append("\"topicMetrics\":[");
        for (int i = 0; i < metrics.topicMetrics().size(); i++) {
            DemoMetrics.TopicMetrics topicMetric = metrics.topicMetrics().get(i);
            if (i > 0) {
                json.append(",");
            }
            json.append("{")
                    .append("\"rawTopicName\":").append(jsonString(topicMetric.rawTopicName())).append(",")
                    .append("\"cleanTopicName\":").append(jsonString(topicMetric.cleanTopicName())).append(",")
                    .append("\"rawTopicCount\":").append(topicMetric.rawTopicCount()).append(",")
                    .append("\"cleanTopicCount\":").append(topicMetric.cleanTopicCount()).append(",")
                    .append("\"reductionPercentage\":").append(jsonNumber(topicMetric.reductionPercentage()))
                    .append("}");
        }
        json.append("],");
        json.append("\"windowMetrics\":[");
        for (int i = 0; i < metrics.windowMetrics().size(); i++) {
            WindowMetrics windowMetric = metrics.windowMetrics().get(i);
            if (i > 0) {
                json.append(",");
            }
            json.append("{")
                    .append("\"sequence\":").append(windowMetric.sequence()).append(",")
                    .append("\"startTime\":").append(jsonString(windowMetric.startTime().toString())).append(",")
                    .append("\"endTime\":").append(jsonString(windowMetric.endTime().toString())).append(",")
                    .append("\"rawEvents\":").append(windowMetric.rawEvents()).append(",")
                    .append("\"reducedEvents\":").append(windowMetric.reducedEvents()).append(",")
                    .append("\"reductionPercentage\":").append(jsonNumber(windowMetric.reductionPercentage())).append(",")
                    .append("\"securityEventsObserved\":").append(windowMetric.securityEventsObserved()).append(",")
                    .append("\"securityEventsPreserved\":").append(windowMetric.securityEventsPreserved())
                    .append("}");
        }
        json.append("],");
        json.append("\"proofMetrics\":{");
        int proofIndex = 0;
        for (Map.Entry<String, String> entry : metrics.proofMetrics().entrySet()) {
            if (proofIndex > 0) {
                json.append(",");
            }
            json.append(jsonString(entry.getKey())).append(":").append(jsonString(entry.getValue()));
            proofIndex++;
        }
        json.append("},");
        json.append("\"businessImpact\":{")
                .append("\"tbPerDaySaved\":").append(jsonNumber(savings.tbPerDaySaved())).append(",")
                .append("\"tbPerMonthSaved\":").append(jsonNumber(savings.tbPerMonthSaved())).append(",")
                .append("\"estimatedMonthlySavings\":").append(jsonNumber(savings.estimatedMonthlySavings())).append(",")
                .append("\"estimatedRetentionDays\":").append(jsonNumber(savings.estimatedRetentionDays())).append(",")
                .append("\"retentionExtensionDays\":").append(jsonNumber(savings.retentionExtensionDays()))
                .append("}");
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

    private static String toggleScript() {
        return """
                  <script>
                  (function () {
                    document.querySelectorAll('[data-dashboard-toggle]').forEach(function (toggle) {
                      var sectionId = toggle.getAttribute('data-dashboard-toggle');
                      var section = document.getElementById(sectionId);
                      if (!section) {
                        toggle.disabled = true;
                        return;
                      }
                      section.hidden = !toggle.checked;
                      toggle.addEventListener('change', function () {
                        section.hidden = !toggle.checked;
                      });
                    });
                  }());
                  </script>
                """;
    }

    private static String escape(String value) {
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
