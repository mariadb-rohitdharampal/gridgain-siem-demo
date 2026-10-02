package com.gridgain.demo.siem.dashboard.live;

public final class LiveKafkaDashboardRenderer {
    public String html() {
        return """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>GridGain SIEM Live Kafka Dashboard</title>
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
                    }
                    .value {
                      margin-top: 6px;
                      font-size: 28px;
                      font-weight: 700;
                    }
                    .subtle { color: var(--muted); font-size: 13px; }
                    .flow {
                      display: grid;
                      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
                      gap: 10px;
                    }
                    .flow .step {
                      min-height: 84px;
                      border-left: 5px solid var(--accent);
                    }
                    .flow .step:nth-child(3) { border-left-color: var(--ignite); }
                    table {
                      width: 100%;
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
                    .bar {
                      height: 10px;
                      border-radius: 999px;
                      background: #dbe2ea;
                      overflow: hidden;
                      margin-top: 10px;
                    }
                    .bar span {
                      display: block;
                      height: 100%;
                      width: 0;
                      background: var(--accent);
                    }
                    .status {
                      display: inline-block;
                      margin-top: 10px;
                      padding: 5px 9px;
                      border-radius: 999px;
                      background: #e7f7f2;
                      color: #006b55;
                      font-size: 13px;
                      font-weight: 700;
                    }
                    @media (max-width: 680px) {
                      header, main { padding-left: 18px; padding-right: 18px; }
                    }
                  </style>
                </head>
                <body>
                  <header>
                    <h1>GridGain SIEM Live Kafka Dashboard</h1>
                    <p>Real Kafka raw topics to clean topics with embedded Apache Ignite reduction state.</p>
                    <span class="status" id="dashboard-status">Waiting for first snapshot</span>
                  </header>
                  <main>
                    <section>
                      <h2>Architecture Flow</h2>
                      <div class="flow">
                        <div class="card step"><strong>Raw Kafka topics</strong><div class="subtle">Firewall, DNS, Windows AD, cloud/zero trust</div></div>
                        <div class="card step"><strong>Kafka reducer</strong><div class="subtle">Windowed processing and security bypass</div></div>
                        <div class="card step"><strong>Apache Ignite</strong><div class="subtle">Distributed reduction state</div></div>
                        <div class="card step"><strong>Clean Kafka topics</strong><div class="subtle">Reduced telemetry for downstream SIEM</div></div>
                      </div>
                    </section>
                    <section>
                      <h2>Live Metrics</h2>
                      <div class="grid">
                        <div class="card"><div class="label">Records Seen</div><div class="value" data-field="recordsSeen">0</div></div>
                        <div class="card"><div class="label">Valid Consumed Events</div><div class="value" data-field="validConsumedEvents">0</div></div>
                        <div class="card"><div class="label">Produced Clean Events</div><div class="value" data-field="producedCleanEvents">0</div></div>
                        <div class="card">
                          <div class="label">Reduction</div>
                          <div class="value"><span data-field="reductionPercentage">0.00</span>%</div>
                          <div class="bar"><span id="reduction-bar"></span></div>
                        </div>
                        <div class="card"><div class="label">Malformed Records</div><div class="value" data-field="malformedRecords">0</div></div>
                        <div class="card"><div class="label">Security Preserved</div><div class="value"><span data-field="securityEventsPreserved">0</span> / <span data-field="securityEventsObserved">0</span></div></div>
                        <div class="card"><div class="label">Windows Processed</div><div class="value" data-field="windowsProcessed">0</div></div>
                        <div class="card"><div class="label">Open Windows</div><div class="value" data-field="openWindows">0</div></div>
                      </div>
                    </section>
                    <section>
                      <h2>Topic Counts</h2>
                      <div class="grid">
                        <div>
                          <h3>Consumed Raw Topics</h3>
                          <table><tbody id="raw-topic-counts"><tr><td>No data yet</td><td>0</td></tr></tbody></table>
                        </div>
                        <div>
                          <h3>Produced Clean Topics</h3>
                          <table><tbody id="clean-topic-counts"><tr><td>No data yet</td><td>0</td></tr></tbody></table>
                        </div>
                      </div>
                    </section>
                    <section>
                      <h2>Ignite Proof Metrics</h2>
                      <table><tbody id="proof-metrics"><tr><td>No Ignite proof snapshot yet</td><td>n/a</td></tr></tbody></table>
                    </section>
                    <noscript>
                      <section class="card">
                        JavaScript is disabled. The live dashboard HTML loaded, but metrics require local polling of /api/progress.
                      </section>
                    </noscript>
                  </main>
                  <script>
                  (function () {
                    function setText(selector, value) {
                      document.querySelectorAll(selector).forEach(function (node) {
                        node.textContent = value;
                      });
                    }
                    function number(value) {
                      return Number(value || 0).toLocaleString();
                    }
                    function percent(value) {
                      return Number(value || 0).toFixed(2);
                    }
                    function renderMap(bodyId, values) {
                      var body = document.getElementById(bodyId);
                      var keys = Object.keys(values || {});
                      if (keys.length === 0) {
                        body.innerHTML = '<tr><td>No data yet</td><td>0</td></tr>';
                        return;
                      }
                      body.innerHTML = keys.map(function (key) {
                        return '<tr><td>' + escapeHtml(key) + '</td><td>' + escapeHtml(String(values[key])) + '</td></tr>';
                      }).join('');
                    }
                    function escapeHtml(value) {
                      return value
                        .replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;')
                        .replace(/"/g, '&quot;');
                    }
                    function apply(data) {
                      setText('[data-field="recordsSeen"]', number(data.recordsSeen));
                      setText('[data-field="validConsumedEvents"]', number(data.validConsumedEvents));
                      setText('[data-field="producedCleanEvents"]', number(data.producedCleanEvents));
                      setText('[data-field="reductionPercentage"]', percent(data.reductionPercentage));
                      setText('[data-field="malformedRecords"]', number(data.malformedRecords));
                      setText('[data-field="securityEventsObserved"]', number(data.securityEventsObserved));
                      setText('[data-field="securityEventsPreserved"]', number(data.securityEventsPreserved));
                      setText('[data-field="windowsProcessed"]', number(data.windowsProcessed));
                      setText('[data-field="openWindows"]', number(data.openWindows));
                      document.getElementById('reduction-bar').style.width = Math.max(0, Math.min(100, Number(data.reductionPercentage || 0))) + '%';
                      renderMap('raw-topic-counts', data.consumedRawTopicCounts);
                      renderMap('clean-topic-counts', data.producedCleanTopicCounts);
                      renderMap('proof-metrics', data.proofMetrics);
                      document.getElementById('dashboard-status').textContent = 'Last update: ' + data.lastUpdatedAt;
                    }
                    async function refresh() {
                      try {
                        var response = await fetch('/api/progress', { cache: 'no-store' });
                        if (response.ok) {
                          apply(await response.json());
                        }
                      } catch (error) {
                        document.getElementById('dashboard-status').textContent = 'Waiting for dashboard server';
                      }
                    }
                    refresh();
                    setInterval(refresh, 2000);
                  }());
                  </script>
                </body>
                </html>
                """;
    }
}
