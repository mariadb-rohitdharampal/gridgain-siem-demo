# Kafka Demo Runbook

## Objective

Run the real Kafka portion of the SIEM reduction demo locally:

```text
real Kafka raw topics
  -> bounded or continuous GridGain / Apache Ignite-backed reducer
  -> real Kafka clean topics
```

This runbook assumes you already have Apache Kafka installed locally. It does
not require Docker, Kafka Streams, or Kafka Connect.

## Start Local Kafka

From your Kafka installation directory, start a single local broker using the
Kafka quick-start configuration that matches your installed Kafka version.

For Kafka distributions using KRaft mode:

```powershell
.\bin\windows\kafka-storage.bat random-uuid
```

Copy the generated UUID and format local storage:

```powershell
.\bin\windows\kafka-storage.bat format -t <UUID> -c .\config\kraft\server.properties
```

Start the broker:

```powershell
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```

Leave that terminal running.

## Topic Commands

Set a convenience variable from the Kafka installation directory:

```powershell
$bootstrap = "localhost:9092"
```

Create raw topics:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic raw-firewall --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic raw-dns --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic raw-windows-ad --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic raw-cloud-zero-trust --partitions 3 --replication-factor 1
```

Create clean topics:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic clean-firewall --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic clean-dns --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic clean-windows-ad --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --create --if-not-exists --topic clean-cloud-zero-trust --partitions 3 --replication-factor 1
```

List topics:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --list
```

Describe topics:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --describe --topic raw-firewall
.\bin\windows\kafka-topics.bat --bootstrap-server $bootstrap --describe --topic clean-firewall
```

## Produce Raw Events

From the project directory:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-produce --events 10000 --bootstrap-servers localhost:9092 --create-topics
```

K2/K3 raw producer output writes only to:

- `raw-firewall`
- `raw-dns`
- `raw-windows-ad`
- `raw-cloud-zero-trust`

## Inspect Raw Topics

From the Kafka installation directory:

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic raw-firewall --from-beginning --max-messages 5
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic raw-dns --from-beginning --max-messages 5
```

## Run Bounded Kafka Reducer

Use a unique consumer group id for each demo run to avoid offset confusion.
For example:

```powershell
$group = "siem-demo-$env:USERNAME-$(Get-Date -Format yyyyMMddHHmmss)"
```

Run the bounded reducer:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --bootstrap-servers localhost:9092 --run-seconds 60 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
```

This starts the embedded Apache Ignite reducer path by default. It consumes
`raw-*` topics and writes reduced output to matching `clean-*` topics.

The default reducer report is concise for customer demos. It shows overall
Kafka records, valid consumed events, produced clean events, reduction
percentage, malformed records, security preservation, windows processed, top
five windows by raw volume, topic counts, and Ignite proof metrics. In
gridgain mode, Ignite proof metrics include distributed primary and backup
reduction state ownership by embedded node. Raw reduction keys are not printed
by default.

Use `--verbose-windows` only for diagnostics when you need every individual
window line.

## Run Continuous Kafka Reducer

Continuous mode keeps `kafka-reduce` running like a service until Ctrl+C:

```powershell
$group = "siem-demo-cont-$env:USERNAME-$(Get-Date -Format yyyyMMddHHmmss)"
.\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --continuous --metrics-interval-seconds 10 --bootstrap-servers localhost:9092 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
```

Add the K5 live dashboard for a customer-facing continuous demo:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --continuous --dashboard --dashboard-port 8080 --metrics-interval-seconds 10 --bootstrap-servers localhost:9092 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
```

Open the dashboard while the reducer is running:

```text
http://localhost:8080/
```

The live dashboard is a local demo view. It uses Java's built-in `HttpServer`,
has no auth or TLS, and is not a production observability system.

`--run-seconds 0` is also accepted as a continuous-mode alias:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --run-seconds 0 --metrics-interval-seconds 10 --bootstrap-servers localhost:9092 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
```

Every metrics interval, the reducer prints one concise cumulative progress
line:

```text
Kafka reduce progress: recordsSeen=..., valid=..., clean=..., reduction=..., malformed=..., security=.../..., windows=..., openWindows=..., primaryOwnerNodes=3 / 3
```

Press Ctrl+C to stop. The reducer requests a graceful shutdown, wakes the Kafka
consumer if it is polling, closes open windows, publishes remaining summary
events, flushes the clean producer, commits offsets, closes Kafka resources, and
then lets the embedded Ignite nodes close.

## Inspect Clean Topics

From the Kafka installation directory:

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic clean-firewall --from-beginning --max-messages 10
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic clean-dns --from-beginning --max-messages 10
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic clean-windows-ad --from-beginning --max-messages 10
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic clean-cloud-zero-trust --from-beginning --max-messages 10
```

For a quick customer-demo check, inspect a small clean-topic sample:

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic clean-firewall --from-beginning --max-messages 5
```

## Reset Offsets

List consumer groups:

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server $bootstrap --list
```

Describe the reducer group:

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server $bootstrap --describe --group $group
```

Reset offsets to the beginning for a demo group:

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server $bootstrap --group $group --reset-offsets --to-earliest --all-topics --execute
```

If Kafka reports that the group is active, stop the reducer process first.

## End-To-End Smoke Flow

1. Start Kafka.
2. Produce raw events:

   ```powershell
   .\scripts\run-gridgain-demo.ps1 --mode kafka-produce --events 10000 --bootstrap-servers localhost:9092 --create-topics
   ```

3. Inspect raw topics with `kafka-console-consumer`.
4. Run the bounded reducer with a unique group id:

   ```powershell
   $group = "siem-demo-$env:USERNAME-$(Get-Date -Format yyyyMMddHHmmss)"
   .\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --bootstrap-servers localhost:9092 --run-seconds 60 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
   ```

5. Inspect clean topics with `kafka-console-consumer`.
6. Review console metrics:
   - Kafka records seen
   - Valid consumed events
   - Produced clean events
   - Reduction percentage
   - Malformed records
   - Security events preserved
   - Windows processed and top five windows by raw volume
   - Ignite proof metrics

For long-running service-style demos, replace step 4 with continuous mode:

```powershell
$group = "siem-demo-cont-$env:USERNAME-$(Get-Date -Format yyyyMMddHHmmss)"
.\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --continuous --metrics-interval-seconds 10 --bootstrap-servers localhost:9092 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
```

For the live dashboard version, add `--dashboard --dashboard-port 8080` and open
`http://localhost:8080/`.

Stop it with Ctrl+C after the progress lines have shown clean output and Ignite
ownership proof.

## Concise Customer Demo Flow

1. Start Kafka and confirm `$bootstrap = "localhost:9092"`.
2. Produce raw events:

   ```powershell
   .\scripts\run-gridgain-demo.ps1 --mode kafka-produce --events 10000 --bootstrap-servers localhost:9092 --create-topics
   ```

3. Create a fresh consumer group id:

   ```powershell
   $group = "siem-demo-$env:USERNAME-$(Get-Date -Format yyyyMMddHHmmss)"
   ```

4. Run the bounded reducer:

   ```powershell
   .\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --bootstrap-servers localhost:9092 --run-seconds 60 --poll-ms 1000 --window-seconds 60 --consumer-group-id $group --create-topics
   ```

5. Point out the concise summary:
   - Kafka records seen and valid consumed events
   - Produced clean events and reduction percentage
   - Malformed records reported separately
   - Security events preserved
   - Top five busiest windows
   - 3-node Ignite proof metrics
   - Primary and backup reduction state ownership by node

6. Inspect clean output:

   ```powershell
   .\bin\windows\kafka-console-consumer.bat --bootstrap-server $bootstrap --topic clean-firewall --from-beginning --max-messages 5
   ```

## Successful Smoke-Test Result

A validated 10,000-event smoke run produced a concise result in this shape:

- Kafka records seen: 10,000
- Valid consumed events: 10,000
- Produced clean events: approximately 6,510
- Reduction: 34.90%
- Malformed records: 0
- Security events preserved: 137 / 137
- Embedded Ignite nodes: 3
- Primary owner nodes observed: 3 / 3
- Backup owner nodes observed: 3 / 3

Actual counts can vary with demo parameters, existing Kafka offsets, and topic
contents. Use a unique `--consumer-group-id` for each customer walkthrough to
avoid offset confusion.

## Distributed Ownership Proof

K3C adds proof that the GridGain / Apache Ignite-backed reducer is using
distributed shared state rather than a single local Java map. The reducer stores
benign reduction keys in a partitioned
`IgniteCache<String, ReductionBucket>`. The console report shows:

- Primary owner nodes observed, for example `3 / 3`
- Backup owner nodes observed, for example `3 / 3`
- Primary reduction state ownership by node, for example `node-1=8, node-2=7, node-3=7`
- Backup reduction state ownership by node, for example `node-1=7, node-2=8, node-3=7`
- Partitions touched
- Local primary and backup cache entries by node

This is the key difference versus the in-memory Java cache fallback: ownership
is distributed across an Ignite partitioned cache with backup copies. The demo
still runs three embedded Ignite server nodes in the same JVM for local
portability; it is not a multi-host production cluster.

## Sample Clean-Topic JSON

Preserved security event:

```json
{"schemaVersion":1,"id":"4f2a0f78-7db5-40b4-93c7-0a4a60c19c9d","timestamp":"2026-06-22T13:00:00Z","source":"FIREWALL","sourceDisplayName":"Firewall","eventType":"FIREWALL_BLOCK","type":"FIREWALL_BLOCK","reductionKey":"security:firewall:block:203.0.113.10","severity":"SECURITY","securityRelevant":true,"representedEventCount":1,"payload":"CEF:0|Acme|Firewall|1.0|900|Blocked outbound suspicious destination|8|src=10.10.9.20 dst=203.0.113.10 dpt=4444 proto=TCP act=blocked cs1Label=threat cs1=command-and-control","message":"CEF:0|Acme|Firewall|1.0|900|Blocked outbound suspicious destination|8|src=10.10.9.20 dst=203.0.113.10 dpt=4444 proto=TCP act=blocked cs1Label=threat cs1=command-and-control"}
```

Reduction summary event:

```json
{"schemaVersion":1,"id":"98d0481f-8f0f-4c52-bcc8-907bc733f6a8","timestamp":"2026-06-22T13:00:04Z","source":"DNS","sourceDisplayName":"DNS","eventType":"REDUCTION_SUMMARY","type":"REDUCTION_SUMMARY","reductionKey":"dns:query:A:intranet.example.internal","severity":"INFO","securityRelevant":false,"representedEventCount":42,"payload":"REDUCTION_SUMMARY source=\"DNS\" reductionKey=\"dns:query:A:intranet.example.internal\" firstSeen=\"2026-06-22T13:02:00Z\" lastSeen=\"2026-06-22T13:02:04Z\" representedEvents=42 count=42","message":"REDUCTION_SUMMARY source=\"DNS\" reductionKey=\"dns:query:A:intranet.example.internal\" firstSeen=\"2026-06-22T13:02:00Z\" lastSeen=\"2026-06-22T13:02:04Z\" representedEvents=42 count=42"}
```

## Semantics

K3B/K4 use manual synchronous commits with Kafka auto-commit disabled. Offsets
are committed after successful clean-topic flushes when no benign windows remain
open. In continuous mode, open-window offsets may lag until the window closes or
until Ctrl+C graceful shutdown force-closes open windows. During graceful
shutdown, the reducer closes open windows, flushes clean topic output, commits
offsets, closes Kafka resources, and then closes the embedded Ignite nodes.

In K5 live dashboard mode, the local dashboard server is also stopped during
shutdown. It is meant for same-machine demo viewing only and does not provide
authentication, TLS, audit logging, or enterprise monitoring retention.

This is at-least-once behavior. If the reducer publishes clean events and then
fails before committing offsets, raw records can be replayed and clean output
can be duplicated. Exactly-once behavior is not part of this demo phase. Kafka
Streams and Kafka Connect are still intentionally out of scope.

## Troubleshooting

`Connection refused` or `TimeoutException`:

- Confirm Kafka is running.
- Confirm the broker listens on `localhost:9092`.
- Confirm no firewall or VPN policy is blocking the local broker port.

Reducer consumes zero records:

- Use a unique `--consumer-group-id` for each demo run.
- Produce fresh raw events.
- Reset offsets for the demo group after stopping the reducer.

Topic does not exist:

- Run with `--create-topics`.
- Or create the topics manually with `kafka-topics.bat`.

Duplicate clean events:

- This is expected after a failure or offset reset because K3B/K4 are
  at-least-once.

Malformed record count is greater than zero:

- Inspect the raw topic payload.
- Confirm records were produced by `--mode kafka-produce`.
- Confirm the JSON has `schemaVersion: 1`.

Ignite startup fails:

- Confirm no other local Ignite process is using the selected ports.
- Retry the command; the demo searches for available loopback discovery ports.
