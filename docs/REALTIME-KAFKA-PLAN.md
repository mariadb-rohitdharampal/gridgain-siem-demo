# Realtime Kafka Integration Plan

## Goal

This branch evaluates how to add real Kafka topics to the SIEM
reduction demo without breaking the current local simulation flow.

The target architecture is:

```text
synthetic or external log producers
  -> real Kafka raw topics
  -> GridGain / Apache Ignite-backed reduction application
  -> real Kafka clean topics
  -> downstream SIEM
  -> dashboard and business impact snapshots
```

The current `direct` and `kafka-sim` modes remain the default-safe demo paths.
Real Kafka will be introduced as an optional mode only.

## Phase K1 Scope

This phase adds architecture documentation and lightweight configuration
scaffolding only. It does not add Kafka runtime behavior, producer logic,
consumer logic, Docker, Kafka Streams, or Kafka Connect runtime components.

## Phase K2 Scope

Phase K2 adds an optional real Kafka producer path for raw topics only. It does
not consume from Kafka, does not reduce events, does not write clean topics, and
does not replace the existing `direct` or `kafka-sim` demo modes.

Run K2 only when a Kafka broker is already available:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-produce --events 10000 --bootstrap-servers localhost:9092 --create-topics
```

K2 output should clearly state that it is producing to real Kafka raw topics and
that no reduction consumer is running yet.

## Phase K3A Scope

Phase K3A adds the Kafka consumer contract foundation without enabling the full
reduction loop. It adds:

- JSON deserialization for the K2 event format.
- Schema version validation.
- A consumed-record model carrying topic, key, partition, offset, and
  `LogEvent`.
- Consumer configuration defaults for group id, offset reset behavior, poll
  timing, and commit strategy.
- Interfaces for future raw-topic consumption and clean-topic publication.

K3A still does not consume Kafka records at runtime, does not reduce real Kafka
records, and does not write clean topics.

## Phase K3B Scope

Phase K3B adds a bounded real Kafka reducer mode:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-reduce --bootstrap-servers localhost:9092 --run-seconds 60 --poll-ms 1000 --window-seconds 60 --create-topics
```

K3B consumes real Kafka `raw-*` topics, assigns events to time windows, publishes
security-relevant events immediately to matching `clean-*` topics, reduces
benign events when windows close, and writes reduced benign output to real
Kafka `clean-*` topics.

The mode is intentionally bounded for demo safety. It runs for the configured
`--run-seconds` value and then force-closes any open windows before shutting
down. It is not an infinite daemon.

K3B does not add dashboard integration yet. Console metrics are the source of
truth for this phase.

K3B still does not use Kafka Streams, Kafka Connect, Docker, SQL, persistence,
or external services beyond an already running Kafka broker.

## Proposed Architecture

The real Kafka path should preserve the existing separation of concerns:

- Log generation produces synthetic firewall, DNS, Windows AD, and Cloud /
  Zero Trust events.
- Kafka topics provide transport between raw telemetry and reduced telemetry.
- The reducer owns the event classification and reduction policy.
- Apache Ignite provides distributed shared reduction state across embedded
  nodes for the demo, and later across GridGain nodes for production-style
  evaluation.
- The dashboard consumes run metrics and business impact estimates.

Topic names should remain aligned with the simulation:

| Source | Raw Topic | Clean Topic |
|--------|-----------|-------------|
| Firewall | `raw-firewall` | `clean-firewall` |
| DNS | `raw-dns` | `clean-dns` |
| Windows AD | `raw-windows-ad` | `clean-windows-ad` |
| Cloud / Zero Trust | `raw-cloud-zero-trust` | `clean-cloud-zero-trust` |

## Why Custom Kafka Consumer / Producer First

Custom Kafka client code is the smallest safe next step because it can reuse
the existing Java reduction services, windowing model, metrics, and dashboard.
It also keeps the demo message focused: Kafka transports the telemetry, while
GridGain / Apache Ignite provides distributed shared reduction state.

The first runtime implementation should use Kafka producer, consumer, and admin
client APIs directly. This keeps the application easy to run, easy to debug,
and incremental.

## Why Kafka Connect Is Deferred

Kafka Connect is strong for moving data between Kafka and external systems.
It is a good future option for integrating real log sources or downstream SIEM
sinks. It is not the best first implementation for the core reduction loop,
because windowed aggregation and Ignite-backed distributed reduction state would require
custom connector tasks and additional worker/runtime packaging.

Kafka Connect should be evaluated after the custom client path proves the raw
topic to clean topic reduction flow.

## Why Kafka Streams Is a Comparison Point

Kafka Streams is a strong Kafka-native option for transformations, stateful
processing, and windowing. For this demo, it is a comparison point rather than
the first implementation because its natural state model is Kafka Streams local
state stores. The SIEM proof point is different: GridGain / Apache Ignite
acts as distributed shared reduction state in front of the SIEM.

Kafka Streams should be considered later if the team wants a Kafka-native
baseline to compare against the Ignite-backed reducer.

## Capability Matrix

| Capability | Local Java Cache | Kafka Streams | Kafka Connect | GridGain / Ignite-Backed Reducer |
|------------|------------------|---------------|---------------|----------------------------------|
| Primary role | Simple local state | Kafka-native stream processing | Ingest and egress integration | Distributed shared reduction state |
| Replaces Kafka | No | No | No | No |
| Real Kafka transport | No | Yes | Yes | Yes, through Kafka clients |
| Distributed reduction state | No | Local task state with changelog recovery | Only through custom connector logic | Yes, via shared Ignite cache |
| Windowed aggregation | Demo-only local logic | Native strength | Possible but custom and awkward | Existing demo logic with Ignite state |
| Best demo use | Fast fallback | Future comparison baseline | Future source or SIEM integration | Primary realtime reduction path |
| Operational complexity | Low | Medium | High | Medium |
| SIEM value alignment | Basic proof | Kafka-centric proof | Integration proof | GridGain/Ignite inline optimization proof |

GridGain / Apache Ignite is positioned as a distributed in-memory state and
optimization layer. It is not positioned as a replacement for Kafka, Splunk,
Elastic, or other infrastructure.

## Configuration Scaffolding

The K1 config model captures the expected knobs for later phases:

- `bootstrapServers`
- `pollMs`
- `runSeconds`
- `createTopics`
- `topicPartitions`
- `topicReplicationFactor`
- raw topic names
- clean topic names

K2 adds `org.apache.kafka:kafka-clients` for producer and admin client support.
Kafka Streams and Kafka Connect dependencies are intentionally not included.

## Phase K2 Serialization

K2 writes Kafka records with:

- key: `<source>|<reductionKey>`
- value: compact JSON string

The JSON value includes:

- `schemaVersion`
- `id`
- `timestamp`
- `source`
- `sourceDisplayName`
- `eventType`
- `type`
- `reductionKey`
- `severity`
- `securityRelevant`
- `representedEventCount`
- `payload`
- `message`

Severity is intentionally simple in this phase: security-relevant events are
marked `SECURITY`; all other events are marked `INFO`.

K3A reads the same schema back into `LogEvent`. Unsupported schema versions and
malformed records fail with explicit errors so K3B can route bad records to a
future error policy instead of failing silently.

## Phase Plan

### K1 - Config and Architecture Docs

Add this plan, README positioning, and lightweight configuration scaffolding.
No runtime Kafka behavior is enabled.

### K2 - Real Kafka Producer for Raw Topics

Add Kafka client dependency and a producer path that writes generated events to
the real raw topics. Keep reduction out of scope for this phase.

Implemented branch behavior:

- optional CLI mode: `--mode kafka-produce`
- broker config: `--bootstrap-servers localhost:9092`
- topic creation: `--create-topics`
- topic defaults: `--topic-partitions 3 --topic-replication-factor 1`
- raw topics only: `raw-firewall`, `raw-dns`, `raw-windows-ad`,
  `raw-cloud-zero-trust`

### K3 - Real Kafka Reducer Consumer / Producer

Add a real Kafka reduction mode that consumes raw topics, preserves
security-relevant events immediately, reduces benign events through the
selected reducer, and writes clean topics. K3 is the first phase that should
produce `clean-*` topics.

K3 is split into:

- K3A - consumer contract foundation, JSON round-trip, and config defaults.
- K3B - bounded runtime consumer loop, Ignite-backed reduction, and clean-topic
  writes.

K3B uses manual synchronous commits with auto-commit disabled. Offsets are
committed only after the clean publisher successfully flushes and there are no
unclosed benign windows waiting to be reduced. At shutdown, open windows are
force-closed, clean output is flushed, and offsets are committed. This is an
at-least-once strategy: a failure after clean-topic publish but before commit
can replay raw records and duplicate clean output. Exactly-once semantics are
outside the K3B scope.

### K4 - Continuous Polling and Graceful Shutdown

Add interval-based execution, bounded demo runs, signal-aware shutdown,
consumer offset handling, and periodic metric snapshots.

### K5 - Dashboard / Live Metrics Snapshots

Add a local live dashboard for continuous real Kafka reducer mode using Java's
built-in `HttpServer`. The dashboard reads periodic reducer snapshots for topic
counts, reduction counts, security preservation, open windows, Ignite proof
metrics, and distributed ownership proof. It is local demo visibility, not
production observability.

### K6 - Kafka Connect Evaluation

Evaluate Kafka Connect for real source ingestion and downstream SIEM export.
Keep it separate from the first custom-client reduction path unless it proves
cleaner operationally.

## Guardrails

- Keep `direct` mode unchanged.
- Keep `kafka-sim` mode as the fallback demo path.
- Add real Kafka only as an optional mode.
- Do not add Docker until a broker runtime is needed.
- If Docker is added later, keep it limited to Kafka for local demos.
- Do not add Kafka Streams unless there is a clear comparison use case.
