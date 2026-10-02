# Demo Objective

Use this runbook to guide internal walkthroughs and customer-facing demos of the
GridGain SIEM Reduction Demo.

The demo is designed to show how an inline reduction layer can:

- Reduce SIEM-bound telemetry volume before data reaches Splunk or Elastic.
- Preserve security-relevant events.
- Demonstrate distributed reduction state using a local 3-node embedded GridGain cluster.
- Show business impact through dashboard metrics, savings estimates, and
  retention impact.

# Audience

- Enterprise architects
- Security and platform stakeholders
- Ryan Fetter
- Chad Davis
- Internal MariaDB/GridGain teams

# Demo Scope

Included:

- Simulated telemetry
- Simulated Kafka topics
- Streaming/windowed processing
- 3-node embedded GridGain cluster
- Interactive dashboard
- Business impact model

Not included:

- Real Kafka
- Real SIEM integration
- GridGain 8 runtime
- Multi-host deployment

# Recommended Demo Command

Run from the repository root:

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-sim --streaming --window-seconds 60 --dashboard
```

# Demo Flow

## Step 1: Show Architecture

Open the architecture diagram in the README and explain the flow:

```text
simulated log sources
  -> simulated raw Kafka topics
  -> reduction layer
  -> simulated clean Kafka topics
  -> downstream SIEM
  -> dashboard and business impact view
```

## Step 2: Explain The SIEM Problem

SIEM platforms become expensive and harder to operate when every repetitive
benign event is indexed. The demo positions GridGain / Apache Ignite as an
inline optimization layer that reduces avoidable volume before Splunk or Elastic
ingest.

## Step 3: Run The Demo

```powershell
.\scripts\run-gridgain-demo.ps1 --mode kafka-sim --streaming --window-seconds 60 --dashboard
```

## Step 4: Review Overall Metrics

Point out:

- Raw events
- Reduced events
- Reduction percentage
- Security events preserved

The most important message: security-relevant events are preserved while
repetitive benign events are summarized.

## Step 5: Review Ignite Proof Metrics

Point out:

- Node count
- Cache mode
- Backup count
- Reduction state buckets

Explain that this is a local embedded proof point for distributed in-memory
state, not a production cluster deployment.

## Step 6: Open Dashboard

Open:

```text
target/demo-dashboard.html
```

No web server is required. The dashboard is a self-contained static HTML file.

## Step 7: Review Dashboard Sections

Review:

- Topic metrics
- Window metrics
- Business impact
- Retention impact

Use the dashboard toggles to show or hide topic, window, Ignite proof, and
business impact sections.

## Step 8: Discuss Future Production Architecture

Close by explaining the production path:

- Real Kafka consumers and producers
- GridGain 8 runtime evaluation
- Multi-host cluster deployment
- Production SIEM integration
- Operational policy, audit, and deployment packaging

# Key Messages

## Why SIEM Reduction Matters

Reducing repetitive benign telemetry before SIEM ingestion lowers index volume,
storage pressure, and analyst noise. This creates more infrastructure headroom
and can extend searchable retention.

## Why Security Preservation Matters

The reduction layer must not hide or summarize security-relevant events. In this
demo, security events bypass reduction and are counted separately so the
presenter can show preservation explicitly.

## Why Ignite/GridGain Matters

Ignite/GridGain provides an in-memory state layer for high-volume reduction key
tracking, summaries, and distributed reduction state. In the demo, the
3-node embedded Apache Ignite cluster shows the future GridGain path without
requiring external infrastructure.

## Why Kafka Was Simulated

Kafka is simulated to keep the proof-of-concept lightweight and runnable on a
local machine. The simulated raw and clean topics show the intended architecture
without adding brokers, Docker, networking, offsets, or delivery semantics.

# Frequently Asked Questions

## What is a SIEM?

A SIEM is a Security Information and Event Management platform, such as Splunk
or Elastic, used to collect, index, search, correlate, and alert on security and
operational telemetry.

## How does reduction work?

The demo groups repetitive benign events by source and reduction key, then
summarizes safe repetitions into compact reduction summary events. Security
events bypass this logic.

## Why 40%?

The default 40% target is intentionally conservative for a buyer-facing demo. It
shows meaningful reduction without relying on unrealistic synthetic compression.

## How are security events preserved?

Each event has a security-relevance flag. Security-relevant events bypass
reduction immediately and are published to the clean simulated stream unchanged.

## Why GridGain instead of Kafka Streams?

Kafka Streams is valuable for stream processing, but this demo focuses on the
in-memory distributed state layer: reduction state buckets, summary state, and fast
pre-SIEM optimization. In a production architecture, Kafka and GridGain can be
complementary rather than mutually exclusive.

## Is this really distributed?

The demo starts three embedded Apache Ignite server nodes in the same JVM and
uses a partitioned cache with backup configuration. That demonstrates
distributed cache behavior locally, but it is not a multi-host production
deployment.


## What would production look like?

Production would use real Kafka consumers and producers, GridGain 8, a
multi-host cluster, operational policies, SIEM integration, monitoring, audit
controls, and deployment packaging.

# Known Limitations

- Kafka is simulated.
- Classification policy is simulated.
- The cluster is embedded and local.
- The project is proof-of-concept scope.
- There is no real SIEM integration.
- There are no production audit or compliance workflows.

# Recommended Next Steps

- Internal review with Ryan Fetter
- Internal review with Chad Davis
- Customer walkthrough
- Future GridGain 8 evaluation
