# Kill It Twice — v1 Specification

## Objective

Build a local data-replication system that streams relational source records to an Elasticsearch current-state index and a RabbitMQ event stream consumed by an independent consumer. It must support initial backfill and continuous incremental changes concurrently, while making crash recovery, sink outages, partial failures, and operational state visible. Prisma provides the typed relational model; explicit SQL is retained for ordered outbox and checkpoint operations.

## Delivery contract

The system provides at-least-once delivery. It achieves effectively-once final state using durable per-sink checkpoints, deterministic record/event IDs, source revisions, Elasticsearch idempotency, durable RabbitMQ messages, and consumer deduplication. It does not claim distributed exactly-once transactions across PostgreSQL, Elasticsearch, and RabbitMQ.

## Data flow

PostgreSQL source records write an ordered outbox event. The worker reads the outbox and independently advances the Elasticsearch and RabbitMQ checkpoints only after each destination accepts an event. RabbitMQ has a separate consumer that records event IDs idempotently. Permanent record failures are written to a destination-specific DLQ and can be replayed.

## Reliability rules

- Backfill uses bounded batches and durable progress.
- Replaying the last incomplete batch is permitted and must be harmless.
- Sink failure pauses/retries only that sink and must not cause a CPU busy-loop.
- A partial batch is handled per record, never as an all-or-nothing rollback.
- Each source record has a monotonic version; older writes must not overwrite newer state.
- Every permanent DLQ item retains enough original context for diagnosis and replay.

## Required commands

```bash
docker compose up --build -d
make seed
make verify
```

## Gate mapping

- G1: worker kill during backfill and recovery from durable checkpoints.
- G2: repeated restarts and idempotent final state in both sinks.
- G3: Elasticsearch outage, backoff, and automatic recovery.
- G4: 497 successful records and 3 destination-specific DLQ entries.
- G5: health, throughput, lag, checkpoint, and DLQ information in API/UI.

## Scope decisions

The first version does not include authentication, multi-tenant isolation, production HA, S3, Redis, ClickHouse, Apache NiFi, or a polished product design. The assignment evaluates fault behavior and evidence, so reliability gates take priority over additional integrations and visual features.
