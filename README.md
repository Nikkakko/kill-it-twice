# Kill It Twice

Failure-oriented data replication demo for the Optio senior engineer assignment.

## Quick start

Prerequisites: Docker with Compose, Node 22+, pnpm 10+, and `curl`. PostgreSQL access is managed through Prisma for typed domain operations and explicit SQL for ordered outbox/checkpoint operations.

```bash
pnpm install
docker compose up --build -d
make seed
make verify
```

Open the operator console at <http://localhost:4200>. The API is available at <http://localhost:3000/api/status>; RabbitMQ management is at <http://localhost:15672> with `replication` / `replication`.

## Architecture

```mermaid
flowchart LR
  S[(PostgreSQL source)] --> O[(Ordered outbox)]
  O --> W[NestJS replication worker]
  W -->|checkpoint: search| E[(Elasticsearch current state)]
  W -->|checkpoint: events| R[(RabbitMQ durable exchange)]
  R --> C[Independent consumer]
  W --> D[(DLQ)]
  W --> H[(Heartbeat and metrics)]
  H --> A[NestJS API]
  A --> U[Angular operator UI]
```

## Delivery guarantee

Transport is at-least-once. Final state is effectively-once: retries can repeat delivery attempts, but stable IDs, source revisions, per-sink checkpoints, Elasticsearch document IDs, durable RabbitMQ messages, and consumer deduplication make repeated effects harmless. Distributed exactly-once transactions are not claimed.

## Capacity notes

The default seed is 20,000 records, intentionally large enough to exercise bounded batches without making local verification impractical. The worker uses 100-record reads and never loads the dataset into memory. Measure actual throughput with `make verify`; the likely bottleneck is Elasticsearch indexing, followed by local PostgreSQL I/O. To double throughput, use Elasticsearch bulk requests, increase worker concurrency per sink, and partition the outbox by sequence range while retaining idempotent version checks.

## ADRs

### ADR-001: Ordered PostgreSQL outbox

Decision: represent source changes as a durable monotonic outbox. Alternatives were polling `updated_at` alone or using a broker as the source of truth. The outbox gives deterministic replay and avoids timestamp ties; the tradeoff is extra source writes and storage.

### ADR-002: Independent sink checkpoints

Decision: store one checkpoint per destination. A single global checkpoint would let an Elasticsearch outage incorrectly block or lose RabbitMQ progress. Independent checkpoints improve isolation but require separate reconciliation and observability.

### ADR-003: Effectively-once instead of distributed exactly-once

Decision: use at-least-once transport plus idempotency. A distributed transaction spanning PostgreSQL, Elasticsearch, and RabbitMQ would be complex and still dependent on external system guarantees. Idempotency makes the crash window safe while keeping the design understandable.

### ADR-004: Per-record DLQ handling

Decision: advance successful records and isolate permanent failures. Rolling back an entire batch would violate G4 and unnecessarily delay good records. The tradeoff is more detailed per-item bookkeeping.

## What was not built and why

Authentication, multi-tenant isolation, production high availability, S3 archival, Redis caching, ClickHouse analytics, NiFi orchestration, and polished product UX were intentionally deferred. The assignment prioritizes failure behavior, verification, and operational clarity within the time limit.

## Where AI deviated from the specification

This section is updated during implementation. Initial agent-generated designs are being checked against `SPEC.md`; any changes to checkpointing, ordering, or test behavior will be recorded here with the reason and corrective action.

## Gate status

Run `make verify` for the current evidence. The verifier intentionally reports FAIL rather than hiding an incomplete or unavailable dependency.
