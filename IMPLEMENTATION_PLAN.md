# Kill It Twice — Implementation Plan

This document turns `tech.md` into implementation phases. Reliability gates take priority over product polish.

## Phase 0 — Specification and workspace

- Create `SPEC.md` and `AGENTS.md` before application code.
- Establish the Turborepo workspace with Angular, NestJS API, NestJS worker, and independent consumer apps.
- Add shared contracts and database packages.
- Add Docker Compose, Make targets, environment documentation, and the README outline.

**Exit criteria:** `pnpm install` and the workspace build are reproducible; the first Git commit contains the specification before implementation.

## Phase 1 — Durable source and infrastructure

- Run PostgreSQL, RabbitMQ, Elasticsearch, API, worker, consumer, and web through Compose.
- Create source records, ordered outbox events, per-sink checkpoints, worker heartbeat, consumed-event deduplication, and DLQ tables.
- Add deterministic seed and source-change simulation endpoints.

**Exit criteria:** `docker compose up` starts the system and `make seed` creates a bounded, stream-sized dataset.

## Phase 2 — Replication engine

- Read the ordered outbox with bounded batches.
- Process Elasticsearch and RabbitMQ independently.
- Persist a checkpoint only after the destination confirms the item.
- Treat replay as safe through stable event IDs, document IDs, source versions, durable messages, and an idempotent consumer.
- Let backfill events and live incremental events coexist in the same ordered stream.

**Exit criteria:** worker restart resumes from durable progress; a newer source revision cannot be replaced by an older write.

## Phase 3 — Failure handling

- Add retry/backoff behavior for temporary sink failures.
- Pause the affected sink during an outage without a busy-loop.
- Classify permanent record errors individually.
- Write rejected records to the DLQ with payload, source revision, destination, sequence, error, and replay metadata.
- Add DLQ replay through the API and UI.

**Exit criteria:** one failed sink does not lose data for the other; 497 of 500 records can succeed while 3 go to the DLQ.

## Phase 4 — Automated verification

- Implement `make verify` as a deterministic integration test runner.
- Kill the worker during backfill and verify restart/recovery.
- Repeat kill/restart cycles and compare source, checkpoints, and destination state.
- Stop Elasticsearch, verify backpressure, restart it, and verify recovery.
- Inject a controlled partial batch failure and verify successful records plus DLQ count.
- Validate observability endpoints and fail with a non-zero exit code when a gate fails.

**Exit criteria:** every gate produces a clear PASS/FAIL result with measured evidence.

## Phase 5 — Operational API and Angular console

- Show checkpoints, pending work, throughput, lag, DLQ count, and dependency health.
- Provide searchable replicated-record browsing.
- Provide start/pause/resume, configuration, and DLQ replay controls.
- Provide sink outage, invalid-record, and source-change simulations.

**Exit criteria:** a new operator can understand and control the pipeline without reading code.

## Phase 6 — Documentation and capacity review

- Add an architecture diagram showing the outbox, checkpoints, sinks, consumer, retry path, and DLQ.
- Add at least four ADRs.
- Document the delivery guarantee, capacity measurements, bottleneck, and scaling plan.
- Document intentionally omitted features.
- Record at least two real AI deviations from `SPEC.md`.
- Review Git history to confirm specification-first development.

**Exit criteria:** README, SPEC, AGENTS, verifier output, and Git history tell the same story.

## Defaults

- Stack: Turborepo, Angular, NestJS, PostgreSQL, RabbitMQ, Elasticsearch, Docker.
- Delivery: at-least-once transport with effectively-once final state.
- Checkpoints: independent per destination.
- Ordering: durable monotonic outbox sequence plus source version.
- Scope: S3, Redis, ClickHouse, Apache NiFi, production HA, authentication, and polished product UX are deferred.
