# Agent Instructions

## Repository conventions

- Use pnpm and Turborepo commands from the repository root.
- Keep Angular code in `apps/web`, NestJS HTTP code in `apps/api`, replication code in `apps/worker`, and the independent RabbitMQ consumer in `apps/consumer`.
- Put cross-app TypeScript contracts in `packages/contracts` and database access/schema helpers in `packages/database`.
- Keep all services runnable through Docker Compose.

## Reliability rules

- Never store a replication checkpoint only in process memory.
- Do not acknowledge an event before its destination operation succeeds.
- Keep Elasticsearch and RabbitMQ progress independent.
- Do not replace per-record failure handling with whole-batch rollback.
- Any retry must be bounded/backed off and observable.
- Preserve source IDs, event IDs, source versions, and error context in logs and DLQ records.

## Verification

Run, in order:

```bash
pnpm install
pnpm build
docker compose up --build -d
make seed
make verify
```

Do not mark a gate PASS based only on a unit test or README claim. The verifier must exercise the running Compose system and print evidence.

## Scope

Do not add production-only infrastructure or cosmetic UI work before G1–G5 are testable. If implementation diverges from `SPEC.md`, update the specification history and document the decision in the README.
