import { Body, Controller, Get, Param, Post, Query } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { Database } from "@kill-it-twice/database";

@Controller()
export class ApiController {
  constructor(private readonly db: Database) {}

  @Get("health")
  async health() {
    await this.db.query("SELECT 1");
    return { status: "ok", service: "api", time: new Date().toISOString() };
  }

  @Get("status")
  async status() {
    const sourceCount = await this.db.prisma.sourceRecord.count({ where: { deleted: false } });
    const outbox = await this.db.query<{ count: string }>(
      "SELECT count(*)::text count FROM outbox_events",
    );
    const cps = await this.db.query<{ sink: string; last_sequence: string }>(
      "SELECT sink, last_sequence::text FROM replication_checkpoints",
    );
    const dlq = await this.db.query<{ count: string }>(
      "SELECT count(*)::text count FROM dead_letters WHERE replayed_at IS NULL",
    );
    const control = await this.db.query<{ value: boolean }>(
      "SELECT value::text::boolean value FROM pipeline_control WHERE key='paused'",
    );
    const runtime = await this.db.query<{
      processed_total: string;
      last_heartbeat: string;
    }>(
      "SELECT processed_total::text, last_heartbeat FROM worker_runtime WHERE id=TRUE",
    );
    const max = await this.db.query<{ max: string }>(
      "SELECT coalesce(max(sequence), 0)::text max FROM outbox_events",
    );
    const checkpoints: Record<string, number> = { search: 0, events: 0 };
    for (const row of cps.rows)
      checkpoints[row.sink] = Number(row.last_sequence);
    const maxSequence = Number(max.rows[0]?.max ?? 0);
    const runtimeRow = runtime.rows[0];
    return {
      sourceCount,
      outboxCount: Number(outbox.rows[0].count),
      checkpoints,
      pending: {
        search: Math.max(0, maxSequence - checkpoints.search),
        events: Math.max(0, maxSequence - checkpoints.events),
      },
      dlqCount: Number(dlq.rows[0].count),
      consumedEventCount: await this.db.prisma.consumedEvent.count(),
      consumerCount: await this.consumerCount(),
      throughputPerSecond: runtimeRow ? Number(runtimeRow.processed_total) : 0,
      incrementalLag: Math.max(
        0,
        maxSequence - Math.min(checkpoints.search, checkpoints.events),
      ),
      paused: Boolean(control.rows[0]?.value),
      health: {
        postgres: "healthy",
        worker:
          runtimeRow &&
          Date.now() - new Date(runtimeRow.last_heartbeat).getTime() < 15000
            ? "healthy"
            : "stale",
        rabbitmq: "managed-by-worker",
        elasticsearch: "managed-by-worker",
      },
    };
  }

  @Get("records")
  async records(@Query("q") q = "", @Query("limit") limit = "50") {
    const safeLimit = Math.min(200, Math.max(1, Number(limit) || 50));
    const result = await this.db.prisma.sourceRecord.findMany({
      where: {
        deleted: false,
        ...(q ? { OR: [{ name: { contains: q, mode: "insensitive" } }, { email: { contains: q, mode: "insensitive" } }] } : {}),
      },
      orderBy: { updatedAt: "desc" },
      take: safeLimit,
      select: { id: true, name: true, email: true, segment: true, valid: true, version: true, updatedAt: true },
    });
    return result.map(row => ({ ...row, version: Number(row.version) }));
  }

  @Get("dlq")
  async dlq() {
    const result = await this.db.prisma.deadLetter.findMany({
      where: { replayedAt: null },
      orderBy: { id: "desc" },
      take: 200,
      select: { id: true, sink: true, sequence: true, recordId: true, errorCode: true, errorMessage: true, retryCount: true, createdAt: true },
    });
    return result.map(row => ({ ...row, id: Number(row.id), sequence: Number(row.sequence) }));
  }

  @Post("pipeline/:action")
  async pipeline(@Param("action") action: string) {
    if (!["start", "pause", "resume"].includes(action))
      return { error: "action must be start, pause, or resume" };
    await this.db.query(
      "INSERT INTO pipeline_control(key,value) VALUES ($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
      ["paused", action === "pause"],
    );
    return { action, paused: action === "pause" };
  }

  @Post("simulation/sink/:sink/:state")
  async sink(@Param("sink") sink: string, @Param("state") state: string) {
    if (!["search", "events"].includes(sink) || !["up", "down"].includes(state))
      return { error: "invalid sink or state" };
    await this.db.query(
      "INSERT INTO pipeline_control(key,value) VALUES ($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
      [`${sink}_outage`, state === "down"],
    );
    return { sink, state };
  }

  @Post("simulation/seed")
  async seed(@Body() body: { count?: number; reset?: boolean }) {
    const count = Math.min(100000, Math.max(1, Number(body?.count ?? 20000)));
    if (body?.reset)
      await this.db.transaction(async client => {
        await client.query(
          "TRUNCATE dead_letters, consumed_events, outbox_events, source_records, replication_checkpoints, worker_runtime RESTART IDENTITY CASCADE",
        );
        await client.query("ALTER SEQUENCE outbox_sequence RESTART WITH 1");
        await client.query(
          "INSERT INTO replication_checkpoints(sink) VALUES ('search'), ('events') ON CONFLICT DO NOTHING",
        );
        await client.query(
          "INSERT INTO worker_runtime(id) VALUES (TRUE) ON CONFLICT DO NOTHING",
        );
      });
    await this.db.transaction(async client => {
      for (let start = 0; start < count; start += 500) {
        const end = Math.min(count, start + 500);
        for (let i = start; i < end; i++) {
          const id = randomUUID();
          const payload = {
            id,
            name: `Customer ${i + 1}`,
            email: `customer-${i + 1}@example.com`,
            segment: i % 3 === 0 ? "enterprise" : "self-serve",
            valid: true,
          };
          await client.query(
            "INSERT INTO source_records(id,name,email,segment,valid) VALUES ($1,$2,$3,$4,TRUE)",
            [id, payload.name, payload.email, payload.segment],
          );
          await client.query(
            `INSERT INTO outbox_events(event_id,record_id,version,operation,payload,mode,target) VALUES ($1,$2,1,'upsert',$3,'backfill','both')`,
            [randomUUID(), id, payload],
          );
        }
      }
    });
    return { seeded: count, mode: "backfill" };
  }

  @Post("simulation/change")
  async change(@Body() body: { id?: string; invalid?: boolean; target?: "search" | "events" | "both" }) {
    const existing = body?.id
      ? await this.db.query<{
          id: string;
          version: string;
          name: string;
          email: string;
          segment: string;
        }>(
          "SELECT id, version::text, name, email, segment FROM source_records WHERE id=$1",
          [body.id],
        )
      : { rows: [] };
    const row = existing.rows[0];
    const id = row?.id ?? randomUUID();
    const version = Number(row?.version ?? 0) + 1;
    const payload = {
      id,
      name: row?.name ?? `Generated ${id.slice(0, 8)}`,
      email: row?.email ?? `${id.slice(0, 8)}@example.com`,
      segment: row?.segment ?? "generated",
      valid: !body?.invalid,
    };
    await this.db.transaction(async client => {
      await client.query(
        `INSERT INTO source_records(id,name,email,segment,valid,version) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,email=EXCLUDED.email,segment=EXCLUDED.segment,valid=EXCLUDED.valid,version=EXCLUDED.version,updated_at=now()`,
        [
          id,
          payload.name,
          payload.email,
          payload.segment,
          payload.valid,
          version,
        ],
      );
      await client.query(
        `INSERT INTO outbox_events(event_id,record_id,version,operation,payload,mode,target) VALUES ($1,$2,$3,'upsert',$4,'incremental',$5)`,
        [randomUUID(), id, version, payload, body?.target ?? "both"],
      );
    });
    return { id, version, valid: payload.valid };
  }

  @Post("simulation/partial")
  async partial(@Body() body: { count?: number }) {
    const count = Math.min(500, Math.max(1, Number(body?.count ?? 500)));
    for (let i = 0; i < count; i++) await this.change({ invalid: i < 3, target: "search" });
    return { created: count, invalid: Math.min(3, count), target: "search" };
  }

  @Post("dlq/replay")
  async replay() {
    const result = await this.db.query<{ id: number }>(
      "UPDATE dead_letters SET replayed_at=now() WHERE replayed_at IS NULL RETURNING id",
    );
    return { replayed: result.rowCount ?? 0 };
  }

  private async consumerCount(): Promise<number> {
    const result = await this.db.query<{ count: string }>(
      `SELECT count(*)::text count FROM consumed_events WHERE consumed_at > now() - interval '5 minutes'`,
    );
    return Number(result.rows[0]?.count ?? 0) > 0 ? 1 : 0;
  }
}
