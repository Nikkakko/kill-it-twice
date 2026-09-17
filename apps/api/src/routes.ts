import { Body, Controller, Get, Param, Post, Query } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { Database } from "@kill-it-twice/database";
import { Client as ElasticsearchClient } from "@elastic/elasticsearch";

@Controller()
export class ApiController {
  private readonly elastic = new ElasticsearchClient({ node: process.env.ELASTICSEARCH_URL ?? "http://localhost:9200" });

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
        rabbitmq: await this.rabbitmqHealth(),
        elasticsearch: await this.elasticsearchHealth(),
      },
    };
  }

  @Get("replicated")
  async replicated(@Query("q") q = "", @Query("limit") limit = "50") {
    const size = Math.min(200, Math.max(1, Number(limit) || 50));
    try {
      const result = await this.elastic.search({
        index: "replicated-records",
        size,
        query: q ? { multi_match: { query: q, fields: ["name", "email", "segment"] } } : { match_all: {} },
        sort: [{ replicated_sequence: "desc" }],
      });
      return result.hits.hits.map(hit => {
        const source = hit._source as Record<string, unknown> | undefined;
        return source
          ? {
              ...source,
              source_version: Number(source.source_version),
              replicated_sequence: Number(source.replicated_sequence),
            }
          : source;
      });
    } catch (error: any) {
      if (error?.meta?.statusCode === 404) return [];
      throw error;
    }
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
    const replayed = await this.db.transaction(async client => {
      const failures = await client.query<{
        id: number;
        sink: "search" | "events";
        sequence: string;
        payload: Record<string, unknown> | null;
        record_id: string;
      }>("SELECT id, sink, sequence::text, payload, record_id FROM dead_letters WHERE replayed_at IS NULL ORDER BY id");
      let count = 0;
      for (const failure of failures.rows) {
        const original = await client.query<{ version: string; operation: string }>(
          "SELECT version::text, operation FROM outbox_events WHERE sequence=$1",
          [failure.sequence],
        );
        const event = original.rows[0];
        if (!event) continue;
        const current = await client.query<{
          version: string;
          payload: Record<string, unknown> | null;
          deleted: boolean;
        }>(
          `SELECT version::text,
                  jsonb_build_object('id', id, 'name', name, 'email', email, 'segment', segment, 'valid', valid) payload,
                  deleted
           FROM source_records WHERE id=$1`,
          [failure.record_id],
        );
        const source = current.rows[0];
        const operation = source?.deleted ? "delete" : event.operation;
        const payload = source?.deleted ? null : source?.payload ?? failure.payload;
        const version = Number(source?.version ?? event.version);
        await client.query(
          "INSERT INTO outbox_events(event_id,record_id,version,operation,payload,mode,target) VALUES ($1,$2,$3,$4,$5,'incremental',$6)",
          [randomUUID(), failure.record_id, version, operation, payload, failure.sink],
        );
        await client.query("UPDATE dead_letters SET replayed_at=now() WHERE id=$1", [failure.id]);
        count++;
      }
      return count;
    });
    return { replayed };
  }

  @Get("config")
  async config() {
    const result = await this.db.query<{ value: number }>("SELECT value::text::integer value FROM pipeline_control WHERE key='batch_size'");
    return { batchSize: Number(result.rows[0]?.value ?? 100) };
  }

  @Post("config")
  async updateConfig(@Body() body: { batchSize?: number }) {
    const batchSize = Math.min(1000, Math.max(1, Number(body?.batchSize ?? 100)));
    await this.db.query(
      "INSERT INTO pipeline_control(key,value) VALUES ('batch_size',$1) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
      [batchSize],
    );
    return { batchSize };
  }

  private async consumerCount(): Promise<number> {
    const result = await this.db.query<{ count: string }>(
      `SELECT count(*)::text count FROM consumed_events WHERE consumed_at > now() - interval '5 minutes'`,
    );
    return Number(result.rows[0]?.count ?? 0) > 0 ? 1 : 0;
  }

  private async elasticsearchHealth(): Promise<string> {
    try {
      await this.elastic.cluster.health();
      return "healthy";
    } catch {
      return "unhealthy";
    }
  }

  private async rabbitmqHealth(): Promise<string> {
    try {
      const response = await fetch(process.env.RABBITMQ_MANAGEMENT_URL ?? "http://localhost:15672/api/overview", {
        headers: {
          authorization: `Basic ${Buffer.from(`${process.env.RABBITMQ_USER ?? "replication"}:${process.env.RABBITMQ_PASSWORD ?? "replication"}`).toString("base64")}`,
        },
        signal: AbortSignal.timeout(1500),
      });
      return response.ok ? "healthy" : "unhealthy";
    } catch {
      return "unhealthy";
    }
  }
}
