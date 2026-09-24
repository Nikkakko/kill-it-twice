import { Injectable } from "@nestjs/common";
import { Client as ElasticsearchClient } from "@elastic/elasticsearch";
import { randomUUID } from "node:crypto";
import { Database } from "@kill-it-twice/database";
import { SinkName } from "@kill-it-twice/contracts";
import { ChangeDto, ConfigDto, PartialDto, SeedDto } from "./api.dto";
import { PipelineAction, SinkState } from "./api.types";

@Injectable()
export class ApiService {
  private readonly elastic = new ElasticsearchClient({
    node: process.env.ELASTICSEARCH_URL ?? "http://localhost:9200",
  });

  constructor(private readonly db: Database) {}

  async health() {
    await this.db.prisma.$queryRaw`SELECT 1`;
    return { status: "ok", service: "api", time: new Date().toISOString() };
  }

  async status() {
    const [
      sourceCount,
      outboxCount,
      checkpoints,
      dlqCount,
      paused,
      runtime,
      maxSequence,
      consumedEventCount,
    ] = await Promise.all([
      this.db.prisma.sourceRecord.count({ where: { deleted: false } }),
      this.db.prisma.outboxEvent.count(),
      this.db.prisma.replicationCheckpoint.findMany({
        select: { sink: true, lastSequence: true },
      }),
      this.db.prisma.deadLetter.count({ where: { replayedAt: null } }),
      this.db.prisma.pipelineControl.findUnique({
        where: { key: "paused" },
        select: { value: true },
      }),
      this.db.prisma.workerRuntime.findUnique({
        where: { id: true },
        select: { processedTotal: true, lastHeartbeat: true },
      }),
      this.db.prisma.outboxEvent.aggregate({ _max: { sequence: true } }),
      this.db.prisma.consumedEvent.count(),
    ]);
    const checkpointValues: Record<SinkName, number> = { search: 0, events: 0 };
    for (const checkpoint of checkpoints)
      checkpointValues[checkpoint.sink as SinkName] = Number(
        checkpoint.lastSequence,
      );
    const max = Number(maxSequence._max.sequence ?? 0);
    const runtimeIsHealthy = runtime
      ? Date.now() - runtime.lastHeartbeat.getTime() < 15000
      : false;
    return {
      sourceCount,
      outboxCount,
      checkpoints: checkpointValues,
      pending: {
        search: Math.max(0, max - checkpointValues.search),
        events: Math.max(0, max - checkpointValues.events),
      },
      dlqCount,
      consumedEventCount,
      consumerCount: await this.consumerCount(),
      throughputPerSecond: runtime ? Number(runtime.processedTotal) : 0,
      incrementalLag: Math.max(
        0,
        max - Math.min(checkpointValues.search, checkpointValues.events),
      ),
      paused: this.isTruthyControl(paused?.value),
      health: {
        postgres: "healthy",
        worker: runtimeIsHealthy ? "healthy" : "stale",
        rabbitmq: await this.rabbitmqHealth(),
        elasticsearch: await this.elasticsearchHealth(),
      },
    };
  }

  async replicated(query = "", limit = "50") {
    const size = Math.min(200, Math.max(1, Number(limit) || 50));
    try {
      const result = await this.elastic.search({
        index: "replicated-records",
        size,
        query: {
          bool: {
            must: query
              ? [{ multi_match: { query, fields: ["name", "email", "segment"] } }]
              : [{ match_all: {} }],
            must_not: [{ term: { deleted: true } }],
          },
        },
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
    } catch (error: unknown) {
      if (this.elasticsearchStatus(error) === 404) return [];
      throw error;
    }
  }

  async records(query = "", limit = "50") {
    const take = Math.min(200, Math.max(1, Number(limit) || 50));
    const result = await this.db.prisma.sourceRecord.findMany({
      where: {
        deleted: false,
        ...(query
          ? {
              OR: [
                { name: { contains: query, mode: "insensitive" } },
                { email: { contains: query, mode: "insensitive" } },
              ],
            }
          : {}),
      },
      orderBy: { updatedAt: "desc" },
      take,
      select: {
        id: true,
        name: true,
        email: true,
        segment: true,
        valid: true,
        version: true,
        updatedAt: true,
      },
    });
    return result.map(row => ({ ...row, version: Number(row.version) }));
  }

  async dlq() {
    const result = await this.db.prisma.deadLetter.findMany({
      where: { replayedAt: null },
      orderBy: { id: "desc" },
      take: 200,
      select: {
        id: true,
        sink: true,
        sequence: true,
        recordId: true,
        errorCode: true,
        errorMessage: true,
        retryCount: true,
        createdAt: true,
      },
    });
    return result.map(row => ({
      ...row,
      id: Number(row.id),
      sequence: Number(row.sequence),
    }));
  }

  async pipeline(action: PipelineAction) {
    if (!["start", "pause", "resume"].includes(action))
      return { error: "action must be start, pause, or resume" };
    await this.db.prisma.pipelineControl.upsert({
      where: { key: "paused" },
      create: { key: "paused", value: action === "pause" },
      update: { value: action === "pause" },
    });
    return { action, paused: action === "pause" };
  }

  async sink(sink: SinkName, state: SinkState) {
    if (!["search", "events"].includes(sink) || !["up", "down"].includes(state))
      return { error: "invalid sink or state" };
    await this.db.prisma.pipelineControl.upsert({
      where: { key: `${sink}_outage` },
      create: { key: `${sink}_outage`, value: state === "down" },
      update: { value: state === "down" },
    });
    return { sink, state };
  }

  async seed(body: SeedDto) {
    const count = Math.min(100000, Math.max(1, Number(body?.count ?? 20000)));
    await this.db.prisma.$transaction(async tx => {
      if (body?.reset) {
        await tx.$executeRaw`TRUNCATE dead_letters, consumed_events, outbox_events, source_records, replication_checkpoints, worker_runtime RESTART IDENTITY CASCADE`;
        await tx.$executeRaw`ALTER SEQUENCE outbox_sequence RESTART WITH 1`;
        await tx.replicationCheckpoint.createMany({
          data: [{ sink: "search" }, { sink: "events" }],
        });
        await tx.workerRuntime.upsert({
          where: { id: true },
          create: { id: true },
          update: {},
        });
      }
      for (let start = 0; start < count; start += 500) {
        const sourceRecords = [];
        const outboxEvents = [];
        for (let i = start; i < Math.min(count, start + 500); i++) {
          const id = randomUUID();
          const payload = {
            id,
            name: `Customer ${i + 1}`,
            email: `customer-${i + 1}@example.com`,
            segment: i % 3 === 0 ? "enterprise" : "self-serve",
            valid: true,
          };
          sourceRecords.push({
            id,
            name: payload.name,
            email: payload.email,
            segment: payload.segment,
            valid: true,
          });
          outboxEvents.push({
            eventId: randomUUID(),
            recordId: id,
            version: BigInt(1),
            operation: "upsert",
            payload,
            mode: "backfill",
            target: "both",
          });
        }
        await tx.sourceRecord.createMany({ data: sourceRecords });
        await tx.outboxEvent.createMany({ data: outboxEvents });
      }
    });
    return { seeded: count, mode: "backfill" };
  }

  async change(body: ChangeDto) {
    return this.db.prisma.$transaction(async tx => {
      const existing = body?.id
        ? await tx.sourceRecord.findUnique({ where: { id: body.id } })
        : null;
      const id = existing?.id ?? body?.id ?? randomUUID();
      const version = Number(existing?.version ?? 0) + 1;
      const payload = {
        id,
        name: existing?.name ?? `Generated ${id.slice(0, 8)}`,
        email: existing?.email ?? `${id.slice(0, 8)}@example.com`,
        segment: existing?.segment ?? "generated",
        valid: !body?.invalid,
      };
      await tx.sourceRecord.upsert({
        where: { id },
        create: { ...payload, version: BigInt(version) },
        update: {
          name: payload.name,
          email: payload.email,
          segment: payload.segment,
          valid: payload.valid,
          version: BigInt(version),
          updatedAt: new Date(),
        },
      });
      await tx.outboxEvent.create({
        data: {
          eventId: randomUUID(),
          recordId: id,
          version: BigInt(version),
          operation: "upsert",
          payload,
          mode: "incremental",
          target: body?.target ?? "both",
        },
      });
      return { id, version, valid: payload.valid };
    });
  }

  async partial(body: PartialDto) {
    const count = Math.min(500, Math.max(1, Number(body?.count ?? 500)));
    for (let i = 0; i < count; i++)
      await this.change({ invalid: i < 3, target: "search" });
    return { created: count, invalid: Math.min(3, count), target: "search" };
  }

  async replay() {
    const replayed = await this.db.prisma.$transaction(async tx => {
      const failures = await tx.deadLetter.findMany({
        where: { replayedAt: null },
        orderBy: { id: "asc" },
        select: {
          id: true,
          sink: true,
          sequence: true,
          payload: true,
          recordId: true,
        },
      });
      let count = 0;
      for (const failure of failures) {
        const original = await tx.outboxEvent.findUnique({
          where: { sequence: failure.sequence },
          select: { version: true, operation: true },
        });
        if (!original) continue;
        const source = await tx.sourceRecord.findUnique({
          where: { id: failure.recordId },
        });
        const operation = source?.deleted ? "delete" : original.operation;
        const payload = source?.deleted
          ? null
          : source
            ? {
                id: source.id,
                name: source.name,
                email: source.email,
                segment: source.segment,
                valid: source.valid,
              }
            : failure.payload;
        await tx.outboxEvent.create({
          data: {
            eventId: randomUUID(),
            recordId: failure.recordId,
            version: source?.version ?? original.version,
            operation,
            payload: payload ?? undefined,
            mode: "incremental",
            target: failure.sink,
          },
        });
        await tx.deadLetter.update({
          where: { id: failure.id },
          data: { replayedAt: new Date() },
        });
        count++;
      }
      return count;
    });
    return { replayed };
  }

  async config() {
    const control = await this.db.prisma.pipelineControl.findUnique({
      where: { key: "batch_size" },
      select: { value: true },
    });
    return { batchSize: Number(control?.value ?? 100) };
  }

  async updateConfig(batchSizeInput: ConfigDto) {
    const batchSize = Math.min(
      1000,
      Math.max(1, Number(batchSizeInput?.batchSize ?? 100)),
    );
    await this.db.prisma.pipelineControl.upsert({
      where: { key: "batch_size" },
      create: { key: "batch_size", value: batchSize },
      update: { value: batchSize },
    });
    return { batchSize };
  }

  private async consumerCount(): Promise<number> {
    const since = new Date(Date.now() - 5 * 60 * 1000);
    return (await this.db.prisma.consumedEvent.count({
      where: { consumedAt: { gt: since } },
    })) > 0
      ? 1
      : 0;
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
      const response = await fetch(
        process.env.RABBITMQ_MANAGEMENT_URL ??
          "http://localhost:15672/api/overview",
        {
          headers: {
            authorization: `Basic ${Buffer.from(`${process.env.RABBITMQ_USER ?? "replication"}:${process.env.RABBITMQ_PASSWORD ?? "replication"}`).toString("base64")}`,
          },
          signal: AbortSignal.timeout(1500),
        },
      );
      return response.ok ? "healthy" : "unhealthy";
    } catch {
      return "unhealthy";
    }
  }

  private isTruthyControl(value: unknown) {
    return value === true || value === "true" || value === 1 || value === "1";
  }
  private elasticsearchStatus(error: unknown) {
    return (error as { meta?: { statusCode?: number } })?.meta?.statusCode;
  }
}
