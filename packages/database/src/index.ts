import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import { PrismaClient } from '@prisma/client';

export const poolConfig = () => ({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  database: process.env.POSTGRES_DB ?? 'replication',
  user: process.env.POSTGRES_USER ?? 'replication',
  password: process.env.POSTGRES_PASSWORD ?? 'replication',
  max: 10,
});

export const schemaSql = `
CREATE TABLE IF NOT EXISTS source_records (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  segment TEXT NOT NULL,
  valid BOOLEAN NOT NULL DEFAULT TRUE,
  version BIGINT NOT NULL DEFAULT 1,
  deleted BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE SEQUENCE IF NOT EXISTS outbox_sequence;
CREATE TABLE IF NOT EXISTS outbox_events (
  sequence BIGINT PRIMARY KEY DEFAULT nextval('outbox_sequence'),
  event_id UUID NOT NULL UNIQUE,
  record_id UUID NOT NULL,
  version BIGINT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('upsert', 'delete')),
  payload JSONB,
  mode TEXT NOT NULL CHECK (mode IN ('backfill', 'incremental')),
  target TEXT NOT NULL DEFAULT 'both' CHECK (target IN ('search', 'events', 'both')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS replication_checkpoints (
  sink TEXT PRIMARY KEY,
  last_sequence BIGINT NOT NULL DEFAULT 0,
  processed_total BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO replication_checkpoints(sink) VALUES ('search'), ('events') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS dead_letters (
  id BIGSERIAL PRIMARY KEY,
  sink TEXT NOT NULL,
  sequence BIGINT NOT NULL,
  event_id UUID NOT NULL,
  record_id UUID NOT NULL,
  payload JSONB,
  error_code TEXT NOT NULL,
  error_message TEXT NOT NULL,
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at TIMESTAMPTZ,
  UNIQUE(sink, sequence)
);
CREATE TABLE IF NOT EXISTS pipeline_control (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL
);
INSERT INTO pipeline_control(key, value) VALUES
  ('paused', 'false'), ('search_outage', 'false'), ('events_outage', 'false'), ('batch_size', '100')
ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS worker_runtime (
  id BOOLEAN PRIMARY KEY DEFAULT TRUE,
  processed_total BIGINT NOT NULL DEFAULT 0,
  last_sequence BIGINT NOT NULL DEFAULT 0,
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO worker_runtime(id) VALUES (TRUE) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS consumed_events (
  event_id UUID PRIMARY KEY,
  sequence BIGINT NOT NULL,
  consumed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`;

export class Database {
  readonly pool = new Pool(poolConfig());
  readonly prisma = new PrismaClient({
    datasources: { db: { url: process.env.DATABASE_URL ?? this.databaseUrl() } },
  });

  async init(): Promise<void> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= 30; attempt++) {
      try {
        await this.prisma.$connect();
        await this.pool.query(schemaSql);
        return;
      } catch (error) {
        lastError = error;
        await new Promise(resolve => setTimeout(resolve, Math.min(1000, attempt * 100)));
      }
    }
    throw lastError;
  }

  async query<T extends QueryResultRow = any>(text: string, values: unknown[] = []): Promise<QueryResult<T>> {
    return this.pool.query<T>(text, values);
  }

  async transaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> { await this.prisma.$disconnect(); await this.pool.end(); }

  private databaseUrl(): string {
    const user = encodeURIComponent(process.env.POSTGRES_USER ?? 'replication');
    const password = encodeURIComponent(process.env.POSTGRES_PASSWORD ?? 'replication');
    const host = process.env.POSTGRES_HOST ?? 'localhost';
    const port = process.env.POSTGRES_PORT ?? '5432';
    const database = process.env.POSTGRES_DB ?? 'replication';
    return `postgresql://${user}:${password}@${host}:${port}/${database}`;
  }
}
