import { Injectable, Logger } from '@nestjs/common';
import { Client as ElasticsearchClient } from '@elastic/elasticsearch';
import * as amqp from 'amqplib';
import { Database } from '@kill-it-twice/database';
import { ReplicationEvent, SinkName } from '@kill-it-twice/contracts';

type OutboxRow = ReplicationEvent & { target: 'search' | 'events' | 'both' };

@Injectable()
export class ReplicationService {
  private readonly logger = new Logger(ReplicationService.name);
  private readonly elastic = new ElasticsearchClient({ node: process.env.ELASTICSEARCH_URL ?? 'http://localhost:9200' });
  private running = false;
  private rabbit?: amqp.ConfirmChannel;
  private rabbitConnection?: amqp.ChannelModel;
  private loopPromises: Promise<void>[] = [];
  private throughputWindow = { total: 0, started: Date.now() };

  constructor(private readonly db: Database) {}

  async start() {
    await this.db.init();
    await this.connectRabbit();
    await this.ensureIndex();
    this.running = true;
    this.loopPromises = [this.sinkLoop('search'), this.sinkLoop('events'), this.heartbeatLoop()];
    this.logger.log('replication worker started');
  }

  async stop() {
    this.running = false;
    await Promise.allSettled(this.loopPromises);
    await this.rabbitConnection?.close();
  }

  private async connectRabbit() {
    this.rabbitConnection = await amqp.connect(process.env.RABBITMQ_URL ?? 'amqp://replication:replication@localhost:5672');
    this.rabbit = await this.rabbitConnection.createConfirmChannel();
    await this.rabbit.assertExchange('replication.events', 'topic', { durable: true });
    await this.rabbit.assertQueue('replication.consumer', { durable: true });
    await this.rabbit.bindQueue('replication.consumer', 'replication.events', 'records');
  }

  private async ensureIndex() {
    try {
      await this.elastic.indices.create({
        index: 'replicated-records',
        mappings: {
          properties: {
            id: { type: 'keyword' },
            name: { type: 'text' },
            email: { type: 'text' },
            segment: { type: 'keyword' },
            valid: { type: 'boolean' },
            source_version: { type: 'long' },
            replicated_sequence: { type: 'long' },
            replicated_at: { type: 'date' },
          },
        },
      });
    } catch (error: any) {
      if (error?.meta?.statusCode !== 400) this.logger.warn(`Elasticsearch index setup deferred: ${error.message}`);
    }
  }

  private async sinkLoop(sink: SinkName) {
    while (this.running) {
      try {
        const paused = await this.isControl('paused');
        if (paused) { await this.delay(500); continue; }
        const rows = await this.nextBatch(sink, await this.batchSize());
        if (!rows.length) { await this.delay(250); continue; }
        for (const event of rows) {
          try {
            await this.processEvent(sink, event);
            await this.advance(sink, event.sequence);
          } catch (error: any) {
            if (this.isPermanent(error, event)) {
              await this.writeDlq(sink, event, error);
              await this.advance(sink, event.sequence);
              continue;
            }
            this.logger.warn(`${sink} paused after transient failure: ${error.message}`);
            await this.delay(1000);
            break;
          }
        }
      } catch (error: any) {
        this.logger.error(`${sink} loop error: ${error.message}`);
        await this.delay(1500);
      }
    }
  }

  private async nextBatch(sink: SinkName, size: number): Promise<OutboxRow[]> {
    const result = await this.db.query<OutboxRow>(
      `SELECT event_id AS "eventId", sequence, record_id AS "recordId", version, operation, payload, mode, target, created_at AS "createdAt"
       FROM outbox_events e JOIN replication_checkpoints c ON c.sink=$1
       WHERE e.sequence > c.last_sequence AND (e.target=$1 OR e.target='both')
       ORDER BY e.sequence LIMIT $2`, [sink, size]);
    return result.rows;
  }

  private async batchSize(): Promise<number> {
    const result = await this.db.query<{ value: string }>("SELECT value::text::integer value FROM pipeline_control WHERE key='batch_size'");
    return Math.min(1000, Math.max(1, Number(result.rows[0]?.value ?? 100)));
  }

  private async processEvent(sink: SinkName, event: OutboxRow) {
    if (await this.isControl(`${sink}_outage`)) throw new Error(`${sink} outage simulation is active`);
    if (event.payload && (event.payload as any).valid === false) {
      const error: any = new Error('payload failed validation');
      error.code = 'INVALID_PAYLOAD';
      throw error;
    }
    if (sink === 'search') {
      if (event.operation === 'delete') {
        try {
          await this.elastic.delete({ index: 'replicated-records', id: event.recordId });
        } catch (error: any) {
          if (error?.meta?.statusCode !== 404) throw error;
        }
      } else await this.elastic.index({ index: 'replicated-records', id: event.recordId, document: { ...(event.payload ?? {}), source_version: event.version, replicated_sequence: event.sequence, replicated_at: new Date().toISOString() } });
    } else {
      if (!this.rabbit) throw new Error('RabbitMQ channel unavailable');
      const message = Buffer.from(JSON.stringify(event));
      await new Promise<void>((resolve, reject) => this.rabbit!.publish('replication.events', 'records', message, { persistent: true, messageId: event.eventId, contentType: 'application/json' }, (err) => err ? reject(err) : resolve()));
    }
    this.throughputWindow.total++;
  }

  private isPermanent(error: any, event: OutboxRow) { return error?.code === 'INVALID_PAYLOAD' || (event.payload && (event.payload as any).valid === false); }

  private async writeDlq(sink: SinkName, event: OutboxRow, error: any) {
    await this.db.query(`INSERT INTO dead_letters(sink,sequence,event_id,record_id,payload,error_code,error_message) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(sink,sequence) DO NOTHING`, [sink, event.sequence, event.eventId, event.recordId, event.payload, error.code ?? 'PERMANENT_ERROR', error.message]);
    this.logger.warn(`DLQ ${sink} sequence=${event.sequence}: ${error.message}`);
  }

  private async advance(sink: SinkName, sequence: number) {
    await this.db.query(`UPDATE replication_checkpoints SET last_sequence=$2, processed_total=processed_total+1, updated_at=now() WHERE sink=$1 AND last_sequence < $2`, [sink, sequence]);
  }

  private async heartbeatLoop() {
    while (this.running) {
      const rate = this.throughputWindow.total / Math.max(1, (Date.now() - this.throughputWindow.started) / 1000);
      await this.db.query(`UPDATE worker_runtime SET processed_total=$1,last_sequence=GREATEST(last_sequence,$1),last_heartbeat=now() WHERE id=TRUE`, [Math.round(rate)]);
      await this.delay(2000);
    }
  }

  private async isControl(key: string): Promise<boolean> {
    const result = await this.db.query<{ value: boolean }>(`SELECT value::text::boolean value FROM pipeline_control WHERE key=$1`, [key]);
    return Boolean(result.rows[0]?.value);
  }

  private delay(ms: number) { return new Promise<void>((resolve) => setTimeout(resolve, ms)); }
}
