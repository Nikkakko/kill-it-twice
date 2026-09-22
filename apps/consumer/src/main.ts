import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { Logger, Module, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import * as amqp from "amqplib";
import { Database } from "@kill-it-twice/database";

const BASE_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;

@Module({ providers: [Database] })
class ConsumerModule implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ConsumerModule.name);
  private connection?: amqp.ChannelModel;
  private channel?: amqp.Channel;
  private writeFailureStreak = 0;
  constructor(private readonly db: Database) {}
  async onModuleInit() {
    await this.db.init();
    this.connection = await amqp.connect(
      process.env.RABBITMQ_URL ??
        "amqp://replication:replication@localhost:5672",
    );
    this.channel = await this.connection.createChannel();
    await this.channel.assertExchange("replication.events", "topic", {
      durable: true,
    });
    await this.channel.assertQueue("replication.consumer", { durable: true });
    await this.channel.assertQueue("replication.consumer.dlq", {
      durable: true,
    });
    await this.channel.bindQueue(
      "replication.consumer",
      "replication.events",
      "records",
    );
    await this.channel.consume("replication.consumer", async message => {
      if (!message) return;
      let event: { eventId: string; sequence: number };
      try {
        event = JSON.parse(message.content.toString());
      } catch (error: any) {
        // Malformed bytes never become valid JSON on retry; requeueing forever
        // just busy-loops. Park it for inspection and move on.
        this.logger.error(
          `poison message routed to replication.consumer.dlq: ${error.message}`,
        );
        this.channel!.sendToQueue(
          "replication.consumer.dlq",
          message.content,
          {
            persistent: true,
            headers: { ...message.properties.headers, "x-error": error.message },
          },
        );
        this.channel?.ack(message);
        return;
      }
      try {
        await this.db.query(
          "INSERT INTO consumed_events(event_id,sequence) VALUES ($1,$2) ON CONFLICT DO NOTHING",
          [event.eventId, event.sequence],
        );
        this.channel?.ack(message);
        this.writeFailureStreak = 0;
      } catch (error: any) {
        // A DB write failure is transient infrastructure trouble (e.g. Postgres
        // restarting), not a poison message, so keep the event durably queued
        // and retry indefinitely with backoff instead of busy-looping.
        this.writeFailureStreak++;
        const delay = Math.min(
          MAX_BACKOFF_MS,
          BASE_BACKOFF_MS * 2 ** (this.writeFailureStreak - 1),
        );
        this.logger.warn(
          `consumer write failed (attempt ${this.writeFailureStreak}, retrying in ${delay}ms): ${error.message}`,
        );
        await new Promise(resolve => setTimeout(resolve, delay));
        this.channel?.nack(message, false, true);
      }
    });
  }
  async onModuleDestroy() {
    await this.channel?.close();
    await this.connection?.close();
    await this.db.close();
  }
}

async function bootstrap() {
  await NestFactory.createApplicationContext(ConsumerModule);
}
bootstrap();
