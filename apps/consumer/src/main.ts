import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { Module, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import * as amqp from "amqplib";
import { Database } from "@kill-it-twice/database";

@Module({ providers: [Database] })
class ConsumerModule implements OnModuleInit, OnModuleDestroy {
  private connection?: amqp.ChannelModel;
  private channel?: amqp.Channel;
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
    await this.channel.bindQueue(
      "replication.consumer",
      "replication.events",
      "records",
    );
    await this.channel.consume("replication.consumer", async message => {
      if (!message) return;
      try {
        const event = JSON.parse(message.content.toString());
        await this.db.query(
          "INSERT INTO consumed_events(event_id,sequence) VALUES ($1,$2) ON CONFLICT DO NOTHING",
          [event.eventId, event.sequence],
        );
        this.channel?.ack(message);
      } catch {
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
