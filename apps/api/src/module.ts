import { Module, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { Database } from "@kill-it-twice/database";
import { ApiController } from "./routes";

@Module({ controllers: [ApiController], providers: [Database] })
export class AppModule implements OnModuleInit, OnModuleDestroy {
  constructor(private readonly db: Database) {}
  async onModuleInit() {
    await this.db.init();
  }
  async onModuleDestroy() {
    await this.db.close();
  }
}
