import { Module, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Database } from '@kill-it-twice/database';
import { ReplicationService } from './replication';

@Module({ providers: [Database, ReplicationService] })
export class WorkerModule implements OnModuleInit, OnModuleDestroy {
  constructor(private readonly db: Database, private readonly replication: ReplicationService) {}
  async onModuleInit() { await this.db.init(); await this.replication.start(); }
  async onModuleDestroy() { await this.replication.stop(); await this.db.close(); }
}
