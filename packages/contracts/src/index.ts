export type SinkName = 'search' | 'events';
export type Operation = 'upsert' | 'delete';

export interface ReplicationEvent {
  eventId: string;
  sequence: number;
  recordId: string;
  version: number;
  operation: Operation;
  payload: Record<string, unknown> | null;
  mode: 'backfill' | 'incremental';
  target: SinkName | 'both';
  createdAt: string;
}

export interface PipelineStatus {
  sourceCount: number;
  outboxCount: number;
  checkpoints: Record<SinkName, number>;
  pending: Record<SinkName, number>;
  dlqCount: number;
  consumedEventCount?: number;
  consumerCount: number;
  throughputPerSecond: number;
  incrementalLag: number;
  paused: boolean;
  health: Record<string, string>;
}
