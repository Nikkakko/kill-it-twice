export type SinkName = "search" | "events";
export type SinkState = "up" | "down";
export type PipelineAction = "start" | "pause" | "resume";
export type ActionKey = "control" | "seed" | "replay" | "partial" | "config" | `sink:${SinkName}:${SinkState}`;

export interface PipelineStatus {
  sourceCount: number;
  outboxCount: number;
  checkpoints: Record<SinkName, number>;
  pending: Record<SinkName, number>;
  dlqCount: number;
  consumedEventCount?: number;
  throughputPerSecond: number;
  incrementalLag: number;
  paused: boolean;
  health: Record<string, string>;
}

export interface ReplicatedRecord {
  id: string;
  name: string;
  email: string;
  segment: string;
  valid: boolean;
  source_version: number;
  replicated_sequence: number;
  replicated_at: string;
}

export interface DeadLetter {
  id: number;
  sink: SinkName;
  sequence: number;
  recordId: string;
  errorCode: string;
  errorMessage: string;
  retryCount: number;
  createdAt: string;
}

export interface Notice {
  kind: "success" | "error" | "info";
  text: string;
}
