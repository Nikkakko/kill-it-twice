import { CommonModule } from "@angular/common";
import { Component, Input } from "@angular/core";
import { PipelineStatus } from "../models";

@Component({
  selector: "app-status-stats",
  standalone: true,
  imports: [CommonModule],
  template: `
    <section class="stats" [class.is-loading]="loading">
      <ng-container *ngIf="status; else skeletons">
        <article><label>Backfill source</label><strong>{{ status.sourceCount | number }}</strong><small>{{ status.outboxCount | number }} ordered events</small></article>
        <article><label>Search checkpoint</label><strong>{{ status.checkpoints.search | number }}</strong><small>{{ status.pending.search | number }} pending</small></article>
        <article><label>Event checkpoint</label><strong>{{ status.checkpoints.events | number }}</strong><small>{{ status.pending.events | number }} pending</small></article>
        <article><label>Incremental lag</label><strong>{{ status.incrementalLag | number }}</strong><small>{{ status.throughputPerSecond | number }} events/s estimate</small></article>
        <article class="danger"><label>DLQ</label><strong>{{ status.dlqCount | number }}</strong><small>unreplayed records</small></article>
      </ng-container>
      <ng-template #skeletons><article *ngFor="let item of skeletonRows"><span class="skeleton skeleton-label"></span><span class="skeleton skeleton-value"></span><span class="skeleton skeleton-small"></span></article></ng-template>
    </section>`,
})
export class StatusStatsComponent {
  @Input() status: PipelineStatus | null = null;
  @Input() loading = true;
  readonly skeletonRows = [1, 2, 3, 4, 5];
}
