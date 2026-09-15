import { CommonModule } from "@angular/common";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { DeadLetter } from "../models";

@Component({
  selector: "app-dlq-panel",
  standalone: true,
  imports: [CommonModule],
  template: `
    <section class="panel">
      <div class="panel-title"><div><h2>Dead-letter queue</h2><p class="muted">Failed records remain available for replay.</p></div><button class="secondary" (click)="refresh.emit()" [disabled]="loading"><span *ngIf="loading" class="spinner dark"></span>Refresh</button></div>
      <div class="table-wrap" [class.is-loading]="loading"><table><thead><tr><th>Sink</th><th>Sequence</th><th>Record</th><th>Error</th><th>Created</th></tr></thead><tbody>
        <ng-container *ngIf="items.length; else dlqState"><tr *ngFor="let item of items"><td>{{ item.sink }}</td><td>{{ item.sequence }}</td><td class="mono">{{ item.recordId | slice:0:8 }}</td><td>{{ item.errorCode }}</td><td>{{ item.createdAt | date:'short' }}</td></tr></ng-container>
        <ng-template #dlqState><ng-container *ngIf="loading; else noDlq"><tr class="skeleton-row" *ngFor="let item of skeletonRows"><td><span class="skeleton"></span></td><td><span class="skeleton skeleton-number"></span></td><td><span class="skeleton"></span></td><td><span class="skeleton"></span></td><td><span class="skeleton"></span></td></tr></ng-container><ng-template #noDlq><tr><td colspan="5" class="muted">{{ error || 'No unreplayed failures.' }}</td></tr></ng-template></ng-template>
      </tbody></table><div *ngIf="loading && items.length" class="loading-overlay"><span class="spinner dark"></span> Loading DLQ…</div></div>
    </section>`,
})
export class DlqPanelComponent {
  @Input() items: DeadLetter[] = [];
  @Input() loading = true;
  @Input() error = "";
  @Output() refresh = new EventEmitter<void>();
  readonly skeletonRows = [1, 2, 3, 4, 5];
}
