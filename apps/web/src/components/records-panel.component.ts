import { CommonModule } from "@angular/common";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { ReplicatedRecord } from "../models";

@Component({
  selector: "app-records-panel",
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="panel">
      <div class="panel-title"><div><h2>Replicated records</h2><p class="muted">Live view from Elasticsearch · refreshes every 5s</p></div><div class="search"><input [ngModel]="query" (ngModelChange)="queryChange.emit($event)" (keyup.enter)="search.emit()" placeholder="Search name or email"><button class="secondary" (click)="search.emit()" [disabled]="loading">Search</button></div></div>
      <div class="table-wrap" [class.is-loading]="loading">
        <table><thead><tr><th>Name</th><th>Email</th><th>Segment</th><th>Version</th><th>Replicated</th></tr></thead><tbody>
          <ng-container *ngIf="records.length; else recordsState"><tr *ngFor="let record of records"><td>{{ record.name }}</td><td>{{ record.email }}</td><td><span class="tag">{{ record.segment }}</span></td><td>{{ record.source_version }}</td><td>{{ record.replicated_at | date:'short' }}</td></tr></ng-container>
          <ng-template #recordsState><ng-container *ngIf="loading; else noRecords"><tr class="skeleton-row" *ngFor="let item of skeletonRows"><td><span class="skeleton"></span></td><td><span class="skeleton"></span></td><td><span class="skeleton skeleton-tag"></span></td><td><span class="skeleton skeleton-number"></span></td><td><span class="skeleton"></span></td></tr></ng-container><ng-template #noRecords><tr><td colspan="5" class="muted">{{ error || 'No replicated records found.' }}</td></tr></ng-template></ng-template>
        </tbody></table>
        <div *ngIf="loading && records.length" class="loading-overlay"><span class="spinner dark"></span> Refreshing replicated state…</div>
      </div>
    </div>`,
})
export class RecordsPanelComponent {
  @Input() records: ReplicatedRecord[] = [];
  @Input() query = "";
  @Input() loading = true;
  @Input() error = "";
  @Output() queryChange = new EventEmitter<string>();
  @Output() search = new EventEmitter<void>();
  readonly skeletonRows = [1, 2, 3, 4, 5];
}
