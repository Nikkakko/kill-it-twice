import { CommonModule } from "@angular/common";
import { HttpClient } from "@angular/common/http";
import { Component, DestroyRef, OnInit, inject, signal } from "@angular/core";
import { takeUntilDestroyed } from "@angular/core/rxjs-interop";
import { FormsModule } from "@angular/forms";
import {
  EMPTY,
  Observable,
  catchError,
  finalize,
  interval,
  startWith,
  switchMap,
} from "rxjs";
import { DlqPanelComponent } from "./components/dlq-panel.component";
import { PipelineControlsComponent } from "./components/pipeline-controls.component";
import { RecordsPanelComponent } from "./components/records-panel.component";
import { SimulationPanelComponent } from "./components/simulation-panel.component";
import { StatusStatsComponent } from "./components/status-stats.component";
import {
  ActionKey,
  DeadLetter,
  Notice,
  PipelineAction,
  PipelineStatus,
  ReplicatedRecord,
  SinkName,
  SinkState,
} from "./models";

@Component({
  selector: "app-root",
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    DlqPanelComponent,
    PipelineControlsComponent,
    RecordsPanelComponent,
    SimulationPanelComponent,
    StatusStatsComponent,
  ],
  template: ` <main>
    <header>
      <div>
        <span class="eyebrow">OPTIO PLATFORM</span>
        <h1>Kill It Twice</h1>
        <p>Replication operations console</p>
      </div>
      <span
        *ngIf="status(); else connecting"
        class="pill"
        [class.bad]="status()?.health?.worker !== 'healthy'"
        >● {{ status()?.health?.worker }}</span
      >
      <ng-template #connecting
        ><span class="pill pending-pill">● connecting</span></ng-template
      >
    </header>
    <div
      *ngIf="notice()"
      class="toast"
      [class.toast-error]="notice()?.kind === 'error'"
      [class.toast-info]="notice()?.kind === 'info'"
      role="status"
      aria-live="polite"
    >
      <span>{{
        notice()?.kind === "error" ? "!" : notice()?.kind === "info" ? "…" : "✓"
      }}</span
      >{{ notice()?.text }}
    </div>
    <app-status-stats [status]="status()" [loading]="statusLoading()" />
    <app-pipeline-controls
      [status]="status()"
      [batchSize]="batchSize()"
      [busy]="actionBusy()"
      (control)="control($event)"
      (seed)="seed()"
      (replay)="replay()"
      (batchSizeChange)="batchSize.set($event)"
      (configChange)="saveConfig()"
    />
    <section class="grid">
      <app-records-panel
        [records]="records()"
        [query]="query()"
        [loading]="recordsLoading()"
        [error]="recordsError()"
        (queryChange)="query.set($event)"
        (search)="loadRecords()"
      />
      <app-simulation-panel
        [busy]="actionBusy()"
        (sink)="sink($event.name, $event.state)"
        (partial)="partial()"
        (change)="generateChange()"
      />
    </section>
    <app-dlq-panel
      [items]="dlq()"
      [loading]="dlqLoading()"
      [error]="dlqError()"
      (refresh)="loadDlq()"
    />
  </main>`,
  styleUrls: ["./styles.css"],
})
export class AppComponent implements OnInit {
  private readonly http = inject(HttpClient);
  private readonly destroyRef = inject(DestroyRef);

  readonly status = signal<PipelineStatus | null>(null);
  readonly records = signal<ReplicatedRecord[]>([]);
  readonly dlq = signal<DeadLetter[]>([]);
  readonly query = signal("");
  readonly batchSize = signal(100);
  readonly notice = signal<Notice | null>(null);
  readonly actionBusy = signal<ActionKey | null>(null);
  readonly statusLoading = signal(true);
  readonly recordsLoading = signal(true);
  readonly dlqLoading = signal(true);
  readonly recordsError = signal("");
  readonly dlqError = signal("");

  ngOnInit() {
    interval(2000)
      .pipe(
        startWith(0),
        switchMap(() =>
          this.http.get<PipelineStatus>("/api/status").pipe(
            catchError(() => {
              this.statusLoading.set(false);
              this.setNotice("error", "Unable to reach the API. Retrying…");
              return EMPTY;
            }),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe(value => {
        this.status.set(value);
        this.statusLoading.set(false);
      });
    this.http
      .get<{ batchSize: number }>("/api/config")
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: value => this.batchSize.set(value.batchSize),
        error: () => undefined,
      });
    this.loadRecords();
    this.loadDlq();
    interval(5000)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => {
        this.refreshRecords(false);
        this.refreshDlq(false);
      });
  }

  control(action: PipelineAction) {
    this.runAction(
      "control",
      this.http.post(`/api/pipeline/${action}`, {}),
      `Pipeline ${action} requested`,
    );
  }
  seed() {
    this.runAction(
      "seed",
      this.http.post("/api/simulation/seed", { count: 20000, reset: true }),
      "Seeded 20,000 records",
      () => {
        this.loadRecords();
        this.loadDlq();
      },
    );
  }
  replay() {
    this.runAction(
      "replay",
      this.http.post("/api/dlq/replay", {}),
      "DLQ replay queued",
      () => this.loadDlq(),
    );
  }
  sink(name: SinkName, state: SinkState) {
    this.runAction(
      `sink:${name}:${state}`,
      this.http.post(`/api/simulation/sink/${name}/${state}`, {}),
      `${name} marked ${state}`,
    );
  }
  partial() {
    this.runAction(
      "partial",
      this.http.post("/api/simulation/partial", { count: 500 }),
      "Injected 500 events, including 3 invalid",
      () => this.loadDlq(),
    );
  }
  generateChange() {
    this.runAction(
      "change",
      this.http.post("/api/simulation/change", { target: "both" }),
      "Generated a source change",
      () => this.loadRecords(),
    );
  }

  saveConfig() {
    const value = Math.min(1000, Math.max(1, Number(this.batchSize()) || 100));
    this.batchSize.set(value);
    this.runAction(
      "config",
      this.http.post<{ batchSize: number }>("/api/config", {
        batchSize: value,
      }),
      `Batch size set to ${value}`,
      result => this.batchSize.set(result.batchSize),
    );
  }

  loadRecords() {
    this.refreshRecords(true);
  }
  loadDlq() {
    this.refreshDlq(true);
  }

  private refreshRecords(showLoader: boolean) {
    if (showLoader) this.recordsLoading.set(true);
    this.recordsError.set("");
    this.http
      .get<ReplicatedRecord[]>(
        `/api/replicated?q=${encodeURIComponent(this.query())}`,
      )
      .pipe(
        finalize(() => this.recordsLoading.set(false)),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe({
        next: value => this.records.set(value),
        error: () => {
          this.recordsError.set("Could not load replicated records");
          this.setNotice("error", "Could not load replicated records");
        },
      });
  }

  private refreshDlq(showLoader: boolean) {
    if (showLoader) this.dlqLoading.set(true);
    this.dlqError.set("");
    this.http
      .get<DeadLetter[]>("/api/dlq")
      .pipe(
        finalize(() => this.dlqLoading.set(false)),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe({
        next: value => this.dlq.set(value),
        error: () => {
          this.dlqError.set("Could not load DLQ");
          this.setNotice("error", "Could not load the dead-letter queue");
        },
      });
  }

  private runAction<T>(
    key: ActionKey,
    request: Observable<T>,
    success: string,
    after?: (value: T) => void,
  ) {
    if (this.actionBusy()) return;
    this.actionBusy.set(key);
    this.setNotice("info", "Working…");
    request
      .pipe(
        finalize(() => this.actionBusy.set(null)),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe({
        next: value => {
          after?.(value);
          this.setNotice("success", success);
          this.refreshStatus();
        },
        error: () =>
          this.setNotice(
            "error",
            "Action failed. Check the service status and try again.",
          ),
      });
  }

  private refreshStatus() {
    this.http
      .get<PipelineStatus>("/api/status")
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: value => this.status.set(value),
        error: () => undefined,
      });
  }
  private setNotice(kind: Notice["kind"], text: string) {
    this.notice.set({ kind, text });
    if (kind !== "error")
      setTimeout(() => {
        if (this.notice()?.text === text) this.notice.set(null);
      }, 3500);
  }
}
