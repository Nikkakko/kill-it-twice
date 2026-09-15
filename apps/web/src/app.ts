import { CommonModule } from "@angular/common";
import { HttpClient } from "@angular/common/http";
import { Component, OnInit, inject } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { interval, startWith, switchMap } from "rxjs";

@Component({
  selector: "app-root",
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <main>
      <header><div><span class="eyebrow">OPTIO PLATFORM</span><h1>Kill It Twice</h1><p>Replication operations console</p></div><span class="pill" [class.bad]="status?.health?.worker !== 'healthy'">● {{ status?.health?.worker || 'connecting' }}</span></header>
      <section class="stats">
        <article><label>Backfill source</label><strong>{{ status?.sourceCount | number }}</strong><small>{{ status?.outboxCount | number }} ordered events</small></article>
        <article><label>Search checkpoint</label><strong>{{ status?.checkpoints?.search | number }}</strong><small>{{ status?.pending?.search | number }} pending</small></article>
        <article><label>Event checkpoint</label><strong>{{ status?.checkpoints?.events | number }}</strong><small>{{ status?.pending?.events | number }} pending</small></article>
        <article><label>Incremental lag</label><strong>{{ status?.incrementalLag | number }}</strong><small>{{ status?.throughputPerSecond | number }} events/s estimate</small></article>
        <article class="danger"><label>DLQ</label><strong>{{ status?.dlqCount | number }}</strong><small>unreplayed records</small></article>
      </section>
      <section class="panel controls"><div><h2>Pipeline control</h2><p>Current state: <b>{{ status?.paused ? 'Paused' : 'Running' }}</b></p></div><div class="actions"><button (click)="control(status?.paused ? 'resume' : 'pause')">{{ status?.paused ? 'Resume' : 'Pause' }}</button><button class="secondary" (click)="seed()">Seed 20k</button><button class="secondary" (click)="replay()">Replay DLQ</button></div></section>
      <section class="grid"><div class="panel"><div class="panel-title"><h2>Replicated records</h2><input [(ngModel)]="query" (keyup.enter)="loadRecords()" placeholder="Search name or email"></div><table><thead><tr><th>Name</th><th>Email</th><th>Segment</th><th>Version</th><th>Updated</th></tr></thead><tbody><tr *ngFor="let record of records"><td>{{ record.name }}</td><td>{{ record.email }}</td><td><span class="tag">{{ record.segment }}</span></td><td>{{ record.version }}</td><td>{{ record.updatedAt | date:'short' }}</td></tr></tbody></table></div><div class="panel"><div class="panel-title"><h2>Simulation</h2></div><p class="muted">Trigger controlled failures to exercise recovery.</p><div class="sim"><span>Elasticsearch</span><button (click)="sink('search','down')">Down</button><button class="secondary" (click)="sink('search','up')">Up</button></div><div class="sim"><span>RabbitMQ publisher</span><button (click)="sink('events','down')">Down</button><button class="secondary" (click)="sink('events','up')">Up</button></div><button class="wide" (click)="partial()">Inject 500 / 3 invalid</button><p class="result">{{ message }}</p></div></section>
      <section class="panel"><div class="panel-title"><h2>Dead-letter queue</h2><button class="secondary" (click)="loadDlq()">Refresh</button></div><table><thead><tr><th>Sink</th><th>Sequence</th><th>Record</th><th>Error</th><th>Created</th></tr></thead><tbody><tr *ngFor="let item of dlq"><td>{{ item.sink }}</td><td>{{ item.sequence }}</td><td class="mono">{{ item.recordId | slice:0:8 }}</td><td>{{ item.errorCode }}</td><td>{{ item.createdAt | date:'short' }}</td></tr><tr *ngIf="!dlq.length"><td colspan="5" class="muted">No unreplayed failures.</td></tr></tbody></table></section>
    </main>`,
  styleUrls: ["./styles.css"],
})
export class AppComponent implements OnInit {
  private readonly http = inject(HttpClient);
  status: any;
  records: any[] = [];
  dlq: any[] = [];
  query = "";
  message = "";
  ngOnInit() {
    interval(2000)
      .pipe(
        startWith(0),
        switchMap(() => this.http.get("/api/status")),
      )
      .subscribe(v => (this.status = v));
    this.loadRecords();
    this.loadDlq();
  }
  control(action: string) {
    this.http
      .post(`/api/pipeline/${action}`, {})
      .subscribe(() => (this.message = `Pipeline ${action} requested`));
  }
  seed() {
    this.http
      .post("/api/simulation/seed", { count: 20000, reset: true })
      .subscribe((v: any) => (this.message = `Seeded ${v.seeded} records`));
  }
  replay() {
    this.http.post("/api/dlq/replay", {}).subscribe((v: any) => {
      this.message = `Replayed ${v.replayed} records`;
      this.loadDlq();
    });
  }
  sink(name: string, state: string) {
    this.http
      .post(`/api/simulation/sink/${name}/${state}`, {})
      .subscribe(() => (this.message = `${name} marked ${state}`));
  }
  partial() {
    this.http.post("/api/simulation/partial", { count: 500 }).subscribe(() => {
      this.message = "Injected 500 events, including 3 invalid";
      this.loadDlq();
    });
  }
  loadRecords() {
    this.http
      .get<any[]>(`/api/records?q=${encodeURIComponent(this.query)}`)
      .subscribe(v => (this.records = v));
  }
  loadDlq() {
    this.http.get<any[]>("/api/dlq").subscribe(v => (this.dlq = v));
  }
}
