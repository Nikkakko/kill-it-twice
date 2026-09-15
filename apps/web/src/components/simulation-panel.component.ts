import { CommonModule } from "@angular/common";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { ActionKey, SinkName, SinkState } from "../models";

@Component({
  selector: "app-simulation-panel",
  standalone: true,
  imports: [CommonModule],
  template: ` <div class="panel">
    <div class="panel-title">
      <div>
        <h2>Simulation</h2>
        <p class="muted">Trigger controlled failures.</p>
      </div>
      <span class="live-dot">● live</span>
    </div>
    <div class="sim">
      <span>Elasticsearch</span
      ><button
        (click)="sink.emit({ name: 'search', state: 'down' })"
        [disabled]="busy !== null"
      >
        <span *ngIf="busy === 'sink:search:down'" class="spinner"></span
        >Down</button
      ><button
        class="secondary"
        (click)="sink.emit({ name: 'search', state: 'up' })"
        [disabled]="busy !== null"
      >
        <span *ngIf="busy === 'sink:search:up'" class="spinner dark"></span>Up
      </button>
    </div>
    <div class="sim">
      <span>RabbitMQ publisher</span
      ><button
        (click)="sink.emit({ name: 'events', state: 'down' })"
        [disabled]="busy !== null"
      >
        <span *ngIf="busy === 'sink:events:down'" class="spinner"></span
        >Down</button
      ><button
        class="secondary"
        (click)="sink.emit({ name: 'events', state: 'up' })"
        [disabled]="busy !== null"
      >
        <span *ngIf="busy === 'sink:events:up'" class="spinner dark"></span>Up
      </button>
    </div>
    <button class="wide" (click)="partial.emit()" [disabled]="busy !== null">
      <span *ngIf="busy === 'partial'" class="spinner"></span>Inject 500 / 3
      invalid
    </button>
  </div>`,
})
export class SimulationPanelComponent {
  @Input() busy: ActionKey | null = null;
  @Output() sink = new EventEmitter<{ name: SinkName; state: SinkState }>();
  @Output() partial = new EventEmitter<void>();
}
