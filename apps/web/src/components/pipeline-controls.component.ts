import { CommonModule } from "@angular/common";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { ActionKey, PipelineAction, PipelineStatus } from "../models";

@Component({
  selector: "app-pipeline-controls",
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: ` <section class="panel controls">
    <div>
      <h2>Pipeline control</h2>
      <p>
        Current state: <b>{{ status?.paused ? "Paused" : "Running" }}</b>
      </p>
    </div>
    <div class="actions">
      <label class="config"
        >Batch size
        <input
          type="number"
          min="1"
          max="1000"
          [ngModel]="batchSize"
          (ngModelChange)="batchSizeChange.emit($event)"
          (change)="configChange.emit()"
          [disabled]="busy !== null"
      /></label>
      <button
        (click)="control.emit(status?.paused ? 'resume' : 'pause')"
        [disabled]="busy !== null || !status"
      >
        <span
          *ngIf="busy === 'control'; else controlText"
          class="spinner"
        ></span
        ><ng-template #controlText>{{
          status?.paused ? "Resume" : "Pause"
        }}</ng-template>
      </button>
      <button
        class="secondary"
        (click)="seed.emit()"
        [disabled]="busy !== null"
      >
        <span *ngIf="busy === 'seed'; else seedText" class="spinner dark"></span
        ><ng-template #seedText>Seed 20k</ng-template>
      </button>
      <button
        class="secondary"
        (click)="replay.emit()"
        [disabled]="busy !== null || !status?.dlqCount"
      >
        <span
          *ngIf="busy === 'replay'; else replayText"
          class="spinner dark"
        ></span
        ><ng-template #replayText>Replay DLQ</ng-template>
      </button>
    </div>
  </section>`,
})
export class PipelineControlsComponent {
  @Input() status: PipelineStatus | null = null;
  @Input() batchSize = 100;
  @Input() busy: ActionKey | null = null;
  @Output() control = new EventEmitter<PipelineAction>();
  @Output() seed = new EventEmitter<void>();
  @Output() replay = new EventEmitter<void>();
  @Output() configChange = new EventEmitter<void>();
  @Output() batchSizeChange = new EventEmitter<number>();
}
