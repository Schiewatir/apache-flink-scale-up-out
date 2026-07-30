import { spawn, type ChildProcess } from 'node:child_process';
import type { AppConfig } from '../config.js';
import type { HealthTracker } from './health.js';

export interface FlinkVertexInfo {
  id: string;
  name: string;
  parallelism: number;
  busy?: number;
  backpressure?: number;
  outRate?: number;
  pendingRecords?: number;
}

export interface FlinkJobGraph {
  jobId: string;
  state: string;
  vertices: FlinkVertexInfo[];
}

export interface FlinkCheckpointInfo {
  lastCheckpointAgeSeconds?: number;
  completedCount?: number;
}

const SOURCE_HINT = 'kafka-source';

/**
 * Owns a `kubectl port-forward` to the Flink REST service and proxies REST reads
 * through it. The forward is lazily (re)started with backoff whenever it is not
 * healthy, and torn down on close so no orphaned forward is left behind.
 */
export class FlinkRestAccess {
  private forward?: ChildProcess;
  private forwardReady = false;
  private restarting = false;
  private lastRestartAt = 0;
  private readonly restartCooldownMs = 3000;

  constructor(
    private readonly config: AppConfig,
    private readonly health: HealthTracker,
  ) {}

  private get baseUrl(): string {
    return `http://127.0.0.1:${this.config.flink.localRestPort}`;
  }

  private ensureForward(): void {
    if (this.forwardReady && this.forward && !this.forward.killed) return;
    const now = Date.now();
    if (this.restarting || now - this.lastRestartAt < this.restartCooldownMs) return;
    this.restarting = true;
    this.lastRestartAt = now;

    this.stopForward();
    const svc = `svc/${this.config.flink.deploymentName}-rest`;
    const args = [
      '-n',
      this.config.namespaces.flink,
      'port-forward',
      svc,
      `${this.config.flink.localRestPort}:8081`,
    ];
    const child = spawn('kubectl', args, { shell: false });
    child.stdout.on('data', (chunk: Buffer) => {
      if (chunk.toString('utf8').includes('Forwarding from')) {
        this.forwardReady = true;
        this.restarting = false;
      }
    });
    child.stderr.on('data', () => {
      /* surfaced via health when reads fail */
    });
    child.on('exit', () => {
      this.forwardReady = false;
      this.restarting = false;
    });
    this.forward = child;
    // Give the forward a brief head start before the first read attempt.
    setTimeout(() => {
      this.restarting = false;
    }, this.restartCooldownMs);
  }

  private async fetchJson<T>(pathname: string): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 4000);
    try {
      const res = await fetch(`${this.baseUrl}${pathname}`, { signal: controller.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${pathname}`);
      return (await res.json()) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  async readJobGraph(): Promise<FlinkJobGraph | undefined> {
    this.ensureForward();
    if (!this.health.shouldAttempt('flink-rest')) return undefined;
    try {
      const jobs = await this.fetchJson<{ jobs: Array<{ id: string; status: string }> }>('/jobs');
      const job = jobs.jobs?.find((j) => j.status === 'RUNNING') ?? jobs.jobs?.[0];
      if (!job) {
        this.health.markDown('flink-rest', 'no jobs reported');
        return undefined;
      }
      const detail = await this.fetchJson<{
        state: string;
        vertices: Array<{ id: string; name: string; parallelism: number; metrics?: unknown }>;
      }>(`/jobs/${job.id}`);

      const vertices: FlinkVertexInfo[] = [];
      for (const v of detail.vertices ?? []) {
        vertices.push(await this.readVertexMetrics(job.id, v));
      }

      this.health.markOk('flink-rest');
      return { jobId: job.id, state: detail.state, vertices };
    } catch (err) {
      this.health.markDown('flink-rest', errMessage(err));
      return undefined;
    }
  }

  private async readVertexMetrics(
    jobId: string,
    vertex: { id: string; name: string; parallelism: number },
  ): Promise<FlinkVertexInfo> {
    const info: FlinkVertexInfo = {
      id: vertex.id,
      name: vertex.name,
      parallelism: vertex.parallelism,
    };
    try {
      const available = await this.fetchJson<Array<{ id: string }>>(
        `/jobs/${jobId}/vertices/${vertex.id}/metrics`,
      );
      const ids = available.map((m) => m.id);
      // Flink namespaces per-subtask task-level metrics as `<subtaskIndex>.<metricName>`
      // (e.g. `0.busyTimeMsPerSecond`) — there is no un-indexed form. Chained
      // operators additionally expose per-operator-in-chain variants of
      // numRecordsOutPerSecond (e.g. `0.parse-json.numRecordsOutPerSecond`); only the
      // bare task-level metric reflects the whole chain's output, so it must be
      // matched precisely rather than by suffix.
      const taskLevelPattern = /^\d+\.(busyTimeMsPerSecond|backPressuredTimeMsPerSecond|numRecordsOutPerSecond)$/;
      const wanted = ids.filter((id) => id.endsWith('pendingRecords') || taskLevelPattern.test(id));
      if (wanted.length === 0) return info;
      const values = await this.fetchJson<Array<{ id: string; value: string }>>(
        `/jobs/${jobId}/vertices/${vertex.id}/metrics?get=${wanted.join(',')}`,
      );
      let pendingSum = 0;
      let hasPending = false;
      let outRateSum = 0;
      let hasOutRate = false;
      const busyValues: number[] = [];
      const backpressureValues: number[] = [];
      for (const m of values) {
        const num = Number.parseFloat(m.value);
        if (!Number.isFinite(num)) continue;
        if (m.id.endsWith('pendingRecords')) {
          pendingSum += num;
          hasPending = true;
        } else if (m.id.endsWith('.busyTimeMsPerSecond')) {
          busyValues.push(num);
        } else if (m.id.endsWith('.backPressuredTimeMsPerSecond')) {
          backpressureValues.push(num);
        } else if (m.id.endsWith('.numRecordsOutPerSecond')) {
          outRateSum += num;
          hasOutRate = true;
        }
      }
      // Busy/backpressure are ratios: take the max across subtasks, per the shared
      // contract's documented semantics. Throughput and backlog are additive.
      if (busyValues.length > 0) info.busy = clamp01(Math.max(...busyValues) / 1000);
      if (backpressureValues.length > 0) {
        info.backpressure = clamp01(Math.max(...backpressureValues) / 1000);
      }
      if (hasOutRate) info.outRate = outRateSum;
      if (hasPending && vertex.name.includes(SOURCE_HINT)) {
        info.pendingRecords = Math.round(pendingSum);
      }
    } catch {
      /* leave metrics undefined; health reflects the top-level read */
    }
    return info;
  }

  async readCheckpoints(): Promise<FlinkCheckpointInfo | undefined> {
    if (!this.health.shouldAttempt('flink-rest')) return undefined;
    try {
      const jobs = await this.fetchJson<{ jobs: Array<{ id: string; status: string }> }>('/jobs');
      const job = jobs.jobs?.find((j) => j.status === 'RUNNING') ?? jobs.jobs?.[0];
      if (!job) return undefined;
      const cp = await this.fetchJson<{
        latest?: { completed?: { trigger_timestamp?: number } };
        counts?: { completed?: number };
      }>(`/jobs/${job.id}/checkpoints`);
      const ts = cp.latest?.completed?.trigger_timestamp;
      return {
        lastCheckpointAgeSeconds:
          ts !== undefined ? Math.max(0, Math.round((Date.now() - ts) / 1000)) : undefined,
        completedCount: cp.counts?.completed,
      };
    } catch (err) {
      this.health.markDown('flink-rest', errMessage(err));
      return undefined;
    }
  }

  private stopForward(): void {
    if (this.forward && !this.forward.killed) {
      this.forward.kill('SIGTERM');
    }
    this.forward = undefined;
    this.forwardReady = false;
  }

  close(): void {
    this.stopForward();
  }
}

function clamp01(n: number): number {
  return Math.max(0, Math.min(1, n));
}

function errMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
