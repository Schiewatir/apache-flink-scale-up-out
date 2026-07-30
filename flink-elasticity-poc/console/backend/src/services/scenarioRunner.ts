import { spawn, type ChildProcess } from 'node:child_process';
import path from 'node:path';
import type {
  ScenarioDescriptor,
  ScenarioId,
  ScenarioResult,
  ScenarioState,
} from '@flink-console/shared';
import { SCENARIOS } from '@flink-console/shared';
import type { AppConfig } from '../config.js';
import type { StreamHub } from '../streaming/hub.js';

/**
 * Executes the existing S1–S7 scenario scripts as the single source of truth for
 * elasticity logic. It streams each script's stdout line by line, enforces a
 * single-run lock, and parses the final `PASS|FAIL S<n> ...` summary line into
 * structured metrics. Only the fixed script allow-list is ever spawned.
 */
export class ScenarioRunner {
  private readonly byId = new Map<ScenarioId, ScenarioDescriptor>(
    SCENARIOS.map((s) => [s.id, s]),
  );
  private readonly states = new Map<ScenarioId, ScenarioState>();
  private running?: { id: ScenarioId; child: ChildProcess };

  constructor(
    private readonly config: AppConfig,
    private readonly hub: StreamHub,
  ) {
    for (const s of SCENARIOS) {
      this.states.set(s.id, { id: s.id, status: 'idle' });
    }
  }

  getStates(): ScenarioState[] {
    return [...this.states.values()];
  }

  descriptor(id: ScenarioId): ScenarioDescriptor | undefined {
    return this.byId.get(id);
  }

  isRunning(): boolean {
    return this.running !== undefined;
  }

  /** Returns an error message when the run cannot start, otherwise undefined. */
  run(id: ScenarioId): string | undefined {
    const descriptor = this.byId.get(id);
    if (!descriptor) return `Unknown scenario: ${id}`;
    if (this.running) {
      return `Scenario ${this.running.id} is already running; only one runs at a time.`;
    }

    const scriptPath = path.join(this.config.scriptsDir, descriptor.script);
    const startedAt = Date.now();
    const child = spawn('bash', [scriptPath], {
      cwd: this.config.pocRoot,
      shell: false,
    });
    this.running = { id, child };
    this.setState({ id, status: 'running', startedAt });

    let lastSummary: string | undefined;
    const handleChunk = (chunk: Buffer) => {
      const text = chunk.toString('utf8');
      for (const line of text.split('\n')) {
        const trimmed = line.replace(/\s+$/, '');
        if (trimmed.length === 0) continue;
        this.hub.broadcast({ type: 'scenarioLog', payload: { id, line: trimmed, at: Date.now() } });
        if (/^(PASS|FAIL)\b/.test(trimmed)) {
          lastSummary = trimmed;
        }
      }
    };
    child.stdout.on('data', handleChunk);
    child.stderr.on('data', handleChunk);

    child.on('error', (err) => {
      this.finish(id, startedAt, -1, undefined, `error: ${err.message}`);
    });
    child.on('close', (code) => {
      this.finish(id, startedAt, code ?? -1, lastSummary);
    });

    return undefined;
  }

  private finish(
    id: ScenarioId,
    startedAt: number,
    exitCode: number,
    summaryLine?: string,
    errorLine?: string,
  ): void {
    this.running = undefined;
    const finishedAt = Date.now();
    const status = deriveStatus(exitCode, summaryLine);
    const result: ScenarioResult = {
      id,
      status,
      summaryLine: summaryLine ?? errorLine,
      metrics: parseSummaryMetrics(summaryLine),
      exitCode,
      startedAt,
      finishedAt,
    };
    this.setState({ id, status, startedAt, result });
  }

  private setState(state: ScenarioState): void {
    this.states.set(state.id, state);
    this.hub.broadcast({ type: 'scenarioState', payload: state });
  }
}

function deriveStatus(
  exitCode: number,
  summaryLine?: string,
): Exclude<ScenarioResult['status'], never> {
  if (summaryLine?.startsWith('PASS')) return 'pass';
  if (summaryLine?.startsWith('FAIL')) return 'fail';
  if (exitCode === 0) return 'pass';
  return 'error';
}

/** Extract numeric key=value pairs from a summary line such as
 * `PASS S2 scale-up rate=12000 base_tm=1 max_tm=2 scaleup_in_s=221`. */
export function parseSummaryMetrics(summaryLine?: string): Record<string, number> {
  const metrics: Record<string, number> = {};
  if (!summaryLine) return metrics;
  const pattern = /([a-zA-Z_][a-zA-Z0-9_]*)=(-?\d+(?:\.\d+)?)/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(summaryLine)) !== null) {
    const value = Number.parseFloat(match[2]);
    if (Number.isFinite(value)) metrics[match[1]] = value;
  }
  return metrics;
}
