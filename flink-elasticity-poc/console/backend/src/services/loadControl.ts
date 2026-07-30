import path from 'node:path';
import type { LoadControlResponse } from '@flink-console/shared';
import type { AppConfig } from '../config.js';
import { execCapture } from '../access/exec.js';

/**
 * Changes the load generator rate by invoking the existing `scripts/set-load.sh`
 * helper (never a shell string built from client input). The rate is validated as
 * a positive integer before the fixed script is spawned.
 */
export class LoadControl {
  constructor(private readonly config: AppConfig) {}

  validate(rate: unknown): number | undefined {
    if (typeof rate !== 'number' || !Number.isInteger(rate) || rate <= 0) {
      return undefined;
    }
    return rate;
  }

  async setRate(rate: number): Promise<LoadControlResponse> {
    const scriptPath = path.join(this.config.scriptsDir, 'set-load.sh');
    const result = await execCapture('bash', [scriptPath, String(rate)], {
      cwd: this.config.pocRoot,
      timeoutMs: 200_000,
    });
    if (result.code === 0) {
      return { ok: true, rate, message: `Load generator set to ${rate} events/s` };
    }
    return {
      ok: false,
      rate,
      message: (result.stderr || result.stdout || 'set-load.sh failed').trim().slice(-500),
    };
  }
}
