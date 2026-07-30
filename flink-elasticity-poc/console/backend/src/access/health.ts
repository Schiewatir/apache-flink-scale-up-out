import type { SignalHealth, SignalSource } from '@flink-console/shared';

/**
 * Tracks per-source health independently so one unavailable upstream never blanks
 * the whole console. Each source records its last successful read, last attempt,
 * and a reason when down, plus a simple exponential backoff hint.
 */
export class HealthTracker {
  private readonly state = new Map<SignalSource, SignalHealth>();
  private readonly backoff = new Map<SignalSource, number>();
  private readonly nextAllowedAt = new Map<SignalSource, number>();

  private readonly baseBackoffMs = 2000;
  private readonly maxBackoffMs = 30_000;

  constructor(sources: SignalSource[]) {
    const now = Date.now();
    for (const source of sources) {
      this.state.set(source, { source, status: 'unknown', lastCheckedAt: now });
      this.backoff.set(source, this.baseBackoffMs);
      this.nextAllowedAt.set(source, 0);
    }
  }

  markOk(source: SignalSource): void {
    const now = Date.now();
    this.state.set(source, { source, status: 'up', lastOkAt: now, lastCheckedAt: now });
    this.backoff.set(source, this.baseBackoffMs);
    this.nextAllowedAt.set(source, 0);
  }

  markDown(source: SignalSource, reason: string): void {
    const now = Date.now();
    const prev = this.state.get(source);
    this.state.set(source, {
      source,
      status: 'down',
      reason,
      lastOkAt: prev?.lastOkAt,
      lastCheckedAt: now,
    });
    const current = this.backoff.get(source) ?? this.baseBackoffMs;
    const next = Math.min(current * 2, this.maxBackoffMs);
    this.backoff.set(source, next);
    this.nextAllowedAt.set(source, now + current);
  }

  /** True when enough backoff has elapsed to retry a currently-down source. */
  shouldAttempt(source: SignalSource): boolean {
    const health = this.state.get(source);
    if (!health || health.status !== 'down') return true;
    return Date.now() >= (this.nextAllowedAt.get(source) ?? 0);
  }

  snapshot(): SignalHealth[] {
    return [...this.state.values()];
  }
}
