import type { AppConfig } from '../config.js';
import type { HealthTracker } from './health.js';
import { kubectl } from './exec.js';

export interface StorageCounts {
  checkpointCount?: number;
  savepointCount?: number;
}

const DEFAULT_MC_IMAGE = 'minio/mc:RELEASE.2024-10-29T15-34-59Z';

/**
 * Lists checkpoint/savepoint object counts from MinIO using the same throwaway
 * `mc` pod pattern as `scripts/s4-suspend.sh`. Credentials are read from the
 * in-cluster `minio-credentials` secret and never leave the backend.
 */
export class StorageAccess {
  private readonly mcImage: string;
  private cachedCreds?: { accessKey: string; secretKey: string };

  constructor(
    private readonly config: AppConfig,
    private readonly health: HealthTracker,
  ) {
    this.mcImage = process.env.MINIO_MC_IMAGE ?? DEFAULT_MC_IMAGE;
  }

  private async credentials(): Promise<{ accessKey: string; secretKey: string } | undefined> {
    if (this.cachedCreds) return this.cachedCreds;
    const ns = this.config.namespaces.storage;
    const access = await kubectl([
      '-n',
      ns,
      'get',
      'secret',
      'minio-credentials',
      '-o',
      'jsonpath={.data.accessKey}',
    ]);
    const secret = await kubectl([
      '-n',
      ns,
      'get',
      'secret',
      'minio-credentials',
      '-o',
      'jsonpath={.data.secretKey}',
    ]);
    const accessKey = decodeB64(access.stdout.trim());
    const secretKey = decodeB64(secret.stdout.trim());
    if (!accessKey || !secretKey) return undefined;
    this.cachedCreds = { accessKey, secretKey };
    return this.cachedCreds;
  }

  async readCounts(): Promise<StorageCounts | undefined> {
    if (!this.health.shouldAttempt('storage')) return undefined;
    try {
      const creds = await this.credentials();
      if (!creds) {
        this.health.markDown('storage', 'minio-credentials secret unavailable');
        return undefined;
      }
      const endpoint = `http://minio.${this.config.namespaces.storage}.svc.cluster.local:9000`;
      const script =
        `mc alias set local ${endpoint} ${creds.accessKey} ${creds.secretKey} >/dev/null && ` +
        `echo "cp=$(mc ls --recursive local/checkpoints 2>/dev/null | wc -l)" && ` +
        `echo "sp=$(mc ls --recursive local/savepoints 2>/dev/null | wc -l)"`;
      const res = await kubectl(
        [
          '-n',
          this.config.namespaces.storage,
          'run',
          `console-mc-${Date.now()}`,
          '--quiet',
          '--rm',
          '-i',
          '--restart=Never',
          `--image=${this.mcImage}`,
          '--command',
          '--',
          '/bin/sh',
          '-c',
          script,
        ],
        { timeoutMs: 25_000 },
      );
      const counts = parseCounts(res.stdout);
      this.health.markOk('storage');
      return counts;
    } catch (err) {
      this.health.markDown('storage', errMessage(err));
      return undefined;
    }
  }
}

function parseCounts(out: string): StorageCounts {
  const cp = /cp=(\d+)/.exec(out);
  const sp = /sp=(\d+)/.exec(out);
  return {
    checkpointCount: cp ? Number.parseInt(cp[1], 10) : undefined,
    savepointCount: sp ? Number.parseInt(sp[1], 10) : undefined,
  };
}

function decodeB64(value: string): string {
  if (!value) return '';
  try {
    return Buffer.from(value, 'base64').toString('utf8');
  } catch {
    return '';
  }
}

function errMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
