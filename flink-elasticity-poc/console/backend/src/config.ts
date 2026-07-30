import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const here = path.dirname(fileURLToPath(import.meta.url));

/**
 * Resolve the PoC root (the directory that contains `scripts/` and
 * `manifests/`). At build time this file lives in `console/backend/dist`, so the
 * PoC root is three levels up. We verify by probing for `scripts/common.sh` and
 * fall back to the current working directory when the layout differs.
 */
function resolvePocRoot(): string {
  const candidates = [
    path.resolve(here, '..', '..', '..'), // dist -> backend -> console -> poc
    path.resolve(here, '..', '..', '..', '..'),
    process.cwd(),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, 'scripts', 'common.sh'))) {
      return candidate;
    }
  }
  return candidates[0];
}

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export interface AppConfig {
  host: string;
  port: number;
  /** When false, all mutating endpoints are rejected. */
  operateMode: boolean;
  pocRoot: string;
  scriptsDir: string;
  frontendDir: string;
  namespaces: {
    flink: string;
    kafka: string;
    loadgen: string;
    storage: string;
  };
  flink: {
    deploymentName: string;
    jobLabel: string;
    /** Local port the managed port-forward binds to. */
    localRestPort: number;
    inTopic: string;
    outTopic: string;
    consumerGroup: string;
  };
  intervals: {
    snapshotMs: number;
    topologyMs: number;
    /** Consumer-group lag requires exec-ing a JVM inside the broker pod; keep this
     * far longer than snapshotMs so it doesn't stack up against the broker's own
     * memory limit. */
    kafkaLagMs: number;
    previewMs: number;
    storageMs: number;
  };
  previewMaxRecords: number;
  timelineMaxSamples: number;
  version: string;
}

export function loadConfig(argv: string[]): AppConfig {
  const operateMode = argv.includes('--operate') || process.env.CONSOLE_OPERATE === '1';
  const pocRoot = resolvePocRoot();
  const frontendDir = path.resolve(here, '..', '..', 'frontend', 'dist');

  return {
    host: process.env.CONSOLE_HOST ?? '127.0.0.1',
    port: intEnv('CONSOLE_PORT', 8088),
    operateMode,
    pocRoot,
    scriptsDir: path.join(pocRoot, 'scripts'),
    frontendDir,
    namespaces: {
      flink: process.env.NAMESPACE_FLINK ?? 'flink-jobs',
      kafka: process.env.NAMESPACE_KAFKA ?? 'kafka',
      loadgen: process.env.NAMESPACE_LOADGEN ?? 'loadgen',
      storage: process.env.NAMESPACE_STORAGE ?? 'storage',
    },
    flink: {
      deploymentName: process.env.FLINK_DEPLOYMENT ?? 'flink-elastic-job',
      jobLabel: process.env.FLINK_JOB_LABEL ?? 'app=flink-elastic-job',
      localRestPort: intEnv('FLINK_LOCAL_REST_PORT', 18081),
      inTopic: process.env.KAFKA_INPUT_TOPIC ?? 'events-in',
      outTopic: process.env.KAFKA_OUTPUT_TOPIC ?? 'events-out',
      consumerGroup: process.env.KAFKA_GROUP_ID ?? 'flink-elastic-consumer',
    },
    intervals: {
      snapshotMs: intEnv('CONSOLE_SNAPSHOT_MS', 3000),
      topologyMs: intEnv('CONSOLE_TOPOLOGY_MS', 3000),
      // kafka-consumer-groups.sh and kafka-console-consumer.sh each spawn a full JVM
      // inside the broker's own (memory-constrained) container via `kubectl exec`.
      // Polling these on the same cadence as the lightweight K8s/Flink REST reads
      // stacks multiple extra JVMs against the broker's cgroup memory limit and can
      // OOM-kill it. These intervals are deliberately much longer.
      kafkaLagMs: intEnv('CONSOLE_KAFKA_LAG_MS', 20_000),
      previewMs: intEnv('CONSOLE_PREVIEW_MS', 12_000),
      storageMs: intEnv('CONSOLE_STORAGE_MS', 30_000),
    },
    previewMaxRecords: intEnv('CONSOLE_PREVIEW_MAX', 12),
    timelineMaxSamples: intEnv('CONSOLE_TIMELINE_MAX', 240),
    version: '0.1.0',
  };
}
