import type {
  ClusterSnapshot,
  MetricSample,
  PreviewTopic,
  TopicPreviewBatch,
} from '@flink-console/shared';
import type { AppConfig } from '../config.js';
import { HealthTracker } from '../access/health.js';
import { KubernetesAccess } from '../access/kubernetes.js';
import { FlinkRestAccess } from '../access/flinkRest.js';
import type { FlinkJobGraph } from '../access/flinkRest.js';
import { KafkaAccess } from '../access/kafka.js';
import { StorageAccess } from '../access/storage.js';
import { kubectl } from '../access/exec.js';
import { buildTopology } from './topology.js';
import { shortVertexName } from './vertexNames.js';
import type { StreamHub } from '../streaming/hub.js';

/**
 * Polls every access module on bounded intervals, normalizes results into the
 * shared contract shapes, retains the latest snapshot/topology and a bounded
 * telemetry timeline, and pushes updates to the stream hub. Each source is polled
 * and failed independently so one outage never blanks the console.
 */
export class Aggregator {
  readonly health: HealthTracker;
  private readonly kube: KubernetesAccess;
  private readonly flink: FlinkRestAccess;
  private readonly kafka: KafkaAccess;
  private readonly storage: StorageAccess;

  private snapshot: ClusterSnapshot = { generatedAt: 0 };
  private lastJobGraph?: FlinkJobGraph;
  private readonly timeline: MetricSample[] = [];
  private readonly previews = new Map<PreviewTopic, TopicPreviewBatch>();
  private timers: NodeJS.Timeout[] = [];
  // Guards against overlapping exec-based polls stacking up inside the broker's
  // own container if a previous call is still running when the next tick fires.
  private lagInFlight = false;
  private readonly previewInFlight = new Set<PreviewTopic>();
  // When true, all broker-exec polling is suspended. The scenario scripts poll
  // consumer-group lag themselves with uncapped-heap JVMs inside the broker pod,
  // and the broker is under peak load exactly then — the console must not add
  // its own exec JVMs on top or the broker's 1Gi cgroup OOMs (observed live).
  private kafkaPollsSuspended: () => boolean = () => false;

  constructor(
    private readonly config: AppConfig,
    private readonly hub: StreamHub,
  ) {
    this.health = new HealthTracker(['kubernetes', 'flink-rest', 'kafka', 'storage']);
    this.kube = new KubernetesAccess(config, this.health);
    this.flink = new FlinkRestAccess(config, this.health);
    this.kafka = new KafkaAccess(config, this.health);
    this.storage = new StorageAccess(config, this.health);
  }

  start(): void {
    const { intervals } = this.config;
    this.timers.push(setInterval(() => void this.pollCore(), intervals.snapshotMs));
    this.timers.push(setInterval(() => void this.pollStorage(), intervals.storageMs));
    this.timers.push(setInterval(() => void this.pollKafkaLag(), intervals.kafkaLagMs));
    this.timers.push(
      setInterval(() => void this.pollPreview(this.config.flink.inTopic as PreviewTopic), intervals.previewMs),
    );
    // Stagger the two preview execs so they never land on the same tick as each
    // other (or as the lag poll) inside the broker's own container.
    const outTimer = setTimeout(() => {
      void this.pollPreview(this.config.flink.outTopic as PreviewTopic);
      this.timers.push(
        setInterval(
          () => void this.pollPreview(this.config.flink.outTopic as PreviewTopic),
          intervals.previewMs,
        ),
      );
    }, Math.floor(intervals.previewMs / 2));
    this.timers.push(outTimer);
    this.timers.push(setInterval(() => this.hub.broadcast({ type: 'signalHealth', payload: this.health.snapshot() }), 3000));
    // Prime immediately.
    void this.pollCore();
    void this.pollStorage();
    void this.pollKafkaLag();
  }

  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    this.flink.close();
  }

  /** Register a probe that reports whether broker-exec polling must pause
   * (e.g. while a scenario script is running its own lag polls). */
  setKafkaPollSuspensionProbe(probe: () => boolean): void {
    this.kafkaPollsSuspended = probe;
  }

  getSnapshot(): ClusterSnapshot {
    return this.snapshot;
  }

  getTimeline(): MetricSample[] {
    return this.timeline;
  }

  getTopology() {
    return buildTopology(this.config, { snapshot: this.snapshot, jobGraph: this.lastJobGraph });
  }

  getPreview(topic: PreviewTopic): TopicPreviewBatch | undefined {
    return this.previews.get(topic);
  }

  private async pollCore(): Promise<void> {
    const [kubeState, jobGraph, checkpoints, loadRate] = await Promise.all([
      this.kube.read(),
      this.flink.readJobGraph(),
      this.flink.readCheckpoints(),
      this.readLoadRate(),
    ]);

    if (jobGraph) this.lastJobGraph = jobGraph;

    const maxParallelism = jobGraph
      ? Math.max(0, ...jobGraph.vertices.map((v) => v.parallelism))
      : this.snapshot.maxParallelism;
    const busyValues = jobGraph?.vertices
      .map((v) => v.busy)
      .filter((b): b is number => b !== undefined);
    const maxVertexBusy =
      busyValues && busyValues.length > 0 ? Math.max(...busyValues) : this.snapshot.maxVertexBusy;
    const vertexParallelism = jobGraph
      ? Object.fromEntries(jobGraph.vertices.map((v) => [shortVertexName(v.name), v.parallelism]))
      : undefined;
    const pendingRecords =
      jobGraph?.vertices.find((v) => v.pendingRecords !== undefined)?.pendingRecords ??
      this.snapshot.pendingRecords;

    this.snapshot = {
      ...this.snapshot,
      jobState: kubeState?.jobState ?? jobGraph?.state ?? this.snapshot.jobState,
      taskManagerCount: kubeState?.taskManagerCount ?? this.snapshot.taskManagerCount,
      taskManagerCpu: kubeState?.taskManagerCpu ?? this.snapshot.taskManagerCpu,
      taskManagerMemory: kubeState?.taskManagerMemory ?? this.snapshot.taskManagerMemory,
      maxVertexBusy,
      maxParallelism,
      pendingRecords,
      lastCheckpointAgeSeconds:
        checkpoints?.lastCheckpointAgeSeconds ?? this.snapshot.lastCheckpointAgeSeconds,
      lastAutoscalerEvent: kubeState?.lastAutoscalerEvent ?? this.snapshot.lastAutoscalerEvent,
      loadRate: loadRate ?? this.snapshot.loadRate,
      generatedAt: Date.now(),
    };

    const sample: MetricSample = {
      t: this.snapshot.generatedAt,
      taskManagerCount: this.snapshot.taskManagerCount,
      maxParallelism: this.snapshot.maxParallelism,
      maxVertexBusy: this.snapshot.maxVertexBusy,
      vertexParallelism,
      pendingRecords: this.snapshot.pendingRecords,
      committedLag: this.snapshot.committedLag,
      lastCheckpointAgeSeconds: this.snapshot.lastCheckpointAgeSeconds,
      loadRate: this.snapshot.loadRate,
    };
    this.timeline.push(sample);
    if (this.timeline.length > this.config.timelineMaxSamples) {
      this.timeline.shift();
    }

    this.hub.broadcast({ type: 'snapshot', payload: this.snapshot });
    this.hub.broadcast({ type: 'metricSample', payload: sample });
    this.hub.broadcast({
      type: 'topology',
      payload: buildTopology(this.config, { snapshot: this.snapshot, jobGraph }),
    });
  }

  private async pollKafkaLag(): Promise<void> {
    if (this.kafkaPollsSuspended()) return;
    if (this.lagInFlight) return;
    this.lagInFlight = true;
    try {
      const lag = await this.kafka.readCommittedLag();
      if (lag === undefined) return;
      this.snapshot = { ...this.snapshot, committedLag: lag, generatedAt: Date.now() };
      this.hub.broadcast({ type: 'snapshot', payload: this.snapshot });
    } finally {
      this.lagInFlight = false;
    }
  }

  private async pollStorage(): Promise<void> {
    const counts = await this.storage.readCounts();
    if (!counts) return;
    this.snapshot = {
      ...this.snapshot,
      checkpointCount: counts.checkpointCount ?? this.snapshot.checkpointCount,
      savepointCount: counts.savepointCount ?? this.snapshot.savepointCount,
      generatedAt: Date.now(),
    };
    this.hub.broadcast({ type: 'snapshot', payload: this.snapshot });
  }

  private async pollPreview(topic: PreviewTopic): Promise<void> {
    if (this.kafkaPollsSuspended()) return;
    if (this.previewInFlight.has(topic)) return;
    this.previewInFlight.add(topic);
    try {
      const result = await this.kafka.previewTopic(topic, this.config.previewMaxRecords);
      if (!result) return;
      const batch: TopicPreviewBatch = {
        topic,
        records: result.records,
        observedRate: result.observedRate,
        generatedAt: Date.now(),
      };
      this.previews.set(topic, batch);
      this.hub.broadcast({ type: 'topicPreview', payload: batch });
    } finally {
      this.previewInFlight.delete(topic);
    }
  }

  private async readLoadRate(): Promise<number | undefined> {
    try {
      const res = await kubectl([
        '-n',
        this.config.namespaces.loadgen,
        'get',
        'configmap',
        'loadgen-config',
        '-o',
        'jsonpath={.data.EVENTS_PER_SEC}',
      ]);
      const rate = Number.parseInt(res.stdout.trim(), 10);
      return Number.isFinite(rate) ? rate : undefined;
    } catch {
      return undefined;
    }
  }
}
