/**
 * Shared contract types for the Flink Elasticity Web Console.
 *
 * These types are the single source of truth for the topology, metric,
 * topic-preview, scenario, cluster-state, and signal-health shapes exchanged
 * between the Fastify backend-for-frontend and the React frontend. The backend
 * normalizes every upstream source (Flink REST, Kubernetes API, Kafka, MinIO)
 * into these shapes so the frontend never needs to know how a value was obtained.
 */

// ---------------------------------------------------------------------------
// Signal health
// ---------------------------------------------------------------------------

export type SignalSource =
  | 'kubernetes'
  | 'flink-rest'
  | 'kafka'
  | 'storage'
  | 'metrics';

export interface SignalHealth {
  source: SignalSource;
  status: 'up' | 'down' | 'unknown';
  /** Human-readable reason when status is not `up`. */
  reason?: string;
  /** Epoch millis of the last successful read from this source. */
  lastOkAt?: number;
  /** Epoch millis of the last read attempt. */
  lastCheckedAt: number;
}

// ---------------------------------------------------------------------------
// Topology
// ---------------------------------------------------------------------------

export type TopologyNodeKind =
  | 'loadgen'
  | 'kafka-topic'
  | 'flink-source'
  | 'flink-operator'
  | 'flink-sink'
  | 'storage-lane';

export interface KafkaTopicMetrics {
  partitions?: number;
  /** Messages produced per second, observed. */
  produceRate?: number;
  /** Consumer-group committed lag (informational; sawtooths on checkpoint). */
  committedLag?: number;
  /** Authoritative real-time backlog reported by the Flink source. */
  pendingRecords?: number;
}

export interface FlinkVertexMetrics {
  /** Flink vertex id. */
  vertexId?: string;
  parallelism?: number;
  /** 0..1 busy ratio (max across subtasks). */
  busy?: number;
  /** 0..1 backpressure ratio (max across subtasks). */
  backpressure?: number;
  /** Records/second flowing out of this vertex. */
  outRate?: number;
}

export interface LoadgenMetrics {
  /** Configured target events/second. */
  targetRate?: number;
}

export interface StorageLaneMetrics {
  checkpointCount?: number;
  savepointCount?: number;
  /** Age in seconds of the most recent completed checkpoint. */
  lastCheckpointAgeSeconds?: number;
}

export interface TopologyNode {
  id: string;
  kind: TopologyNodeKind;
  label: string;
  /** Optional sublabel, e.g. topic name or vertex role. */
  sublabel?: string;
  loadgen?: LoadgenMetrics;
  kafka?: KafkaTopicMetrics;
  flink?: FlinkVertexMetrics;
  storage?: StorageLaneMetrics;
}

export interface TopologyEdge {
  id: string;
  source: string;
  target: string;
  /** Records/second along this edge, if known. */
  rate?: number;
  /** True when the downstream vertex is backpressured. */
  backpressured?: boolean;
  /** Rendered as a dashed "state persistence" edge into the storage lane. */
  storage?: boolean;
}

export interface TopologyDocument {
  nodes: TopologyNode[];
  edges: TopologyEdge[];
  generatedAt: number;
}

// ---------------------------------------------------------------------------
// Cluster snapshot / metrics
// ---------------------------------------------------------------------------

export interface ClusterSnapshot {
  /** FlinkDeployment job state, e.g. RUNNING, SUSPENDED. */
  jobState?: string;
  taskManagerCount?: number;
  /** Declared per-TaskManager CPU from spec.taskManager.resource. */
  taskManagerCpu?: number;
  /** Declared per-TaskManager memory, verbatim (e.g. "1536m"). */
  taskManagerMemory?: string;
  /** Busy ratio (0..1) of the busiest vertex — the S7 vertical-scale signal. */
  maxVertexBusy?: number;
  /** Highest parallelism across vertices. */
  maxParallelism?: number;
  pendingRecords?: number;
  committedLag?: number;
  lastCheckpointAgeSeconds?: number;
  checkpointCount?: number;
  savepointCount?: number;
  loadRate?: number;
  /** Most recent autoscaler event message, if any. */
  lastAutoscalerEvent?: string;
  generatedAt: number;
}

/** A single point appended to the shared telemetry timeline. */
export interface MetricSample {
  t: number;
  taskManagerCount?: number;
  maxParallelism?: number;
  /** Busy ratio (0..1) of the busiest vertex. */
  maxVertexBusy?: number;
  /** Current parallelism per vertex, keyed by shortened stable vertex name. */
  vertexParallelism?: Record<string, number>;
  pendingRecords?: number;
  committedLag?: number;
  lastCheckpointAgeSeconds?: number;
  loadRate?: number;
}

// ---------------------------------------------------------------------------
// Topic preview
// ---------------------------------------------------------------------------

export type PreviewTopic = 'events-in' | 'events-out';

/** A parsed input event (events-in). */
export interface InputEventRecord {
  device_id?: string;
  event_type?: string;
  value?: number;
  ts?: string;
}

/** A parsed aggregate result (events-out). */
export interface AggregateResultRecord {
  device_id?: string;
  count?: number;
  avg_value?: number;
  running_total?: number;
  window_start?: number;
  window_end?: number;
  emitted_at?: number;
}

export interface TopicPreviewRecord {
  /** Raw record payload as read from Kafka. */
  raw: string;
  /** Best-effort parsed JSON object; undefined when the payload is not JSON. */
  parsed?: InputEventRecord | AggregateResultRecord | Record<string, unknown>;
}

export interface TopicPreviewBatch {
  topic: PreviewTopic;
  records: TopicPreviewRecord[];
  /** Observed messages/second for the topic at read time. */
  observedRate?: number;
  generatedAt: number;
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

export type ScenarioId = 's1' | 's2' | 's3' | 's4' | 's5' | 's6' | 's7';

export interface ScenarioDescriptor {
  id: ScenarioId;
  name: string;
  script: string;
  description: string;
  /** True when the scenario mutates the cluster and needs operate mode. */
  mutating: boolean;
}

export type ScenarioStatus = 'idle' | 'running' | 'pass' | 'fail' | 'error';

export interface ScenarioLogLine {
  id: ScenarioId;
  line: string;
  at: number;
}

export interface ScenarioResult {
  id: ScenarioId;
  status: Exclude<ScenarioStatus, 'idle' | 'running'>;
  /** The parsed final summary line, verbatim. */
  summaryLine?: string;
  /** Parsed key/value metrics from the summary line (counts and timings). */
  metrics: Record<string, number>;
  exitCode: number;
  startedAt: number;
  finishedAt: number;
}

export interface ScenarioState {
  id: ScenarioId;
  status: ScenarioStatus;
  startedAt?: number;
  result?: ScenarioResult;
}

// ---------------------------------------------------------------------------
// Streaming envelope
// ---------------------------------------------------------------------------

export type StreamMessage =
  | { type: 'topology'; payload: TopologyDocument }
  | { type: 'snapshot'; payload: ClusterSnapshot }
  | { type: 'metricSample'; payload: MetricSample }
  | { type: 'topicPreview'; payload: TopicPreviewBatch }
  | { type: 'scenarioLog'; payload: ScenarioLogLine }
  | { type: 'scenarioState'; payload: ScenarioState }
  | { type: 'signalHealth'; payload: SignalHealth[] }
  | { type: 'serverInfo'; payload: ServerInfo };

export interface ServerInfo {
  operateMode: boolean;
  scenarios: ScenarioDescriptor[];
  version: string;
}

// ---------------------------------------------------------------------------
// REST responses
// ---------------------------------------------------------------------------

export interface LoadControlRequest {
  rate: number;
}

export interface LoadControlResponse {
  ok: boolean;
  rate: number;
  message: string;
}

export interface ScenarioRunResponse {
  ok: boolean;
  id: ScenarioId;
  message: string;
}

export const SCENARIOS: ScenarioDescriptor[] = [
  {
    id: 's1',
    name: 'Baseline',
    script: 's1-baseline.sh',
    description: 'Steady-state at low load: stable TM count, low backlog, regular checkpoints.',
    mutating: false,
  },
  {
    id: 's2',
    name: 'Scale-up',
    script: 's2-scale-up.sh',
    description: 'Raise load until the autoscaler adds TaskManagers, then drain the backlog.',
    mutating: true,
  },
  {
    id: 's3',
    name: 'Scale-down',
    script: 's3-scale-down.sh',
    description: 'Lower load and confirm delayed scale-down of TaskManagers.',
    mutating: true,
  },
  {
    id: 's4',
    name: 'Suspend',
    script: 's4-suspend.sh',
    description: 'Suspend to zero with a savepoint written to MinIO; job pods terminate.',
    mutating: true,
  },
  {
    id: 's5',
    name: 'Resume',
    script: 's5-resume.sh',
    description: 'Resume from savepoint; first checkpoint succeeds and backlog drains.',
    mutating: true,
  },
  {
    id: 's6',
    name: 'Rescale cost',
    script: 's6-rescale-cost.sh',
    description: 'Manual rescale at small and larger state; measure the restart gap.',
    mutating: true,
  },
  {
    id: 's7',
    name: 'Vertical scale',
    script: 's7-vertical-scale.sh',
    description:
      'Enlarge the TaskManager at constant parallelism; compare busy-time and measure the resize gap.',
    mutating: true,
  },
];
