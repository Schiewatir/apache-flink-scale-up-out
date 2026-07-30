import type {
  ClusterSnapshot,
  TopologyDocument,
  TopologyEdge,
  TopologyNode,
} from '@flink-console/shared';
import type { FlinkJobGraph, FlinkVertexInfo } from '../access/flinkRest.js';
import type { AppConfig } from '../config.js';

/**
 * Maps live cluster signals into the normalized topology document the frontend
 * renders. The graph is a fixed narrative shape — loadgen -> events-in -> Flink
 * vertices -> events-out — with a MinIO storage lane hanging off the stateful
 * vertices. When the Flink job graph is available its vertices/metrics are used;
 * otherwise a static skeleton is emitted so the canvas is never blank.
 */

interface TopologyInputs {
  snapshot: ClusterSnapshot;
  jobGraph?: FlinkJobGraph;
}

const VERTEX_ORDER = [
  { key: 'source', match: 'kafka-source', label: 'Kafka Source', kind: 'flink-source' as const },
  { key: 'parse', match: 'parse-json', label: 'Parse JSON', kind: 'flink-operator' as const },
  {
    key: 'running-total',
    match: 'running-total',
    label: 'Running Total (state)',
    kind: 'flink-operator' as const,
  },
  {
    key: 'window',
    match: 'windowed-aggregate',
    label: 'Sliding Window',
    kind: 'flink-operator' as const,
  },
  { key: 'sink', match: 'kafka-sink', label: 'Kafka Sink', kind: 'flink-sink' as const },
];

const STATEFUL_KEYS = new Set(['running-total', 'window']);

export function buildTopology(config: AppConfig, inputs: TopologyInputs): TopologyDocument {
  const { snapshot, jobGraph } = inputs;
  const nodes: TopologyNode[] = [];
  const edges: TopologyEdge[] = [];

  nodes.push({
    id: 'loadgen',
    kind: 'loadgen',
    label: 'Load Generator',
    sublabel: 'JSON events',
    loadgen: { targetRate: snapshot.loadRate },
  });

  nodes.push({
    id: 'events-in',
    kind: 'kafka-topic',
    label: config.flink.inTopic,
    sublabel: '12 partitions',
    kafka: {
      partitions: 12,
      produceRate: snapshot.loadRate,
      committedLag: snapshot.committedLag,
      pendingRecords: snapshot.pendingRecords,
    },
  });
  edges.push({ id: 'e-loadgen-in', source: 'loadgen', target: 'events-in', rate: snapshot.loadRate });

  const vertexByKey = matchVertices(jobGraph?.vertices ?? []);
  let prevId = 'events-in';
  for (const spec of VERTEX_ORDER) {
    const vertex = vertexByKey.get(spec.key);
    const nodeId = `flink-${spec.key}`;
    nodes.push({
      id: nodeId,
      kind: spec.kind,
      label: spec.label,
      sublabel: vertex ? `p=${vertex.parallelism}` : undefined,
      flink: vertex
        ? {
            vertexId: vertex.id,
            parallelism: vertex.parallelism,
            busy: vertex.busy,
            backpressure: vertex.backpressure,
            outRate: vertex.outRate,
          }
        : undefined,
    });
    edges.push({
      id: `e-${prevId}-${nodeId}`,
      source: prevId,
      target: nodeId,
      rate: vertex?.outRate,
      backpressured: (vertex?.backpressure ?? 0) > 0.5,
    });
    prevId = nodeId;
  }

  nodes.push({
    id: 'events-out',
    kind: 'kafka-topic',
    label: config.flink.outTopic,
    sublabel: 'aggregate results',
    kafka: { produceRate: vertexByKey.get('sink')?.outRate },
  });
  edges.push({ id: 'e-sink-out', source: prevId, target: 'events-out' });

  nodes.push({
    id: 'storage-lane',
    kind: 'storage-lane',
    label: 'MinIO Storage',
    sublabel: 'checkpoints / savepoints',
    storage: {
      checkpointCount: snapshot.checkpointCount,
      savepointCount: snapshot.savepointCount,
      lastCheckpointAgeSeconds: snapshot.lastCheckpointAgeSeconds,
    },
  });
  for (const spec of VERTEX_ORDER) {
    if (!STATEFUL_KEYS.has(spec.key)) continue;
    edges.push({
      id: `e-${spec.key}-storage`,
      source: `flink-${spec.key}`,
      target: 'storage-lane',
      storage: true,
    });
  }

  return { nodes, edges, generatedAt: Date.now() };
}

function matchVertices(vertices: FlinkVertexInfo[]): Map<string, FlinkVertexInfo> {
  const byKey = new Map<string, FlinkVertexInfo>();
  for (const spec of VERTEX_ORDER) {
    const found = vertices.find((v) => v.name.includes(spec.match));
    if (found) byKey.set(spec.key, found);
  }
  return byKey;
}
