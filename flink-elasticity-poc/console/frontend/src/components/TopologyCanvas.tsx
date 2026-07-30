import { useMemo, useState } from 'react';
import ReactFlow, { Background, Controls } from 'reactflow';
import type { Edge, Node } from 'reactflow';
import 'reactflow/dist/style.css';
import type { TopologyDocument } from '@flink-console/shared';
import { nodeTypes } from './topologyNodes.js';
import type { StorageLaneData } from './topologyNodes.js';

const COLUMN_WIDTH = 220;
const MAIN_ROW_Y = 120;
const STORAGE_ROW_Y = 320;

function layout(
  doc: TopologyDocument,
  collapsed: boolean,
  onToggle: () => void,
): { nodes: Node[]; edges: Edge[] } {
  const mainNodes = doc.nodes.filter((n) => n.kind !== 'storage-lane');
  const storageNode = doc.nodes.find((n) => n.kind === 'storage-lane');

  const xById = new Map<string, number>();
  const nodes: Node[] = mainNodes.map((n, i) => {
    const x = i * COLUMN_WIDTH;
    xById.set(n.id, x);
    return { id: n.id, type: n.kind, position: { x, y: MAIN_ROW_Y }, data: n, draggable: false };
  });

  if (storageNode) {
    const feeders = doc.edges.filter((e) => e.storage && e.target === storageNode.id);
    const xs = feeders.map((e) => xById.get(e.source)).filter((x): x is number => x !== undefined);
    const avgX = xs.length > 0 ? xs.reduce((a, b) => a + b, 0) / xs.length : 0;
    const data: StorageLaneData = { ...storageNode, collapsed, onToggle };
    nodes.push({
      id: storageNode.id,
      type: 'storage-lane',
      position: { x: avgX, y: STORAGE_ROW_Y },
      data,
      draggable: false,
    });
  }

  const edges: Edge[] = doc.edges.map((e) => {
    if (e.storage) {
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        sourceHandle: 'storage',
        targetHandle: 'storage',
        style: { strokeDasharray: '4 4', stroke: '#10b981', strokeWidth: 1.5 },
        animated: false,
      };
    }
    const backpressured = Boolean(e.backpressured);
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      animated: (e.rate ?? 0) > 0,
      label: e.rate !== undefined ? `${Math.round(e.rate)}/s` : undefined,
      labelStyle: { fill: '#cbd5e1', fontSize: 10 },
      labelBgStyle: { fill: '#0f172a' },
      style: {
        stroke: backpressured ? '#f43f5e' : '#6366f1',
        strokeWidth: backpressured ? 3 : 2,
      },
    };
  });

  return { nodes, edges };
}

export function TopologyCanvas({ topology }: { topology?: TopologyDocument }) {
  const [collapsed, setCollapsed] = useState(false);

  const { nodes, edges } = useMemo(() => {
    if (!topology) return { nodes: [] as Node[], edges: [] as Edge[] };
    return layout(topology, collapsed, () => setCollapsed((c) => !c));
  }, [topology, collapsed]);

  if (!topology) {
    return (
      <div className="flex h-[420px] items-center justify-center rounded-lg border border-slate-800 bg-slate-900/40 text-sm text-slate-400">
        Topology unavailable — waiting for the backend.
      </div>
    );
  }

  return (
    <div className="h-[480px] rounded-lg border border-slate-800 bg-slate-950">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
        nodesConnectable={false}
        elementsSelectable={false}
      >
        <Background color="#1e293b" gap={24} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
