import { Handle, Position } from 'reactflow';
import type { NodeProps } from 'reactflow';
import type { TopologyNode } from '@flink-console/shared';

function pct(v: number | undefined): string {
  return v === undefined ? '—' : `${Math.round(v * 100)}%`;
}

function rate(v: number | undefined): string {
  return v === undefined ? '—' : `${Math.round(v)}/s`;
}

const cardBase =
  'rounded-lg border px-3 py-2 shadow-sm text-xs min-w-[150px] bg-slate-900 border-slate-700';

export function LoadgenNodeView({ data }: NodeProps<TopologyNode>) {
  return (
    <div className={`${cardBase} border-sky-700`}>
      <Handle type="source" position={Position.Right} />
      <div className="font-semibold text-sky-300">{data.label}</div>
      <div className="text-slate-400">{data.sublabel}</div>
      <div className="mt-1 text-slate-200">target: {rate(data.loadgen?.targetRate)}</div>
    </div>
  );
}

export function KafkaTopicNodeView({ data }: NodeProps<TopologyNode>) {
  const k = data.kafka;
  return (
    <div className={`${cardBase} border-amber-700`}>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
      <div className="font-semibold text-amber-300">{data.label}</div>
      <div className="text-slate-400">{data.sublabel}</div>
      <div className="mt-1 space-y-0.5 text-slate-200">
        {k?.partitions !== undefined && <div>partitions: {k.partitions}</div>}
        {k?.produceRate !== undefined && <div>rate: {rate(k.produceRate)}</div>}
        {k?.pendingRecords !== undefined && (
          <div className="font-medium text-emerald-400">
            pendingRecords: {Math.round(k.pendingRecords)}
          </div>
        )}
        {k?.committedLag !== undefined && (
          <div className="text-slate-500">lag (info): {Math.round(k.committedLag)}</div>
        )}
      </div>
    </div>
  );
}

export function FlinkVertexNodeView({ data }: NodeProps<TopologyNode>) {
  const f = data.flink;
  const backpressured = (f?.backpressure ?? 0) > 0.5;
  return (
    <div
      className={`${cardBase} ${backpressured ? 'border-rose-600' : 'border-indigo-700'}`}
    >
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
      {data.kind !== 'flink-source' && <Handle type="source" position={Position.Bottom} id="storage" />}
      <div className="flex items-center justify-between">
        <div className="font-semibold text-indigo-300">{data.label}</div>
        {f?.parallelism !== undefined && (
          <span className="rounded bg-indigo-950 px-1.5 py-0.5 text-[10px] text-indigo-300">
            p={f.parallelism}
          </span>
        )}
      </div>
      <div className="mt-1 space-y-0.5 text-slate-200">
        <div>busy: {pct(f?.busy)}</div>
        <div className={backpressured ? 'font-medium text-rose-400' : ''}>
          backpressure: {pct(f?.backpressure)}
        </div>
        {f?.outRate !== undefined && <div>out: {rate(f.outRate)}</div>}
      </div>
    </div>
  );
}

export interface StorageLaneData extends TopologyNode {
  collapsed: boolean;
  onToggle: () => void;
}

export function StorageLaneNodeView({ data }: NodeProps<StorageLaneData>) {
  const s = data.storage;
  return (
    <div className={`${cardBase} border-emerald-800 bg-slate-900/90`}>
      <Handle type="target" position={Position.Top} id="storage" />
      <button
        type="button"
        onClick={data.onToggle}
        className="flex w-full items-center justify-between font-semibold text-emerald-300"
      >
        <span>{data.label}</span>
        <span className="text-slate-400">{data.collapsed ? '▸' : '▾'}</span>
      </button>
      {!data.collapsed && (
        <div className="mt-1 space-y-0.5 text-slate-200">
          <div>checkpoints: {s?.checkpointCount ?? '—'}</div>
          <div>savepoints: {s?.savepointCount ?? '—'}</div>
          <div>
            last checkpoint age:{' '}
            {s?.lastCheckpointAgeSeconds !== undefined ? `${s.lastCheckpointAgeSeconds}s` : '—'}
          </div>
        </div>
      )}
    </div>
  );
}

export const nodeTypes = {
  loadgen: LoadgenNodeView,
  'kafka-topic': KafkaTopicNodeView,
  'flink-source': FlinkVertexNodeView,
  'flink-operator': FlinkVertexNodeView,
  'flink-sink': FlinkVertexNodeView,
  'storage-lane': StorageLaneNodeView,
};
