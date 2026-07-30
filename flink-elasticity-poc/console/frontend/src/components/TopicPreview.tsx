import type {
  AggregateResultRecord,
  InputEventRecord,
  PreviewTopic,
  TopicPreviewBatch,
} from '@flink-console/shared';

function isInputEvent(v: unknown): v is InputEventRecord {
  return typeof v === 'object' && v !== null && ('device_id' in v || 'event_type' in v);
}

function isAggregateResult(v: unknown): v is AggregateResultRecord {
  return typeof v === 'object' && v !== null && ('running_total' in v || 'avg_value' in v);
}

function InputEventTable({ batch }: { batch: TopicPreviewBatch }) {
  return (
    <table className="w-full text-left text-xs">
      <thead className="text-slate-400">
        <tr>
          <th className="py-1 pr-3">device_id</th>
          <th className="py-1 pr-3">event_type</th>
          <th className="py-1 pr-3">value</th>
          <th className="py-1 pr-3">ts</th>
        </tr>
      </thead>
      <tbody className="text-slate-200">
        {batch.records.map((r, i) => {
          const p = isInputEvent(r.parsed) ? r.parsed : undefined;
          return (
            <tr key={i} className="border-t border-slate-800">
              <td className="py-1 pr-3">{p?.device_id ?? '—'}</td>
              <td className="py-1 pr-3">{p?.event_type ?? '—'}</td>
              <td className="py-1 pr-3">{p?.value ?? '—'}</td>
              <td className="py-1 pr-3 text-slate-400">{p?.ts ?? (p ? '—' : r.raw.slice(0, 40))}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function AggregateResultTable({ batch }: { batch: TopicPreviewBatch }) {
  return (
    <table className="w-full text-left text-xs">
      <thead className="text-slate-400">
        <tr>
          <th className="py-1 pr-3">device_id</th>
          <th className="py-1 pr-3">count</th>
          <th className="py-1 pr-3">avg_value</th>
          <th className="py-1 pr-3">running_total</th>
          <th className="py-1 pr-3">window</th>
        </tr>
      </thead>
      <tbody className="text-slate-200">
        {batch.records.map((r, i) => {
          const p = isAggregateResult(r.parsed) ? r.parsed : undefined;
          const window =
            p?.window_start !== undefined && p?.window_end !== undefined
              ? `${new Date(p.window_start).toLocaleTimeString()}–${new Date(
                  p.window_end,
                ).toLocaleTimeString()}`
              : '—';
          return (
            <tr key={i} className="border-t border-slate-800">
              <td className="py-1 pr-3">{p?.device_id ?? '—'}</td>
              <td className="py-1 pr-3">{p?.count ?? '—'}</td>
              <td className="py-1 pr-3">{p?.avg_value?.toFixed(2) ?? '—'}</td>
              <td className="py-1 pr-3">{p?.running_total ?? (p ? '—' : r.raw.slice(0, 40))}</td>
              <td className="py-1 pr-3 text-slate-400">{window}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function TopicPreview({ topic, batch }: { topic: PreviewTopic; batch?: TopicPreviewBatch }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-slate-200">{topic}</h3>
        <span className="text-xs text-slate-400">
          {batch?.observedRate !== undefined ? `${Math.round(batch.observedRate)} msg/s observed` : '—'}
        </span>
      </div>
      {!batch || batch.records.length === 0 ? (
        <div className="py-6 text-center text-xs text-slate-500">No recent records.</div>
      ) : topic === 'events-in' ? (
        <InputEventTable batch={batch} />
      ) : (
        <AggregateResultTable batch={batch} />
      )}
    </div>
  );
}
