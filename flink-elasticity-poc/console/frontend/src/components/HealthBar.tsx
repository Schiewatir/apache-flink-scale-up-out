import type { SignalHealth } from '@flink-console/shared';

const STATUS_DOT: Record<SignalHealth['status'], string> = {
  up: 'bg-emerald-500',
  down: 'bg-rose-500',
  unknown: 'bg-slate-500',
};

const SOURCE_LABEL: Record<SignalHealth['source'], string> = {
  kubernetes: 'Kubernetes',
  'flink-rest': 'Flink REST',
  kafka: 'Kafka',
  storage: 'MinIO',
  metrics: 'Metrics',
};

export function HealthBar({ health, wsConnected }: { health: SignalHealth[]; wsConnected: boolean }) {
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-slate-800 bg-slate-900/40 px-3 py-1.5 text-xs">
      <div className="flex items-center gap-1.5" title={wsConnected ? 'Live stream connected' : 'Reconnecting…'}>
        <span className={`h-2 w-2 rounded-full ${wsConnected ? 'bg-emerald-500' : 'bg-amber-500 animate-pulse'}`} />
        <span className="text-slate-400">stream</span>
      </div>
      {health.map((h) => (
        <div key={h.source} className="flex items-center gap-1.5" title={h.reason ?? h.status}>
          <span className={`h-2 w-2 rounded-full ${STATUS_DOT[h.status]}`} />
          <span className="text-slate-400">{SOURCE_LABEL[h.source]}</span>
          {h.status === 'down' && <span className="text-rose-400">unavailable</span>}
        </div>
      ))}
    </div>
  );
}
