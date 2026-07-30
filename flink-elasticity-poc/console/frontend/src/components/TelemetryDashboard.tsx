import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { ClusterSnapshot, MetricSample } from '@flink-console/shared';

function fmtTime(t: number): string {
  return new Date(t).toLocaleTimeString();
}

function StatTile({
  label,
  value,
  sub,
  emphasize,
}: {
  label: string;
  value: string;
  sub?: string;
  emphasize?: boolean;
}) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 px-3 py-2">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`text-lg font-semibold ${emphasize ? 'text-emerald-400' : 'text-slate-100'}`}>
        {value}
      </div>
      {sub && <div className="text-[11px] text-slate-500">{sub}</div>}
    </div>
  );
}

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h3 className="mb-2 text-sm font-semibold text-slate-200">{title}</h3>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          {children as React.ReactElement}
        </ResponsiveContainer>
      </div>
    </div>
  );
}

const axisStyle = { fontSize: 10, fill: '#94a3b8' };

// Categorical series colors (validated dark-mode palette, fixed slot order):
// slot 1 blue = TaskManagers, slots 2-4 = vertex series by sorted name.
const TM_COLOR = '#3987e5';
const VERTEX_COLORS = ['#d95926', '#199e70', '#c98500'];
const BUSY_COLOR = '#d55181';

type ChartRow = MetricSample & Record<string, unknown>;

/** Flatten the per-vertex parallelism map and busy ratio into chartable fields. */
function flattenTimeline(timeline: MetricSample[]): {
  rows: ChartRow[];
  vertexKeys: string[];
} {
  const keys = new Set<string>();
  for (const s of timeline) {
    for (const k of Object.keys(s.vertexParallelism ?? {})) keys.add(k);
  }
  const vertexKeys = [...keys].sort();
  const rows = timeline.map((s) => {
    const row: ChartRow = { ...s };
    for (const k of vertexKeys) row[`vp_${k}`] = s.vertexParallelism?.[k];
    row.busyPct = s.maxVertexBusy !== undefined ? Math.round(s.maxVertexBusy * 100) : undefined;
    return row;
  });
  return { rows, vertexKeys };
}

export function TelemetryDashboard({
  timeline,
  snapshot,
}: {
  timeline: MetricSample[];
  snapshot?: ClusterSnapshot;
}) {
  const { rows, vertexKeys } = flattenTimeline(timeline);
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-8">
        <StatTile label="Job state" value={snapshot?.jobState ?? '—'} />
        <StatTile label="TaskManagers" value={String(snapshot?.taskManagerCount ?? '—')} />
        <StatTile label="Max parallelism" value={String(snapshot?.maxParallelism ?? '—')} />
        <StatTile
          label="TM size"
          value={
            snapshot?.taskManagerCpu !== undefined && snapshot?.taskManagerMemory
              ? `${snapshot.taskManagerCpu} cpu · ${snapshot.taskManagerMemory}`
              : '—'
          }
          sub="per TaskManager"
        />
        <StatTile
          label="pendingRecords"
          value={String(snapshot?.pendingRecords ?? '—')}
          sub="authoritative backlog"
          emphasize
        />
        <StatTile
          label="Consumer lag"
          value={String(snapshot?.committedLag ?? '—')}
          sub="informational only"
        />
        <StatTile
          label="Checkpoint age"
          value={
            snapshot?.lastCheckpointAgeSeconds !== undefined
              ? `${snapshot.lastCheckpointAgeSeconds}s`
              : '—'
          }
        />
        <StatTile label="Checkpoints" value={String(snapshot?.checkpointCount ?? '—')} />
        <StatTile label="Savepoints" value={String(snapshot?.savepointCount ?? '—')} />
      </div>

      {snapshot?.lastAutoscalerEvent && (
        <div className="rounded-md border border-indigo-800 bg-indigo-950/40 px-3 py-1.5 text-xs text-indigo-300">
          Latest autoscaler event: {snapshot.lastAutoscalerEvent}
        </div>
      )}

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-3">
        <ChartCard title="TaskManagers & per-vertex parallelism">
          <LineChart data={rows}>
            <CartesianGrid stroke="#1e293b" />
            <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
            <YAxis tick={axisStyle} allowDecimals={false} />
            <Tooltip
              labelFormatter={fmtTime}
              contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Line
              type="monotone"
              dataKey="taskManagerCount"
              name="TaskManagers"
              stroke={TM_COLOR}
              strokeWidth={2}
              dot={false}
            />
            {vertexKeys.map((k, i) => (
              <Line
                key={k}
                type="stepAfter"
                dataKey={`vp_${k}`}
                name={`p: ${k}`}
                stroke={VERTEX_COLORS[i % VERTEX_COLORS.length]}
                dot={false}
              />
            ))}
          </LineChart>
        </ChartCard>

        <ChartCard title="Busiest vertex busy (%)">
          <LineChart data={rows}>
            <CartesianGrid stroke="#1e293b" />
            <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
            <YAxis tick={axisStyle} domain={[0, 100]} />
            <Tooltip
              labelFormatter={fmtTime}
              contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            />
            <Line
              type="monotone"
              dataKey="busyPct"
              name="busy % (S7 signal)"
              stroke={BUSY_COLOR}
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ChartCard>

        <ChartCard title="Backlog: pendingRecords vs. lag">
          <LineChart data={rows}>
            <CartesianGrid stroke="#1e293b" />
            <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
            <YAxis tick={axisStyle} />
            <Tooltip
              labelFormatter={fmtTime}
              contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Line
              type="monotone"
              dataKey="pendingRecords"
              name="pendingRecords (authoritative)"
              stroke="#34d399"
              strokeWidth={2}
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="committedLag"
              name="consumer lag (informational)"
              stroke="#64748b"
              strokeDasharray="4 4"
              dot={false}
            />
          </LineChart>
        </ChartCard>

        <ChartCard title="Checkpoint age & load rate">
          <LineChart data={rows}>
            <CartesianGrid stroke="#1e293b" />
            <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
            <YAxis tick={axisStyle} />
            <Tooltip
              labelFormatter={fmtTime}
              contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Line
              type="monotone"
              dataKey="lastCheckpointAgeSeconds"
              name="checkpoint age (s)"
              stroke="#fbbf24"
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="loadRate"
              name="load rate (ev/s)"
              stroke="#f472b6"
              dot={false}
            />
          </LineChart>
        </ChartCard>
      </div>
    </div>
  );
}
