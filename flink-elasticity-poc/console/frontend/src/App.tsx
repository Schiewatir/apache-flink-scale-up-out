import type { ScenarioId } from '@flink-console/shared';
import { useConsoleStream } from './lib/useConsoleStream.js';
import { api } from './lib/api.js';
import { HealthBar } from './components/HealthBar.js';
import { TopologyCanvas } from './components/TopologyCanvas.js';
import { TopicPreview } from './components/TopicPreview.js';
import { TelemetryDashboard } from './components/TelemetryDashboard.js';
import { ScenarioPanel } from './components/ScenarioPanel.js';
import { LoadControl } from './components/LoadControl.js';

function UnavailableBanner({ reason }: { reason?: string }) {
  if (!reason) return null;
  return (
    <div className="mb-2 rounded-md border border-amber-800 bg-amber-950/40 px-3 py-1.5 text-xs text-amber-300">
      Signal unavailable: {reason}
    </div>
  );
}

export default function App() {
  const state = useConsoleStream();
  const operateMode = state.serverInfo?.operateMode ?? false;
  const scenarios = state.serverInfo?.scenarios ?? [];

  const downReason = (source: string) =>
    state.health.find((h) => h.source === source && h.status === 'down')?.reason;

  const runScenario = async (id: ScenarioId) => {
    const { data } = await api.runScenario(id);
    return { ok: data.ok, message: data.message };
  };

  const setRate = async (rate: number) => {
    const { data } = await api.setLoad(rate);
    return { ok: data.ok, message: data.message };
  };

  return (
    <div className="mx-auto max-w-7xl space-y-4 p-4">
      <header className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-bold text-slate-50">Flink Elasticity Console</h1>
          <p className="text-xs text-slate-500">
            Live data flow, telemetry, and scenario execution for the elasticity PoC.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span
            className={`rounded-full px-3 py-1 text-xs font-medium ${
              operateMode ? 'bg-rose-950 text-rose-300' : 'bg-emerald-950 text-emerald-300'
            }`}
          >
            {operateMode ? 'operate mode' : 'read-only mode'}
          </span>
        </div>
      </header>

      <HealthBar health={state.health} wsConnected={state.wsConnected} />

      <section>
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-400">
          Data-flow topology
        </h2>
        <UnavailableBanner reason={downReason('flink-rest') ?? downReason('kubernetes')} />
        <TopologyCanvas topology={state.topology} />
      </section>

      <section className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        <div>
          <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-400">
            Topic preview
          </h2>
          <UnavailableBanner reason={downReason('kafka')} />
        </div>
        <div className="hidden lg:block" />
        <TopicPreview topic="events-in" batch={state.previews['events-in']} />
        <TopicPreview topic="events-out" batch={state.previews['events-out']} />
      </section>

      <section>
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-400">
          Elasticity telemetry
        </h2>
        <TelemetryDashboard timeline={state.timeline} snapshot={state.snapshot} />
      </section>

      <section>
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-400">
          Scenarios &amp; load control
        </h2>
        <div className="space-y-3">
          <LoadControl operateMode={operateMode} currentRate={state.snapshot?.loadRate} onSetRate={setRate} />
          <ScenarioPanel
            scenarios={scenarios}
            states={state.scenarios}
            logs={state.scenarioLogs}
            operateMode={operateMode}
            onRun={runScenario}
          />
        </div>
      </section>
    </div>
  );
}
