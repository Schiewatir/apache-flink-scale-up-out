import { useEffect, useState } from 'react';
import type {
  ScenarioDescriptor,
  ScenarioId,
  ScenarioLogLine,
  ScenarioState,
} from '@flink-console/shared';
import { ConfirmDialog } from './ConfirmDialog.js';

const STATUS_STYLES: Record<ScenarioState['status'], string> = {
  idle: 'bg-slate-800 text-slate-400',
  running: 'bg-sky-900 text-sky-300',
  pass: 'bg-emerald-900 text-emerald-300',
  fail: 'bg-rose-900 text-rose-300',
  error: 'bg-rose-900 text-rose-300',
};

export function ScenarioPanel({
  scenarios,
  states,
  logs,
  operateMode,
  onRun,
}: {
  scenarios: ScenarioDescriptor[];
  states: Partial<Record<ScenarioId, ScenarioState>>;
  logs: Partial<Record<ScenarioId, ScenarioLogLine[]>>;
  operateMode: boolean;
  onRun: (id: ScenarioId) => Promise<{ ok: boolean; message: string }>;
}) {
  const [selected, setSelected] = useState<ScenarioId>(scenarios[0]?.id ?? 's1');
  const [pending, setPending] = useState<ScenarioDescriptor | undefined>();
  const [runError, setRunError] = useState<string | undefined>();

  const anyRunning = Object.values(states).some((s) => s?.status === 'running');

  // Follow whichever scenario starts running so the log view tracks it live.
  useEffect(() => {
    const running = scenarios.find((s) => states[s.id]?.status === 'running');
    if (running) setSelected(running.id);
  }, [states, scenarios]);

  const startRun = async (descriptor: ScenarioDescriptor) => {
    setRunError(undefined);
    setSelected(descriptor.id);
    const result = await onRun(descriptor.id);
    if (!result.ok) setRunError(result.message);
  };

  const handleRunClick = (descriptor: ScenarioDescriptor) => {
    if (descriptor.mutating) {
      setPending(descriptor);
    } else {
      void startRun(descriptor);
    }
  };

  const selectedLogs = logs[selected] ?? [];
  const selectedState = states[selected];

  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
      <div className="space-y-2">
        {scenarios.map((s) => {
          const state = states[s.id];
          const disabled = !operateMode || anyRunning;
          return (
            <div
              key={s.id}
              className={`cursor-pointer rounded-lg border px-3 py-2 ${
                selected === s.id ? 'border-sky-700 bg-slate-900/70' : 'border-slate-800 bg-slate-900/30'
              }`}
              onClick={() => setSelected(s.id)}
            >
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold text-slate-100">
                  {s.id.toUpperCase()} · {s.name}
                  {s.mutating && (
                    <span className="ml-2 rounded bg-amber-950 px-1.5 py-0.5 text-[10px] text-amber-400">
                      mutating
                    </span>
                  )}
                </div>
                <span className={`rounded px-2 py-0.5 text-[10px] ${STATUS_STYLES[state?.status ?? 'idle']}`}>
                  {state?.status ?? 'idle'}
                </span>
              </div>
              <p className="mt-1 text-xs text-slate-400">{s.description}</p>
              <button
                type="button"
                disabled={disabled}
                onClick={(e) => {
                  e.stopPropagation();
                  handleRunClick(s);
                }}
                className="mt-2 rounded-md bg-sky-700 px-3 py-1 text-xs font-medium text-white enabled:hover:bg-sky-600 disabled:cursor-not-allowed disabled:bg-slate-800 disabled:text-slate-500"
                title={!operateMode ? 'Enable operate mode to run scenarios' : undefined}
              >
                Run {s.id.toUpperCase()}
              </button>
            </div>
          );
        })}
        {!operateMode && (
          <p className="text-xs text-slate-500">
            Console is in read-only mode — start it with <code>--operate</code> to run scenarios.
          </p>
        )}
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-950 p-3">
        <div className="mb-2 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-slate-200">{selected.toUpperCase()} log</h3>
          {selectedState?.status && (
            <span className={`rounded px-2 py-0.5 text-[10px] ${STATUS_STYLES[selectedState.status]}`}>
              {selectedState.status}
            </span>
          )}
        </div>
        <div className="h-56 overflow-y-auto rounded bg-black/40 p-2 font-mono text-[11px] text-slate-300">
          {selectedLogs.length === 0 ? (
            <div className="text-slate-600">No output yet.</div>
          ) : (
            selectedLogs.map((line, i) => <div key={i}>{line.line}</div>)
          )}
        </div>
        {selectedState?.result && (
          <div className="mt-2 rounded border border-slate-800 p-2 text-xs">
            <div className="font-medium text-slate-200">{selectedState.result.summaryLine}</div>
            <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-0.5 text-slate-400">
              {Object.entries(selectedState.result.metrics).map(([k, v]) => (
                <div key={k}>
                  {k}: <span className="text-slate-200">{v}</span>
                </div>
              ))}
            </div>
          </div>
        )}
        {runError && <div className="mt-2 text-xs text-rose-400">{runError}</div>}
      </div>

      {pending && (
        <ConfirmDialog
          title={`Run ${pending.id.toUpperCase()} — ${pending.name}`}
          message={`This scenario mutates the cluster: ${pending.description}`}
          confirmLabel="Run scenario"
          onCancel={() => setPending(undefined)}
          onConfirm={() => {
            const descriptor = pending;
            setPending(undefined);
            void startRun(descriptor);
          }}
        />
      )}
    </div>
  );
}
