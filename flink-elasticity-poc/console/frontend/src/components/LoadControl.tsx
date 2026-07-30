import { useState } from 'react';
import { ConfirmDialog } from './ConfirmDialog.js';

export function LoadControl({
  operateMode,
  currentRate,
  onSetRate,
}: {
  operateMode: boolean;
  currentRate?: number;
  onSetRate: (rate: number) => Promise<{ ok: boolean; message: string }>;
}) {
  const [input, setInput] = useState('');
  const [pendingRate, setPendingRate] = useState<number | undefined>();
  const [error, setError] = useState<string | undefined>();
  const [status, setStatus] = useState<string | undefined>();
  const [submitting, setSubmitting] = useState(false);

  const validate = (raw: string): number | undefined => {
    if (!/^\d+$/.test(raw.trim())) return undefined;
    const n = Number.parseInt(raw, 10);
    return n > 0 ? n : undefined;
  };

  const submit = () => {
    const rate = validate(input);
    if (rate === undefined) {
      setError('Rate must be a positive integer');
      return;
    }
    setError(undefined);
    setPendingRate(rate);
  };

  const confirmApply = async () => {
    const rate = pendingRate;
    setPendingRate(undefined);
    if (rate === undefined) return;
    setSubmitting(true);
    setStatus(undefined);
    const result = await onSetRate(rate);
    setSubmitting(false);
    setStatus(result.message);
    if (!result.ok) setError(result.message);
  };

  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h3 className="mb-2 text-sm font-semibold text-slate-200">Load control</h3>
      <div className="mb-2 text-xs text-slate-400">
        Current target rate: <span className="text-slate-200">{currentRate ?? '—'} events/s</span>
      </div>
      <div className="flex gap-2">
        <input
          type="text"
          inputMode="numeric"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          disabled={!operateMode || submitting}
          placeholder="events/s"
          className="w-32 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100 disabled:opacity-50"
        />
        <button
          type="button"
          onClick={submit}
          disabled={!operateMode || submitting}
          className="rounded-md bg-sky-700 px-3 py-1 text-xs font-medium text-white enabled:hover:bg-sky-600 disabled:cursor-not-allowed disabled:bg-slate-800 disabled:text-slate-500"
        >
          Apply
        </button>
      </div>
      {!operateMode && (
        <p className="mt-2 text-xs text-slate-500">
          Console is in read-only mode — start it with <code>--operate</code> to change load.
        </p>
      )}
      {error && <p className="mt-2 text-xs text-rose-400">{error}</p>}
      {status && !error && <p className="mt-2 text-xs text-emerald-400">{status}</p>}

      {pendingRate !== undefined && (
        <ConfirmDialog
          title="Change load generator rate"
          message={`Set the load generator to ${pendingRate} events/s?`}
          confirmLabel="Apply rate"
          onCancel={() => setPendingRate(undefined)}
          onConfirm={() => void confirmApply()}
        />
      )}
    </div>
  );
}
