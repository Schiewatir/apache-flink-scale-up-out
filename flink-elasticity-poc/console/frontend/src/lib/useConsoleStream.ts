import { useEffect, useReducer, useRef } from 'react';
import type { StreamMessage } from '@flink-console/shared';
import { api } from './api.js';
import { appReducer, initialAppState } from './state.js';

const BASE_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 15_000;

/**
 * Owns the single WebSocket connection to the BFF stream and the REST bootstrap
 * fetch, folding both into one app state. Reconnects with exponential backoff so
 * a restarted backend or a transient network blip recovers without a page reload.
 */
export function useConsoleStream() {
  const [state, dispatch] = useReducer(appReducer, initialAppState);
  const backoffRef = useRef(BASE_BACKOFF_MS);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const [info, snapshot, topology, timeline, scenarios, health, inPreview, outPreview] =
        await Promise.allSettled([
          api.info(),
          api.snapshot(),
          api.topology(),
          api.timeline(),
          api.scenarios(),
          api.health(),
          api.preview('events-in'),
          api.preview('events-out'),
        ]);
      if (cancelled) return;
      dispatch({
        kind: 'bootstrap',
        info: unwrap(info),
        snapshot: unwrap(snapshot),
        topology: unwrap(topology),
        timeline: unwrap(timeline),
        scenarios: unwrap(scenarios),
        health: unwrap(health),
        previews: [unwrap(inPreview), unwrap(outPreview)].filter(Boolean) as never,
      });
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let socket: WebSocket | undefined;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let closedByEffect = false;

    const connect = () => {
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      socket = new WebSocket(`${proto}://${window.location.host}/api/stream`);

      socket.addEventListener('open', () => {
        backoffRef.current = BASE_BACKOFF_MS;
        dispatch({ kind: 'wsStatus', connected: true });
      });

      socket.addEventListener('message', (event) => {
        try {
          const message = JSON.parse(event.data as string) as StreamMessage;
          dispatch({ kind: 'stream', message });
        } catch {
          // Ignore malformed frames rather than tearing down the connection.
        }
      });

      const scheduleReconnect = () => {
        dispatch({ kind: 'wsStatus', connected: false });
        if (closedByEffect) return;
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, MAX_BACKOFF_MS);
        reconnectTimer = setTimeout(connect, delay);
      };

      socket.addEventListener('close', scheduleReconnect);
      socket.addEventListener('error', () => socket?.close());
    };

    connect();
    return () => {
      closedByEffect = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  return state;
}

function unwrap<T>(result: PromiseSettledResult<T>): T | undefined {
  return result.status === 'fulfilled' ? result.value : undefined;
}
