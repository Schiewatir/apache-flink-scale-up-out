import type {
  ClusterSnapshot,
  LoadControlResponse,
  MetricSample,
  PreviewTopic,
  ScenarioId,
  ScenarioRunResponse,
  ScenarioState,
  ServerInfo,
  SignalHealth,
  TopicPreviewBatch,
  TopologyDocument,
} from '@flink-console/shared';

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`GET ${path} failed: HTTP ${res.status}`);
  return (await res.json()) as T;
}

async function postJson<T>(path: string, body?: unknown): Promise<{ status: number; data: T }> {
  const res = await fetch(path, {
    method: 'POST',
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = (await res.json()) as T;
  return { status: res.status, data };
}

export const api = {
  info: () => getJson<ServerInfo>('/api/info'),
  snapshot: () => getJson<ClusterSnapshot>('/api/snapshot'),
  topology: () => getJson<TopologyDocument>('/api/topology'),
  timeline: () => getJson<MetricSample[]>('/api/timeline'),
  health: () => getJson<SignalHealth[]>('/api/health'),
  scenarios: () => getJson<ScenarioState[]>('/api/scenarios'),
  preview: (topic: PreviewTopic) => getJson<TopicPreviewBatch>(`/api/preview/${topic}`),
  runScenario: (id: ScenarioId) =>
    postJson<ScenarioRunResponse>(`/api/scenarios/${id}/run`, {}),
  setLoad: (rate: number) => postJson<LoadControlResponse>('/api/load', { rate }),
};
