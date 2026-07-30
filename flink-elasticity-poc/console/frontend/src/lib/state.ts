import type {
  ClusterSnapshot,
  MetricSample,
  PreviewTopic,
  ScenarioId,
  ScenarioLogLine,
  ScenarioState,
  ServerInfo,
  SignalHealth,
  StreamMessage,
  TopicPreviewBatch,
  TopologyDocument,
} from '@flink-console/shared';

export interface AppState {
  serverInfo?: ServerInfo;
  snapshot?: ClusterSnapshot;
  topology?: TopologyDocument;
  timeline: MetricSample[];
  previews: Partial<Record<PreviewTopic, TopicPreviewBatch>>;
  scenarios: Partial<Record<ScenarioId, ScenarioState>>;
  scenarioLogs: Partial<Record<ScenarioId, ScenarioLogLine[]>>;
  health: SignalHealth[];
  wsConnected: boolean;
}

export const initialAppState: AppState = {
  timeline: [],
  previews: {},
  scenarios: {},
  scenarioLogs: {},
  health: [],
  wsConnected: false,
};

const MAX_TIMELINE_SAMPLES = 240;
const MAX_LOG_LINES = 500;

export type AppAction =
  | { kind: 'stream'; message: StreamMessage }
  | {
      kind: 'bootstrap';
      info?: ServerInfo;
      snapshot?: ClusterSnapshot;
      topology?: TopologyDocument;
      timeline?: MetricSample[];
      scenarios?: ScenarioState[];
      health?: SignalHealth[];
      previews?: TopicPreviewBatch[];
    }
  | { kind: 'wsStatus'; connected: boolean };

export function appReducer(state: AppState, action: AppAction): AppState {
  switch (action.kind) {
    case 'wsStatus':
      return { ...state, wsConnected: action.connected };

    case 'bootstrap': {
      const previews = { ...state.previews };
      for (const batch of action.previews ?? []) previews[batch.topic] = batch;
      const scenarios = { ...state.scenarios };
      for (const s of action.scenarios ?? []) scenarios[s.id] = s;
      return {
        ...state,
        serverInfo: action.info ?? state.serverInfo,
        snapshot: action.snapshot ?? state.snapshot,
        topology: action.topology ?? state.topology,
        timeline: action.timeline ?? state.timeline,
        scenarios,
        previews,
        health: action.health ?? state.health,
      };
    }

    case 'stream':
      return applyStreamMessage(state, action.message);

    default:
      return state;
  }
}

function applyStreamMessage(state: AppState, message: StreamMessage): AppState {
  switch (message.type) {
    case 'serverInfo':
      return { ...state, serverInfo: message.payload };

    case 'snapshot':
      return { ...state, snapshot: message.payload };

    case 'topology':
      return { ...state, topology: message.payload };

    case 'metricSample': {
      const timeline = [...state.timeline, message.payload];
      if (timeline.length > MAX_TIMELINE_SAMPLES) timeline.shift();
      return { ...state, timeline };
    }

    case 'topicPreview':
      return {
        ...state,
        previews: { ...state.previews, [message.payload.topic]: message.payload },
      };

    case 'signalHealth':
      return { ...state, health: message.payload };

    case 'scenarioState': {
      const scenarios = { ...state.scenarios, [message.payload.id]: message.payload };
      const scenarioLogs = { ...state.scenarioLogs };
      // A fresh run start clears the previous run's log.
      if (message.payload.status === 'running') {
        scenarioLogs[message.payload.id] = [];
      }
      return { ...state, scenarios, scenarioLogs };
    }

    case 'scenarioLog': {
      const id = message.payload.id;
      const existing = state.scenarioLogs[id] ?? [];
      const lines = [...existing, message.payload];
      if (lines.length > MAX_LOG_LINES) lines.shift();
      return { ...state, scenarioLogs: { ...state.scenarioLogs, [id]: lines } };
    }

    default:
      return state;
  }
}
