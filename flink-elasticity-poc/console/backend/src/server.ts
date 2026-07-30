import Fastify, { type FastifyInstance } from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import fastifyStatic from '@fastify/static';
import fs from 'node:fs';
import type {
  LoadControlResponse,
  PreviewTopic,
  ScenarioId,
  ScenarioRunResponse,
  ServerInfo,
} from '@flink-console/shared';
import { SCENARIOS } from '@flink-console/shared';
import type { AppConfig } from './config.js';
import { StreamHub } from './streaming/hub.js';
import { Aggregator } from './services/aggregator.js';
import { ScenarioRunner } from './services/scenarioRunner.js';
import { LoadControl } from './services/loadControl.js';

const VALID_SCENARIO_IDS = new Set<ScenarioId>(SCENARIOS.map((s) => s.id));
const VALID_PREVIEW_TOPICS = new Set(['events-in', 'events-out']);

export async function buildServer(config: AppConfig): Promise<{
  app: FastifyInstance;
  aggregator: Aggregator;
}> {
  const app = Fastify({ logger: { level: 'info' } });
  const hub = new StreamHub();
  const aggregator = new Aggregator(config, hub);
  const scenarios = new ScenarioRunner(config, hub);
  const loadControl = new LoadControl(config);

  // Scenario scripts run their own lag polls inside the broker pod at peak load;
  // the console's broker execs pause for the duration so they never stack.
  aggregator.setKafkaPollSuspensionProbe(() => scenarios.isRunning());

  await app.register(fastifyWebsocket);

  const serverInfo: ServerInfo = {
    operateMode: config.operateMode,
    scenarios: SCENARIOS,
    version: config.version,
  };

  // Guard applied to every mutating endpoint: rejects when not in operate mode.
  const requireOperate = (): { code: number; message: string } | undefined => {
    if (!config.operateMode) {
      return {
        code: 403,
        message: 'Console is in read-only mode. Start with --operate to enable mutating actions.',
      };
    }
    return undefined;
  };

  // ---- Read endpoints -----------------------------------------------------

  app.get('/api/info', async () => serverInfo);

  app.get('/api/snapshot', async () => aggregator.getSnapshot());

  app.get('/api/topology', async () => aggregator.getTopology());

  app.get('/api/timeline', async () => aggregator.getTimeline());

  app.get('/api/health', async () => aggregator.health.snapshot());

  app.get('/api/scenarios', async () => scenarios.getStates());

  app.get('/api/preview/:topic', async (request, reply) => {
    const { topic } = request.params as { topic: string };
    if (!VALID_PREVIEW_TOPICS.has(topic)) {
      return reply.code(400).send({ ok: false, message: 'Unknown topic' });
    }
    const batch = aggregator.getPreview(topic as PreviewTopic);
    return batch ?? { topic, records: [], generatedAt: Date.now() };
  });

  // ---- Mutating endpoints (operate-mode + allow-list) ---------------------

  app.post('/api/scenarios/:id/run', async (request, reply) => {
    const denied = requireOperate();
    if (denied) return reply.code(denied.code).send({ ok: false, message: denied.message });

    const { id } = request.params as { id: string };
    if (!VALID_SCENARIO_IDS.has(id as ScenarioId)) {
      return reply.code(400).send({ ok: false, id, message: 'Unknown scenario id' });
    }
    const scenarioId = id as ScenarioId;
    const error = scenarios.run(scenarioId);
    const response: ScenarioRunResponse = error
      ? { ok: false, id: scenarioId, message: error }
      : { ok: true, id: scenarioId, message: `Started ${scenarioId}` };
    return reply.code(error ? 409 : 202).send(response);
  });

  app.post('/api/load', async (request, reply) => {
    const denied = requireOperate();
    if (denied) return reply.code(denied.code).send({ ok: false, message: denied.message });

    const body = request.body as { rate?: unknown };
    const rate = loadControl.validate(body?.rate);
    if (rate === undefined) {
      const response: LoadControlResponse = {
        ok: false,
        rate: 0,
        message: 'Rate must be a positive integer',
      };
      return reply.code(400).send(response);
    }
    const result = await loadControl.setRate(rate);
    return reply.code(result.ok ? 200 : 500).send(result);
  });

  // ---- Streaming ----------------------------------------------------------

  app.register(async (scoped) => {
    scoped.get('/api/stream', { websocket: true }, (socket) => {
      hub.add(socket);
      socket.send(JSON.stringify({ type: 'serverInfo', payload: serverInfo }));
    });
  });

  // ---- Static frontend ----------------------------------------------------

  if (fs.existsSync(config.frontendDir)) {
    await app.register(fastifyStatic, { root: config.frontendDir, prefix: '/' });
    app.setNotFoundHandler((request, reply) => {
      if (request.raw.url?.startsWith('/api')) {
        return reply.code(404).send({ ok: false, message: 'Not found' });
      }
      return reply.sendFile('index.html');
    });
  } else {
    app.get('/', async () => ({
      ok: true,
      message: 'Frontend bundle not built. Run `npm run build` in console/.',
    }));
  }

  return { app, aggregator };
}
