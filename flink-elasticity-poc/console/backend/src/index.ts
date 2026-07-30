import { loadConfig } from './config.js';
import { buildServer } from './server.js';

async function main(): Promise<void> {
  const config = loadConfig(process.argv.slice(2));
  const { app, aggregator } = await buildServer(config);

  aggregator.start();

  const shutdown = async (signal: string) => {
    app.log.info(`Received ${signal}, shutting down`);
    aggregator.stop();
    try {
      await app.close();
    } finally {
      process.exit(0);
    }
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  try {
    await app.listen({ host: config.host, port: config.port });
    app.log.info(
      `Flink Elasticity Console on http://${config.host}:${config.port} ` +
        `(mode: ${config.operateMode ? 'operate' : 'read-only'})`,
    );
  } catch (err) {
    app.log.error(err);
    aggregator.stop();
    process.exit(1);
  }
}

void main();
