import type { AppConfig } from '../config.js';
import type { HealthTracker } from './health.js';
import type { PreviewTopic, TopicPreviewRecord } from '@flink-console/shared';
import { kubectl } from './exec.js';

/**
 * Reads Kafka signals by exec-ing into the broker pod, exactly as
 * `scripts/common.sh` does. Bounded topic reads use `kafka-console-consumer.sh`
 * with `--max-messages` and a timeout so they never disturb the running job's
 * consumer group or its committed offsets, and consumer-group lag mirrors
 * `get_kafka_consumer_group_lag`.
 */
export class KafkaAccess {
  /** Last observed sum of end offsets per topic, for produce-rate estimation. */
  private readonly lastEndOffsets = new Map<PreviewTopic, { sum: number; at: number }>();

  constructor(
    private readonly config: AppConfig,
    private readonly health: HealthTracker,
  ) {}

  private async brokerPod(): Promise<string | undefined> {
    const res = await kubectl([
      '-n',
      this.config.namespaces.kafka,
      'get',
      'pods',
      '-l',
      'app=kafka',
      '-o',
      'jsonpath={.items[0].metadata.name}',
    ]);
    const name = res.stdout.trim();
    return name.length > 0 ? name : undefined;
  }

  async readCommittedLag(): Promise<number | undefined> {
    if (!this.health.shouldAttempt('kafka')) return undefined;
    try {
      const pod = await this.brokerPod();
      if (!pod) {
        this.health.markDown('kafka', 'kafka broker pod not found');
        return undefined;
      }
      const bootstrap = `kafka.${this.config.namespaces.kafka}.svc.cluster.local:9092`;
      // The broker's own KAFKA_HEAP_OPTS is exported inside its entrypoint shell,
      // which `kubectl exec` sessions do not inherit — without an explicit cap here
      // this JVM would default to a much larger heap stacked on top of the broker's
      // own memory inside the same container, which can OOM-kill the broker.
      const script =
        `export KAFKA_HEAP_OPTS="-Xmx128m -Xms64m"; ` +
        `/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server ${bootstrap} ` +
        `--group ${this.config.flink.consumerGroup} --describe 2>/dev/null | ` +
        `awk 'NR>1 {sum+=$6} END {print (sum==""?0:sum)}'`;
      const res = await kubectl(
        ['-n', this.config.namespaces.kafka, 'exec', pod, '--', 'bash', '-lc', script],
        { timeoutMs: 15_000 },
      );
      const lag = Number.parseInt(res.stdout.trim(), 10);
      if (!Number.isFinite(lag)) {
        this.health.markDown('kafka', 'could not parse consumer-group lag');
        return undefined;
      }
      this.health.markOk('kafka');
      return lag;
    } catch (err) {
      this.health.markDown('kafka', errMessage(err));
      return undefined;
    }
  }

  async previewTopic(
    topic: PreviewTopic,
    maxRecords: number,
  ): Promise<{ records: TopicPreviewRecord[]; observedRate?: number } | undefined> {
    if (!this.health.shouldAttempt('kafka')) return undefined;
    try {
      const pod = await this.brokerPod();
      if (!pod) {
        this.health.markDown('kafka', 'kafka broker pod not found');
        return undefined;
      }
      const bootstrap = `kafka.${this.config.namespaces.kafka}.svc.cluster.local:9092`;
      // Bounded, read-only tail with NO group id: console-consumer then uses a
      // random group with auto-commit disabled, so no offsets are ever committed
      // (the Flink group is untouched) and the transient group evaporates when the
      // consumer exits instead of accumulating metadata on the broker. A fixed
      // group id is deliberately avoided — a poll killed mid-read leaves a dead
      // member whose 45s session timeout stalls the next poll's rebalance.
      // KAFKA_HEAP_OPTS is capped for the same reason as the lag exec above — this
      // JVM does not inherit the broker's own heap setting and must not stack an
      // uncapped heap on top of it inside the same memory-limited container.
      // --timeout-ms must absorb JVM cold-start plus partition assignment on the
      // CPU-throttled broker (~3-5s before the first record) or every read exits
      // empty; 8s is comfortable while still bounding the read.
      // kafka-get-offsets.sh runs first (same serialized exec, so still at most
      // one poll in flight) so the observed rate can be derived from end-offset
      // deltas between polls — a batch of consecutive records can only measure
      // the producer's burst rate, not the topic's actual throughput.
      const script =
        `export KAFKA_HEAP_OPTS="-Xmx128m -Xms64m"; ` +
        `/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${bootstrap} ` +
        `--topic ${topic} --time -1 2>/dev/null | ` +
        `awk -F: '{sum+=$NF} END {print "ENDOFFSETSUM=" (sum==""?0:sum)}'; ` +
        `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${bootstrap} ` +
        `--topic ${topic} --max-messages ${maxRecords} --timeout-ms 8000 ` +
        `2>/dev/null || true`;
      const res = await kubectl(
        ['-n', this.config.namespaces.kafka, 'exec', pod, '--', 'bash', '-lc', script],
        { timeoutMs: 30_000 },
      );
      const lines = res.stdout
        .split('\n')
        .map((l) => l.trim())
        .filter((l) => l.length > 0);

      let observedRate: number | undefined;
      const offsetLine = lines.find((l) => l.startsWith('ENDOFFSETSUM='));
      if (offsetLine) {
        const sum = Number.parseInt(offsetLine.slice('ENDOFFSETSUM='.length), 10);
        if (Number.isFinite(sum)) {
          const now = Date.now();
          const prev = this.lastEndOffsets.get(topic);
          this.lastEndOffsets.set(topic, { sum, at: now });
          if (prev && now > prev.at && sum >= prev.sum) {
            observedRate = Math.round((sum - prev.sum) / ((now - prev.at) / 1000));
          }
        }
      }

      const records: TopicPreviewRecord[] = lines
        .filter((l) => !l.startsWith('ENDOFFSETSUM='))
        .slice(-maxRecords)
        .map((raw) => {
          let parsed: TopicPreviewRecord['parsed'];
          try {
            parsed = JSON.parse(raw) as Record<string, unknown>;
          } catch {
            parsed = undefined;
          }
          return { raw, parsed };
        });

      this.health.markOk('kafka');
      return { records, observedRate };
    } catch (err) {
      this.health.markDown('kafka', errMessage(err));
      return undefined;
    }
  }
}

function errMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
