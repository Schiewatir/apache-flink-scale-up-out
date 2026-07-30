import * as k8s from '@kubernetes/client-node';
import type { AppConfig } from '../config.js';
import type { HealthTracker } from './health.js';

export interface KubeState {
  taskManagerCount: number;
  jobState?: string;
  taskManagerCpu?: number;
  taskManagerMemory?: string;
  lastAutoscalerEvent?: string;
}

/**
 * Reads structured Kubernetes signals (TaskManager pod count, FlinkDeployment
 * job state, and the most recent autoscaler event) using the current kube
 * context. FlinkDeployment is a custom resource, so it is read via the generic
 * custom-objects API.
 */
export class KubernetesAccess {
  private readonly kc: k8s.KubeConfig;
  private readonly core: k8s.CoreV1Api;
  private readonly custom: k8s.CustomObjectsApi;

  constructor(
    private readonly config: AppConfig,
    private readonly health: HealthTracker,
  ) {
    this.kc = new k8s.KubeConfig();
    this.kc.loadFromDefault();
    this.core = this.kc.makeApiClient(k8s.CoreV1Api);
    this.custom = this.kc.makeApiClient(k8s.CustomObjectsApi);
  }

  async read(): Promise<KubeState | undefined> {
    if (!this.health.shouldAttempt('kubernetes')) {
      return undefined;
    }
    try {
      const ns = this.config.namespaces.flink;
      const tmPods = await this.core.listNamespacedPod(
        ns,
        undefined,
        undefined,
        undefined,
        undefined,
        'component=taskmanager',
      );
      const taskManagerCount = tmPods.body.items.length;

      let jobState: string | undefined;
      let taskManagerCpu: number | undefined;
      let taskManagerMemory: string | undefined;
      try {
        const dep = (await this.custom.getNamespacedCustomObject(
          'flink.apache.org',
          'v1beta1',
          ns,
          'flinkdeployments',
          this.config.flink.deploymentName,
        )) as {
          body?: {
            spec?: { taskManager?: { resource?: { cpu?: number; memory?: string } } };
            status?: { jobStatus?: { state?: string } };
          };
        };
        jobState = dep.body?.status?.jobStatus?.state;
        taskManagerCpu = dep.body?.spec?.taskManager?.resource?.cpu;
        taskManagerMemory = dep.body?.spec?.taskManager?.resource?.memory;
      } catch {
        jobState = undefined;
      }

      const lastAutoscalerEvent = await this.readLastEvent(ns);

      this.health.markOk('kubernetes');
      return { taskManagerCount, jobState, taskManagerCpu, taskManagerMemory, lastAutoscalerEvent };
    } catch (err) {
      this.health.markDown('kubernetes', errMessage(err));
      return undefined;
    }
  }

  private async readLastEvent(namespace: string): Promise<string | undefined> {
    try {
      const events = await this.core.listNamespacedEvent(namespace);
      const related = events.body.items
        .filter((e) => e.involvedObject?.name === this.config.flink.deploymentName)
        .sort((a, b) => eventTime(a) - eventTime(b));
      const latest = related[related.length - 1];
      if (!latest) return undefined;
      return `${latest.reason ?? ''}: ${latest.message ?? ''}`.trim();
    } catch {
      return undefined;
    }
  }
}

function eventTime(e: k8s.CoreV1Event): number {
  const ts = e.lastTimestamp ?? e.eventTime ?? e.metadata?.creationTimestamp;
  return ts ? new Date(ts as unknown as string).getTime() : 0;
}

function errMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
