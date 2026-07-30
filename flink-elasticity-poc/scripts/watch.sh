#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

while true; do
  clear
  printf 'Time: %s\n' "$(date -u +%FT%TZ)"
  printf 'TaskManager pods: %s\n' "$(get_tm_count)"

  printf 'Flink parallelism summary:\n'
  printf '  state: %s\n' "$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || echo 'unknown')"
  printf '  vertices: %s\n' "$(get_vertex_parallelism)"

  lag="$(get_kafka_consumer_group_lag)"
  printf 'Kafka consumer lag (group flink-elastic-consumer): %s\n' "${lag}"

  printf 'Latest autoscaler event:\n'
  kubectl -n flink-jobs describe flinkdeployment flink-elastic-job 2>/dev/null | awk '/Events:/,0' | tail -n 5 || true

  printf '\nHint: run scripts/ui.sh in another terminal for the Flink Web UI\n'
  sleep 10
done
