#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

echo "S5 - Resume from savepoint"
echo "Expected: job resumes, first checkpoint succeeds, buffered lag drains."

echo "Keeping load generator active at 300 events/s during resume"
"${ROOT_DIR}/scripts/set-load.sh" 300

start_ts="$(date +%s)"
kubectl -n flink-jobs patch flinkdeployment flink-elastic-job --type merge -p '{"spec":{"job":{"state":"running"}}}'

echo "Waiting for first successful checkpoint"
first_checkpoint_at=""
deadline=$(( start_ts + 1200 ))
while (( $(date +%s) < deadline )); do
  ck_age="$(get_last_checkpoint_age_seconds)"
  if [[ "${ck_age}" != "-1" ]]; then
    first_checkpoint_at="$(date +%s)"
    break
  fi
  sleep 15
done

[[ -n "${first_checkpoint_at}" ]] || { echo "FAIL S5 resume checkpoint did not appear"; exit 1; }

# Drain is judged on the source operator's pendingRecords (true unread backlog),
# not consumer-group lag: committed offsets only advance on the 30s checkpoint
# cadence, so at 300 ev/s the reported lag statistically never falls below 200
# even when the pipeline is fully caught up.
echo "Waiting for backlog (pendingRecords) to drain"
drained_at=""
while (( $(date +%s) < deadline )); do
  pending="$(get_source_pending_records)"
  echo "poll pendingRecords=${pending}"
  if [[ "${pending}" != "-1" ]] && (( pending < 200 )); then
    drained_at="$(date +%s)"
    break
  fi
  sleep 15
done

[[ -n "${drained_at}" ]] || { echo "FAIL S5 resume backlog did not drain"; exit 1; }

lag="$(get_kafka_consumer_group_lag)"
echo "info committed consumer-group lag=${lag} (checkpoint-cadence distorted, informational only)"

cold_start_s=$(( first_checkpoint_at - start_ts ))
drain_s=$(( drained_at - start_ts ))
echo "PASS S5 resume cold_start_to_checkpoint_s=${cold_start_s} backlog_drain_s=${drain_s} pending_records=${pending}"
