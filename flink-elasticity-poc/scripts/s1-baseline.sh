#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

echo "S1 - Baseline"
echo "Expected: stable TM count, low lag, regular checkpoints."
echo "Observability: scripts/watch.sh"

"${ROOT_DIR}/scripts/set-load.sh" 50

echo "Waiting for baseline stabilization..."
sleep 60

# Baseline health is judged on real-time signals. The Kafka *consumer-group* lag
# is reported for information only: the Flink source commits offsets to the group
# just once per checkpoint, so that number sawtooths up between checkpoints even
# when the pipeline is fully caught up. The authoritative "caught up" signal is
# the source's real-time pendingRecords, which stays ~0 at baseline load.
pass=false
tm_count="-1"; lag="-1"; ck_age="-1"; pending="-1"
for attempt in $(seq 1 8); do
  tm_count="$(get_tm_count)"
  lag="$(get_kafka_consumer_group_lag)"
  ck_age="$(get_last_checkpoint_age_seconds)"
  pending="$(get_source_pending_records)"
  echo "Observed: tm_count=${tm_count} pendingRecords=${pending} lag=${lag} checkpoint_age_s=${ck_age}"

  if (( tm_count >= 1 )) \
     && [[ "${pending}" != "-1" ]] && (( pending < 1000 )) \
     && [[ "${ck_age}" != "-1" ]] && (( ck_age <= 300 )); then
    pass=true
    break
  fi
  sleep 20
done

if [[ "${pass}" == "true" ]]; then
  echo "PASS S1 baseline tm_count=${tm_count} pendingRecords=${pending} lag=${lag} checkpoint_age_s=${ck_age}"
else
  echo "FAIL S1 baseline tm_count=${tm_count} pendingRecords=${pending} lag=${lag} checkpoint_age_s=${ck_age}"
  exit 1
fi
