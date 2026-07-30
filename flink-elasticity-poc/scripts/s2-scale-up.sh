#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

echo "S2 - Scale-up"
echo "Expected: lag rises, autoscaler increases parallelism/TM count, lag drains."
echo "Observability: scripts/watch.sh and kubectl get pods -n flink-jobs -w"

# The Flink job is lightweight: a single TaskManager (parallelism 1) sustains
# ~11k events/s at ~100% busy before it becomes the bottleneck. The autoscaler
# only scales up once a vertex crosses its target utilization (0.6), so the load
# must genuinely saturate one TM to trigger scale-up and build a drainable backlog.
# A 3k/s target left the pipeline at ~13% utilization, so no scaling occurred.
node_cpu="$(kubectl get node -o jsonpath='{.items[0].status.capacity.cpu}')"
if (( node_cpu >= 6 )); then
  target_rate=12000
else
  target_rate=6000
fi

base_tm="$(get_tm_count)"
start_ts="$(date +%s)"
"${ROOT_DIR}/scripts/set-load.sh" "${target_rate}"

echo "Raised load to ${target_rate} events/s (base_tm=${base_tm})"
echo "Phase 1: expect backlog to build and the autoscaler to add TaskManagers."

# NOTE on lag metrics: the Flink Kafka source commits offsets to the consumer
# group only on checkpoint (every ~30s), so the *consumer-group* lag stays large
# under continuous high load even when the pipeline keeps up. We therefore use:
#   - consumer-group lag  -> evidence that load was applied (backlog built)
#   - TaskManager count   -> evidence the autoscaler scaled UP
#   - source pendingRecords (real-time, commit-independent) + a return to
#     baseline load -> evidence the backlog actually DRAINS after scale-up.

max_tm="${base_tm}"
seen_backlog=false
scaled_up=false
scaleup_ts=0
deadline=$(( start_ts + 1800 ))

# Phase 1: drive load until the autoscaler adds capacity.
while (( $(date +%s) < deadline )); do
  lag="$(get_kafka_consumer_group_lag)"
  tm="$(get_tm_count)"
  (( tm > max_tm )) && max_tm="${tm}"

  if [[ "${lag}" != "-1" ]] && (( lag > 1000 )); then
    seen_backlog=true
  fi
  if (( max_tm > base_tm )); then
    scaled_up=true
    scaleup_ts="$(date +%s)"
  fi

  # vertices= is observability-only enrichment; the helper degrades to -1 and
  # never affects the pass/fail conditions below.
  echo "phase1 lag=${lag} tm=${tm} max_tm=${max_tm} seen_backlog=${seen_backlog} vertices=$(get_vertex_parallelism)"
  if [[ "${seen_backlog}" == "true" ]] && [[ "${scaled_up}" == "true" ]]; then
    break
  fi
  sleep 15
done

scaleup_elapsed=$(( scaleup_ts - start_ts ))

# Phase 2: drop back to baseline load and confirm the (now larger) cluster drains
# the backlog. Use real-time pendingRecords and the receding consumer-group lag.
echo "Phase 2: reducing load to baseline and waiting for backlog to drain."
"${ROOT_DIR}/scripts/set-load.sh" 50
drain_start="$(date +%s)"
drained=false
while (( $(date +%s) < deadline )); do
  pending="$(get_source_pending_records)"
  lag="$(get_kafka_consumer_group_lag)"
  tm="$(get_tm_count)"
  echo "phase2 pendingRecords=${pending} lag=${lag} tm=${tm} vertices=$(get_vertex_parallelism)"
  # Drained when the real-time backlog is ~empty AND the committed lag has
  # receded to steady-state levels (same threshold family as S1's lag check).
  if [[ "${pending}" != "-1" ]] && (( pending < 1000 )) && [[ "${lag}" != "-1" ]] && (( lag < 300 )); then
    drained=true
    break
  fi
  sleep 15
done

end_ts="$(date +%s)"
drain_elapsed=$(( end_ts - drain_start ))

pass=true
if [[ "${seen_backlog}" != "true" ]]; then
  echo "FAIL: backlog did not build; load may be too low for this VM"
  pass=false
fi
if [[ "${scaled_up}" != "true" ]] || (( max_tm <= base_tm )); then
  echo "FAIL: autoscaler did not add TaskManagers under load"
  pass=false
fi
if [[ "${drained}" != "true" ]]; then
  echo "FAIL: backlog did not drain after returning to baseline load"
  pass=false
fi

if [[ "${pass}" == "true" ]]; then
  echo "PASS S2 scale-up rate=${target_rate} base_tm=${base_tm} max_tm=${max_tm} scaleup_in_s=${scaleup_elapsed} drain_in_s=${drain_elapsed}"
else
  echo "FAIL S2 scale-up"
  exit 1
fi
