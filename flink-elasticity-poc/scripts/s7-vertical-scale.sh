#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cmd python3
need_cluster

echo "S7 - Vertical scale (bigger TaskManager, same parallelism)"
echo "Expected: TM count and parallelism stay constant, busy-time drops at fixed load"
echo "after the resize, and the resize restart gap is measured."
echo "Observability: scripts/watch.sh and kubectl get pods -n flink-jobs -w"

# Vertical scaling must be measured WITHOUT tripping the horizontal autoscaler:
# the fixed measurement load has to keep the busiest vertex well below the 0.6
# target utilization on the baseline TM (1 TM sustains ~11k events/s at ~100%
# busy, see s2-scale-up.sh), while still producing a busy-time signal large
# enough that halving the CPU throttle is clearly visible.
node_cpu="$(kubectl get node -o jsonpath='{.items[0].status.capacity.cpu}')"
if (( node_cpu >= 6 )); then
  measure_rate=3000
else
  measure_rate=1500
fi
if (( node_cpu < 4 )); then
  echo "FAIL S7 node has ${node_cpu} CPUs; need >= 4 to fit the enlarged TaskManager"
  exit 1
fi

# Baseline resources are read live (flink-job.yaml stays the source of truth at
# rest) and restored on every exit path via the trap below.
BASE_CPU="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.spec.taskManager.resource.cpu}')"
BASE_MEM="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.spec.taskManager.resource.memory}')"
[[ -n "${BASE_CPU}" && -n "${BASE_MEM}" ]] || die "unable to read baseline taskManager resources"
[[ "${BASE_MEM}" =~ ^([0-9]+)m$ ]] || die "unexpected taskManager memory format '${BASE_MEM}' (expected e.g. 1536m)"
NEW_CPU="$(python3 -c "print(format(float('${BASE_CPU}')*2, 'g'))")"
NEW_MEM="$(( BASH_REMATCH[1] + 1024 ))m"

echo "Baseline TM: cpu=${BASE_CPU} memory=${BASE_MEM} -> enlarged TM: cpu=${NEW_CPU} memory=${NEW_MEM}"
echo "Measurement load: ${measure_rate} events/s (node_cpu=${node_cpu})"

RESIZED=false
RESTORED=false

restore_baseline() {
  local rc=$?
  "${ROOT_DIR}/scripts/set-load.sh" 50 >&2 || true
  if [[ "${RESIZED}" == "true" && "${RESTORED}" != "true" ]]; then
    echo "Restoring baseline TaskManager resources (cpu=${BASE_CPU}, memory=${BASE_MEM})"
    local patch="{\"spec\":{\"taskManager\":{\"resource\":{\"cpu\":${BASE_CPU},\"memory\":\"${BASE_MEM}\"}}}}"
    if kubectl -n flink-jobs patch flinkdeployment flink-elastic-job --type merge -p "${patch}"; then
      local deadline=$(( $(date +%s) + 600 )) state
      while (( $(date +%s) < deadline )); do
        state="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
        [[ "${state}" == "RUNNING" ]] && { RESTORED=true; break; }
        sleep 10
      done
      if [[ "${RESTORED}" == "true" ]]; then
        echo "Baseline TaskManager resources restored."
      else
        echo "WARN: restore patch applied but job not RUNNING after 600s; check the deployment."
      fi
    else
      echo "FAIL S7: could not restore baseline TM resources. Run manually:"
      echo "  kubectl -n flink-jobs patch flinkdeployment flink-elastic-job --type merge -p '${patch}'"
      rc=1
    fi
  fi
  exit "${rc}"
}
trap restore_baseline EXIT

# Average of N busy-time samples of the busiest vertex; needs >= 3 valid
# samples, otherwise echoes -1.
sample_busy() {
  local n=5 i val sum=0 count=0
  for (( i=0; i<n; i++ )); do
    val="$(get_max_vertex_busy_time)"
    if [[ "${val}" != "-1" ]]; then
      sum=$(( sum + val ))
      count=$(( count + 1 ))
    fi
    echo "sample busyTimeMsPerSecond=${val}" >&2
    sleep 15
  done
  if (( count >= 3 )); then
    echo $(( sum / count ))
  else
    echo "-1"
  fi
}

# Resize restart gap, adapted from s6-rescale-cost.sh measure_gap: the operator
# applies a taskManager.resource change as a savepoint upgrade under a new
# jobId, so "stable again" is anchored on the NEW job being RUNNING with the
# real-time source backlog drained. Consumer-group lag is unusable here (it
# only moves on the 30s checkpoint cadence). stdout is the gap in seconds.
measure_resize_gap() {
  local start done prev_jid jid state pending
  prev_jid="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.jobId}' 2>/dev/null || true)"
  start="$(date +%s)"
  kubectl -n flink-jobs patch flinkdeployment flink-elastic-job --type merge \
    -p "{\"spec\":{\"taskManager\":{\"resource\":{\"cpu\":${NEW_CPU},\"memory\":\"${NEW_MEM}\"}}}}" >&2

  done=""
  local deadline=$(( start + 1200 ))
  while (( $(date +%s) < deadline )); do
    jid="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.jobId}' 2>/dev/null || true)"
    state="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
    if [[ -n "${jid}" && "${jid}" != "${prev_jid}" && "${state}" == "RUNNING" ]]; then
      pending="$(get_source_pending_records)"
      echo "poll resize state=${state} jobId=new pendingRecords=${pending}" >&2
      if [[ "${pending}" != "-1" ]] && (( pending < 1000 )); then
        done="$(date +%s)"
        break
      fi
    else
      echo "poll resize state=${state:-unknown} jobId=$([[ "${jid}" == "${prev_jid}" ]] && echo old || echo "${jid:-none}")" >&2
    fi
    sleep 10
  done

  [[ -n "${done}" ]] || die "resize did not return to stable state in time"
  echo $(( done - start ))
}

base_tm="$(get_tm_count)"
base_vertices="$(get_vertex_parallelism)"
echo "Baseline: tm=${base_tm} vertices=${base_vertices}"

# --- Window A: busy-time on the baseline TM at fixed sub-trigger load ---
"${ROOT_DIR}/scripts/set-load.sh" "${measure_rate}"
echo "Window A: stabilizing 90s at ${measure_rate} events/s on baseline TM"
sleep 90
busy_before="$(sample_busy)"
tm_a="$(get_tm_count)"
vertices_a="$(get_vertex_parallelism)"
echo "Window A result: busy_before=${busy_before} tm=${tm_a} vertices=${vertices_a}"

if [[ "${busy_before}" == "-1" ]]; then
  echo "FAIL S7 could not collect busy-time samples before resize"
  exit 1
fi
if (( busy_before < 100 )); then
  echo "FAIL S7 busy_before=${busy_before} ms/s is too weak a signal; raise the measurement rate for this VM"
  exit 1
fi
if [[ "${tm_a}" != "${base_tm}" || "${vertices_a}" != "${base_vertices}" ]]; then
  echo "FAIL S7 autoscaler interfered during window A (tm ${base_tm}->${tm_a}, vertices changed); load target too high for this VM"
  exit 1
fi

# --- Resize: enlarge the TaskManager, measure the restart gap ---
echo "Resizing TaskManager to cpu=${NEW_CPU} memory=${NEW_MEM} (load stays at ${measure_rate} events/s)"
RESIZED=true
resize_gap="$(measure_resize_gap)"
echo "Resize gap: ${resize_gap}s (patch -> new job RUNNING with backlog drained)"

# --- Window B: same load, same sampling, on the enlarged TM ---
echo "Window B: stabilizing 90s at ${measure_rate} events/s on enlarged TM"
sleep 90
busy_after="$(sample_busy)"
tm_b="$(get_tm_count)"
vertices_b="$(get_vertex_parallelism)"
echo "Window B result: busy_after=${busy_after} tm=${tm_b} vertices=${vertices_b}"

if [[ "${busy_after}" == "-1" ]]; then
  echo "FAIL S7 could not collect busy-time samples after resize"
  exit 1
fi
if [[ "${tm_b}" != "${base_tm}" || "${vertices_b}" != "${base_vertices}" ]]; then
  echo "FAIL S7 autoscaler interfered during window B (tm ${base_tm}->${tm_b}, vertices changed); load target too high for this VM"
  exit 1
fi

# PASS requires a >= 20% relative busy-time drop: doubling the CPU quota at a
# fixed event rate should roughly halve busy-time, so 20% is noise-tolerant
# while still proving the per-TM capacity gain.
threshold=$(( busy_before * 80 / 100 ))
if (( busy_after <= threshold )); then
  echo "PASS S7 vertical-scale base_cpu=${BASE_CPU} new_cpu=${NEW_CPU} busy_before=${busy_before} busy_after=${busy_after} resize_gap_s=${resize_gap} tm_count=${base_tm}"
else
  echo "FAIL S7 busy_after=${busy_after} not <= 80% of busy_before=${busy_before}; no measurable vertical capacity gain"
  exit 1
fi
