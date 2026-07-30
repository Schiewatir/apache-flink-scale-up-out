#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

echo "S6 - Manual rescale cost"
echo "Expected: measurable restart gap; larger-state run has >= small-state gap."

current_p="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.spec.job.parallelism}')"
if [[ -z "${current_p}" ]]; then
  echo "FAIL S6 unable to read current parallelism"
  exit 1
fi
next_p=$(( current_p == 6 ? 5 : current_p + 1 ))

measure_gap() {
  local label="$1"
  local from_p="$2"
  local to_p="$3"

  local start done prev_jid jid state pending
  # The operator applies a parallelism change as a savepoint upgrade that
  # redeploys the job under a new jobId. The old job stays RUNNING with an
  # empty backlog until then, so "stable" must be anchored on the NEW job:
  # jobId changed, state RUNNING, and pendingRecords drained. Consumer-group
  # lag is unusable here: offsets commit only on the 30s checkpoint cadence,
  # so under load the reported lag sawtooths to rate*30.
  prev_jid="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.jobId}' 2>/dev/null || true)"
  start="$(date +%s)"
  # stdout of this function is captured as the measured gap; kubectl output
  # must go to stderr so it does not pollute the command substitution.
  kubectl -n flink-jobs patch flinkdeployment flink-elastic-job --type merge -p "{\"spec\":{\"job\":{\"parallelism\":${to_p}}}}" >&2

  done=""
  local deadline=$(( start + 1200 ))
  while (( $(date +%s) < deadline )); do
    jid="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.jobId}' 2>/dev/null || true)"
    state="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
    if [[ -n "${jid}" && "${jid}" != "${prev_jid}" && "${state}" == "RUNNING" ]]; then
      pending="$(get_source_pending_records)"
      echo "poll ${label} state=${state} jobId=new pendingRecords=${pending}" >&2
      if [[ "${pending}" != "-1" ]] && (( pending < 1000 )); then
        done="$(date +%s)"
        break
      fi
    else
      echo "poll ${label} state=${state:-unknown} jobId=$([[ "${jid}" == "${prev_jid}" ]] && echo old || echo "${jid:-none}")" >&2
    fi
    sleep 10
  done

  [[ -n "${done}" ]] || die "${label} rescale did not return to stable state in time"
  echo $(( done - start ))
}

small_gap="$(measure_gap small-state "${current_p}" "${next_p}")"

echo "Growing state for 15 minutes at 500 ev/s"
"${ROOT_DIR}/scripts/set-load.sh" 500
sleep 900

large_target=$(( next_p == 6 ? 5 : next_p + 1 ))
large_gap="$(measure_gap large-state "${next_p}" "${large_target}")"

if (( large_gap >= small_gap )); then
  echo "PASS S6 rescale-cost small_gap_s=${small_gap} large_gap_s=${large_gap}"
else
  echo "FAIL S6 expected large-state gap >= small-state gap but got ${large_gap} < ${small_gap}"
  exit 1
fi
