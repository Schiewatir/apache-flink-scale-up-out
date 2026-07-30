#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="flink-elastic"
NAMESPACE_FLINK="flink-jobs"
NAMESPACE_KAFKA="kafka"
NAMESPACE_LOADGEN="loadgen"
NAMESPACE_STORAGE="storage"

info() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

die() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

need_cluster() {
  kubectl config current-context | grep -q "${PROFILE}" || die "kubectl context is not set to minikube profile ${PROFILE}"
}

get_kafka_pod() {
  kubectl -n "${NAMESPACE_KAFKA}" get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

get_kafka_consumer_group_lag() {
  local pod
  pod="$(get_kafka_pod)"
  if [[ -z "${pod}" ]]; then
    echo "-1"
    return 0
  fi

  # KAFKA_HEAP_OPTS must be capped: exec sessions do not inherit the broker's
  # entrypoint heap opts, and an uncapped CLI JVM inside the broker cgroup can
  # OOM-kill kafka-0.
  kubectl -n "${NAMESPACE_KAFKA}" exec "${pod}" -- bash -lc \
    "export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'; /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --group flink-elastic-consumer --describe 2>/dev/null | awk 'NR>1 {sum+=\$6} END {print (sum==\"\"?0:sum)}'" \
    2>/dev/null || echo "-1"
}

get_tm_count() {
  kubectl -n "${NAMESPACE_FLINK}" get pods -l component=taskmanager --no-headers 2>/dev/null | wc -l | tr -d ' '
}

# Real-time source backlog (unread Kafka records) reported by the Flink source
# operator. Unlike the Kafka consumer-group lag, this is NOT distorted by the
# checkpoint-interval offset-commit cadence, so it reflects the true in-flight
# backlog and drops to ~0 once the pipeline keeps up. Echoes "-1" if unavailable.
get_source_pending_records() {
  local pod jid vid
  pod="$(kubectl -n "${NAMESPACE_FLINK}" get pod -l component=jobmanager,app=flink-elastic-job -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [[ -n "${pod}" ]] || { echo "-1"; return 0; }

  jid="$(kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s localhost:8081/jobs 2>/dev/null \
    | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["jobs"][0]["id"])
except Exception: pass' 2>/dev/null)"
  [[ -n "${jid}" ]] || { echo "-1"; return 0; }

  vid="$(kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s "localhost:8081/jobs/${jid}" 2>/dev/null \
    | python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
    print(next(v["id"] for v in d["vertices"] if "kafka-source" in v["name"]))
except Exception: pass' 2>/dev/null)"
  [[ -n "${vid}" ]] || { echo "-1"; return 0; }

  # Discover all pendingRecords metric ids (one per subtask) and sum their values.
  local ids
  ids="$(kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s "localhost:8081/jobs/${jid}/vertices/${vid}/metrics" 2>/dev/null \
    | python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
    print(",".join(m["id"] for m in d if m["id"].endswith("pendingRecords")))
except Exception: pass' 2>/dev/null)"
  [[ -n "${ids}" ]] || { echo "-1"; return 0; }

  kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s "localhost:8081/jobs/${jid}/vertices/${vid}/metrics?get=${ids}" 2>/dev/null \
    | python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
    print(int(sum(float(m["value"]) for m in d if m.get("value") not in (None,""))))
except Exception: print(-1)' 2>/dev/null || echo "-1"
}

get_jm_pod() {
  kubectl -n "${NAMESPACE_FLINK}" get pod -l component=jobmanager,app=flink-elastic-job -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

get_flink_job_id() {
  local pod="$1"
  kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s localhost:8081/jobs 2>/dev/null \
    | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["jobs"][0]["id"])
except Exception: pass' 2>/dev/null
}

# Current parallelism of every job vertex, as one comma-separated line of
# name=parallelism pairs. Vertex names are sanitized (spaces and punctuation
# collapsed to "_", truncated) so the output stays a single parsable token
# inside key=value scenario log lines. Echoes "-1" if unavailable.
get_vertex_parallelism() {
  local pod jid out
  pod="$(get_jm_pod)"
  [[ -n "${pod}" ]] || { echo "-1"; return 0; }
  jid="$(get_flink_job_id "${pod}")"
  [[ -n "${jid}" ]] || { echo "-1"; return 0; }

  out="$(kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s "localhost:8081/jobs/${jid}" 2>/dev/null \
    | python3 -c 'import json,re,sys
try:
    d=json.load(sys.stdin)
    print(",".join(
        re.sub(r"[^A-Za-z0-9_.-]+","_",v["name"]).strip("_")[:40]+"="+str(v["parallelism"])
        for v in d["vertices"]))
except Exception: pass' 2>/dev/null)"
  [[ -n "${out}" ]] && echo "${out}" || echo "-1"
}

# busyTimeMsPerSecond (subtask average, 0..1000) of the busiest vertex. Used by
# S7 as the per-TM capacity signal: at a fixed event rate, a bigger CPU quota
# means less busy time per wall second. Echoes "-1" if unavailable.
get_max_vertex_busy_time() {
  local pod jid vids vid val max="-1"
  pod="$(get_jm_pod)"
  [[ -n "${pod}" ]] || { echo "-1"; return 0; }
  jid="$(get_flink_job_id "${pod}")"
  [[ -n "${jid}" ]] || { echo "-1"; return 0; }

  vids="$(kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s "localhost:8081/jobs/${jid}" 2>/dev/null \
    | python3 -c 'import json,sys
try: print(" ".join(v["id"] for v in json.load(sys.stdin)["vertices"]))
except Exception: pass' 2>/dev/null)"
  [[ -n "${vids}" ]] || { echo "-1"; return 0; }

  for vid in ${vids}; do
    val="$(kubectl -n "${NAMESPACE_FLINK}" exec "${pod}" -- curl -s "localhost:8081/jobs/${jid}/vertices/${vid}/subtasks/metrics?get=busyTimeMsPerSecond" 2>/dev/null \
      | python3 -c 'import json,math,sys
try:
    a=next(m["avg"] for m in json.load(sys.stdin) if m["id"]=="busyTimeMsPerSecond")
    a=float(a)
    print(int(round(a)) if not math.isnan(a) else "")
except Exception: pass' 2>/dev/null)"
    if [[ -n "${val}" ]] && (( val > max )); then
      max="${val}"
    fi
  done
  echo "${max}"
}

get_last_checkpoint_age_seconds() {
  local rest_svc
  rest_svc="$(kubectl -n "${NAMESPACE_FLINK}" get svc -o name | grep flink-elastic-job | grep rest | head -n1 | cut -d/ -f2 || true)"
  [[ -n "${rest_svc}" ]] || { echo "-1"; return 0; }

  kubectl -n "${NAMESPACE_FLINK}" run rest-curl --quiet --rm -i --restart=Never --image=curlimages/curl:8.9.1 -- \
    -s "http://${rest_svc}:8081/jobs/overview" 2>/dev/null | grep -q '"jobs"' || { echo "-1"; return 0; }

  local job_id
  job_id="$(kubectl -n "${NAMESPACE_FLINK}" run rest-curl --quiet --rm -i --restart=Never --image=curlimages/curl:8.9.1 -- -s "http://${rest_svc}:8081/jobs/overview" | sed -n 's/.*"jid":"\([^"]*\)".*/\1/p' | head -n1)"
  [[ -n "${job_id}" ]] || { echo "-1"; return 0; }

  local ts now
  ts="$(kubectl -n "${NAMESPACE_FLINK}" run rest-curl --quiet --rm -i --restart=Never --image=curlimages/curl:8.9.1 -- -s "http://${rest_svc}:8081/jobs/${job_id}/checkpoints" | sed -n 's/.*"latest".*"completed".*"trigger_timestamp":\([0-9]*\).*/\1/p' | head -n1)"
  [[ -n "${ts}" ]] || { echo "-1"; return 0; }

  now="$(date +%s)"
  echo "$((now - (ts / 1000)))"
}

wait_for_rollout() {
  local ns="$1"
  local kind_name="$2"
  kubectl -n "${ns}" rollout status "${kind_name}" --timeout=300s
}
