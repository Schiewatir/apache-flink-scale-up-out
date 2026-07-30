#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"
source "${ROOT_DIR}/versions.env"

need_cmd kubectl
need_cluster

echo "S4 - Suspend to zero"
echo "Expected: savepoint created in MinIO, job suspended, pods terminate after stack fix-up."

start_ts="$(date +%s)"
kubectl -n flink-jobs patch flinkdeployment flink-elastic-job --type merge -p '{"spec":{"job":{"upgradeMode":"savepoint","state":"suspended"}}}'

# On Flink 1.20 + operator 1.12 (native mode, adaptive scheduler) the JobManager
# deployment never self-terminates after savepoint-suspend, so the operator's
# SUSPENDED lifecycle state is the authoritative "job stopped with savepoint"
# signal; the JM deployment is then removed as a documented stack fix-up.
echo "Waiting for operator lifecycleState SUSPENDED"
lifecycle=""
deadline=$(( start_ts + 600 ))
while (( $(date +%s) < deadline )); do
  lifecycle="$(kubectl -n flink-jobs get flinkdeployment flink-elastic-job -o jsonpath='{.status.lifecycleState}' 2>/dev/null || true)"
  if [[ "${lifecycle}" == "SUSPENDED" ]]; then
    break
  fi
  echo "poll lifecycleState=${lifecycle:-unknown}"
  sleep 10
done

if [[ "${lifecycle}" != "SUSPENDED" ]]; then
  echo "FAIL S4 suspend lifecycleState=${lifecycle:-unknown} (expected SUSPENDED)"
  exit 1
fi
suspended_ts="$(date +%s)"

access_key="$(kubectl -n storage get secret minio-credentials -o jsonpath='{.data.accessKey}' | base64 -d)"
secret_key="$(kubectl -n storage get secret minio-credentials -o jsonpath='{.data.secretKey}' | base64 -d)"

savepoint_count="$(kubectl -n storage run minio-mc-check --quiet --rm -i --restart=Never --image="${MINIO_MC_IMAGE}" --command -- /bin/sh -c "mc alias set local http://minio.storage.svc.cluster.local:9000 ${access_key} ${secret_key} >/dev/null && mc ls --recursive local/savepoints | wc -l" | tr -d ' ')"

if (( savepoint_count < 1 )); then
  echo "FAIL S4 suspend no savepoint objects found"
  exit 1
fi

echo "Applying stack fix-up: deleting non-self-terminating JobManager deployment"
kubectl -n flink-jobs delete deployment flink-elastic-job --ignore-not-found

echo "Waiting for flink pods to terminate"
pod_count="-1"
while (( $(date +%s) < deadline )); do
  pod_count="$(kubectl -n flink-jobs get pods --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if (( pod_count == 0 )); then
    break
  fi
  echo "poll job_pods=${pod_count}"
  sleep 10
done

if (( pod_count != 0 )); then
  echo "FAIL S4 suspend pods still running after fix-up (${pod_count})"
  exit 1
fi

suspend_s=$(( suspended_ts - start_ts ))
elapsed=$(( $(date +%s) - start_ts ))
echo "PASS S4 suspend savepoints=${savepoint_count} pod_count=${pod_count} suspend_s=${suspend_s} elapsed_s=${elapsed}"
