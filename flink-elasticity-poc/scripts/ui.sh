#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

svc="$(kubectl -n flink-jobs get svc -o name | grep flink-elastic-job | grep rest | head -n1 | cut -d/ -f2 || true)"
[[ -n "${svc}" ]] || die "Flink REST service not found in namespace flink-jobs"

info "Forwarding Flink UI on http://127.0.0.1:8081"
kubectl -n flink-jobs port-forward svc/"${svc}" 8081:8081
