#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd minikube

info "Deleting PoC namespaces (best effort)"
for ns in cert-manager flink-operator storage kafka loadgen flink-jobs metrics; do
  kubectl delete namespace "${ns}" --ignore-not-found=true >/dev/null 2>&1 || true
done

info "Deleting minikube profile ${PROFILE}"
minikube delete -p "${PROFILE}" || true

info "Teardown complete"
