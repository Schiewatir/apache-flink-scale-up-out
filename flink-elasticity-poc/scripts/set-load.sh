#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

RATE="${1:-}"
[[ -n "${RATE}" ]] || die "Usage: scripts/set-load.sh <events_per_sec>"
[[ "${RATE}" =~ ^[0-9]+$ ]] || die "Rate must be a positive integer"
(( RATE > 0 )) || die "Rate must be greater than zero"

info "Setting load generator rate to ${RATE} events/s"
kubectl -n loadgen patch configmap loadgen-config --type merge -p "{\"data\":{\"EVENTS_PER_SEC\":\"${RATE}\"}}"
kubectl -n loadgen rollout restart deployment/loadgen
kubectl -n loadgen rollout status deployment/loadgen --timeout=180s

info "Load generator now running at ${RATE} events/s"
