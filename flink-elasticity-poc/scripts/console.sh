#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONSOLE_DIR="${ROOT_DIR}/console"
source "${ROOT_DIR}/scripts/common.sh"

OPERATE=false
if [[ "${1:-}" == "--operate" ]]; then
  OPERATE=true
fi

need_cmd node
need_cmd npm
need_cmd kubectl
need_cluster

NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
if (( NODE_MAJOR < 20 )); then
  die "Console requires Node.js >= 20 (found $(node --version)); see console/.nvmrc"
fi

if [[ ! -d "${CONSOLE_DIR}/node_modules" ]]; then
  info "Installing console dependencies (first run)"
  npm --prefix "${CONSOLE_DIR}" install
fi

if [[ ! -f "${CONSOLE_DIR}/backend/dist/index.js" || ! -f "${CONSOLE_DIR}/frontend/dist/index.html" ]]; then
  info "Building console (first run)"
  npm --prefix "${CONSOLE_DIR}" run build
fi

CONSOLE_PORT="${CONSOLE_PORT:-8088}"
info "Starting Flink Elasticity Console on http://127.0.0.1:${CONSOLE_PORT} (mode: $([[ "${OPERATE}" == "true" ]] && echo operate || echo read-only))"
info "The console establishes its own managed Flink REST port-forward and cleans it up on exit."

cd "${CONSOLE_DIR}"
if [[ "${OPERATE}" == "true" ]]; then
  exec node backend/dist/index.js --operate
else
  exec node backend/dist/index.js
fi
