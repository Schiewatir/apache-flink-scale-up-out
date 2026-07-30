#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${ROOT_DIR}/scripts/common.sh"
source "${ROOT_DIR}/versions.env"

WITH_METRICS=false
WITH_CONSOLE=false
for arg in "$@"; do
  case "${arg}" in
    --with-metrics) WITH_METRICS=true ;;
    --with-console) WITH_CONSOLE=true ;;
  esac
done

need_cmd nproc
need_cmd free
need_cmd df
need_cmd kubectl
need_cmd helm
need_cmd minikube
need_cmd openssl
need_cmd awk

TOTAL_CPUS="$(nproc)"
TOTAL_MEM_GB="$(free -g | awk '/^Mem:/ {print $2}')"
DISK_FREE_GB="$(df -BG / | awk 'NR==2 {gsub(/G/,"",$4); print $4}')"

FALLBACK=false
if (( TOTAL_CPUS < 4 || TOTAL_MEM_GB < 8 )); then
  FALLBACK=true
  warn "Host is below recommended minimum (4 CPU, 8 GB RAM). Proceeding in reduced-footprint mode."
fi

compute_clamped() {
  local v="$1"
  local min="$2"
  local max="$3"
  if (( v < min )); then
    echo "${min}"
  elif (( v > max )); then
    echo "${max}"
  else
    echo "${v}"
  fi
}

MINIKUBE_CPUS_RAW=$(( TOTAL_CPUS - 2 ))
MINIKUBE_MEM_GB_RAW=$(( TOTAL_MEM_GB - 4 ))

if [[ "${FALLBACK}" == "true" ]]; then
  MINIKUBE_CPUS="$(compute_clamped "$(( TOTAL_CPUS - 1 ))" 2 4)"
  MINIKUBE_MEM_GB="$(compute_clamped "$(( TOTAL_MEM_GB - 1 ))" 4 8)"
else
  MINIKUBE_CPUS="$(compute_clamped "${MINIKUBE_CPUS_RAW}" 4 8)"
  MINIKUBE_MEM_GB="$(compute_clamped "${MINIKUBE_MEM_GB_RAW}" 8 16)"
fi

USE_STRIMZI=false
if (( MINIKUBE_CPUS >= 6 )); then
  USE_STRIMZI=true
fi

HEADROOM_CPU="$(awk -v c="${MINIKUBE_CPUS}" 'BEGIN {printf "%.1f", c*0.2}')"
HEADROOM_MEM="$(awk -v m="${MINIKUBE_MEM_GB}" 'BEGIN {printf "%.1f", m*0.2}')"

info "Host resource snapshot"
nproc
free -g
df -h /

cat <<EOF

Resource Budget (Minikube envelope)
-----------------------------------
Host CPUs: ${TOTAL_CPUS}
Host Memory(GB): ${TOTAL_MEM_GB}
Host Free Disk(GB): ${DISK_FREE_GB}
Minikube CPUs: ${MINIKUBE_CPUS}
Minikube Memory(GB): ${MINIKUBE_MEM_GB}
Reserved headroom CPU: ${HEADROOM_CPU}
Reserved headroom Memory(GB): ${HEADROOM_MEM}
Kafka mode: $( [[ "${USE_STRIMZI}" == "true" ]] && echo "Strimzi KRaft single-broker" || echo "Fallback single-pod StatefulSet" )

Component requests (cpu/memory)
- cert-manager: 0.2 / 0.25Gi
- flink-operator: 0.2 / 1.0Gi
- minio: 0.25 / 0.5Gi
- kafka: 0.5 / 1.5Gi
- flink JM: 0.5 / 1.0Gi
- flink TM per pod: 0.5 / 1.5Gi
- loadgen: 0.1 / 0.125Gi
EOF

if (( DISK_FREE_GB < 20 )); then
  die "Need at least 20GB free disk for images, PVCs, and build artifacts."
fi

info "Starting or reusing minikube profile ${PROFILE}"
minikube start \
  -p "${PROFILE}" \
  --driver=docker \
  --kubernetes-version="${KUBERNETES_VERSION}" \
  --cpus="${MINIKUBE_CPUS}" \
  --memory="$(( MINIKUBE_MEM_GB * 1024 ))"

kubectl config use-context "${PROFILE}"
need_cluster

info "Pre-pulling pinned images"
minikube -p "${PROFILE}" image pull "${FLINK_IMAGE}"
minikube -p "${PROFILE}" image pull "${MINIO_IMAGE}"
minikube -p "${PROFILE}" image pull "${MINIO_MC_IMAGE}"
minikube -p "${PROFILE}" image pull "${KAFKA_FALLBACK_IMAGE}"
minikube -p "${PROFILE}" image pull "${PROMETHEUS_IMAGE}"
minikube -p "${PROFILE}" image pull "${BUSYBOX_IMAGE}"

info "Deploying cert-manager"
helm repo add jetstack https://charts.jetstack.io >/dev/null
helm repo update >/dev/null
kubectl create namespace cert-manager --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --version "${CERT_MANAGER_VERSION}" \
  --set crds.enabled=true
kubectl -n cert-manager rollout status deployment/cert-manager --timeout=300s
kubectl -n cert-manager rollout status deployment/cert-manager-webhook --timeout=300s
kubectl -n cert-manager rollout status deployment/cert-manager-cainjector --timeout=300s

info "Deploying Flink Kubernetes Operator with autoscaler enabled"
kubectl create namespace flink-operator --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace flink-jobs --dry-run=client -o yaml | kubectl apply -f -
for attempt in 1 2 3; do
  if helm upgrade --install flink-kubernetes-operator \
    https://archive.apache.org/dist/flink/flink-kubernetes-operator-${FLINK_OPERATOR_CHART_VERSION}/flink-kubernetes-operator-${FLINK_OPERATOR_CHART_VERSION}-helm.tgz \
    --namespace flink-operator \
    -f "${ROOT_DIR}/manifests/flink-operator/values.yaml"; then
    break
  fi
  warn "Flink operator install attempt ${attempt}/3 failed; waiting for cert-manager webhook and retrying"
  kubectl -n cert-manager rollout status deployment/cert-manager-webhook --timeout=120s || true
  sleep 10
  if (( attempt == 3 )); then
    die "Flink operator installation failed after retries"
  fi
done
kubectl -n flink-operator rollout status deployment/flink-kubernetes-operator --timeout=300s
kubectl get crd flinkdeployments.flink.apache.org >/dev/null

info "Deploying MinIO"
kubectl create namespace storage --dry-run=client -o yaml | kubectl apply -f -
if kubectl -n storage get secret minio-credentials >/dev/null 2>&1; then
  MINIO_ACCESS_KEY="$(kubectl -n storage get secret minio-credentials -o jsonpath='{.data.accessKey}' | base64 -d)"
  MINIO_SECRET_KEY="$(kubectl -n storage get secret minio-credentials -o jsonpath='{.data.secretKey}' | base64 -d)"
  info "Reusing existing MinIO credentials secret"
else
  MINIO_ACCESS_KEY="minio$(openssl rand -hex 4)"
  MINIO_SECRET_KEY="$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24)"
  kubectl -n storage create secret generic minio-credentials \
    --from-literal=accessKey="${MINIO_ACCESS_KEY}" \
    --from-literal=secretKey="${MINIO_SECRET_KEY}"
fi
kubectl apply -f "${ROOT_DIR}/manifests/storage/minio.yaml"
wait_for_rollout storage deployment/minio

info "Creating checkpoint and savepoint buckets"
kubectl -n storage run minio-mc --rm -i --restart=Never --image="${MINIO_MC_IMAGE}" --command -- \
  /bin/sh -c "mc alias set local http://minio.storage.svc.cluster.local:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} && mc mb -p local/checkpoints && mc mb -p local/savepoints"

info "Deploying Kafka"
kubectl apply -f "${ROOT_DIR}/manifests/kafka/namespace.yaml"

deploy_fallback_kafka() {
  warn "Using fallback single-pod Kafka StatefulSet"
  kubectl -n kafka delete statefulset kafka --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n kafka delete service kafka --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl apply -f "${ROOT_DIR}/manifests/kafka/fallback-kafka.yaml"
  kubectl -n kafka rollout status statefulset/kafka --timeout=600s
  KAFKA_POD="$(kubectl -n kafka get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n kafka exec "${KAFKA_POD}" -- bash -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 --create --if-not-exists --topic events-in --partitions 12 --replication-factor 1"
  kubectl -n kafka exec "${KAFKA_POD}" -- bash -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 --create --if-not-exists --topic events-out --partitions 6 --replication-factor 1"
}

if [[ "${USE_STRIMZI}" == "true" ]]; then
  helm repo add strimzi https://strimzi.io/charts/ >/dev/null
  helm repo update >/dev/null
  helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace kafka \
    --version "${STRIMZI_CHART_VERSION}"
  kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=300s
  kubectl apply -f "${ROOT_DIR}/manifests/kafka/strimzi-kafka.yaml"
  if ! kubectl -n kafka wait kafka/kafka --for=condition=Ready --timeout=180s; then
    warn "Strimzi Kafka did not reach Ready. Falling back to single-pod Kafka for this VM run."
    kubectl -n kafka delete kafka kafka --ignore-not-found=true || true
    kubectl -n kafka delete kafkatopic events-in events-out --ignore-not-found=true || true
    deploy_fallback_kafka
  fi
else
  deploy_fallback_kafka
fi

info "Building local images into minikube cache"
minikube -p "${PROFILE}" image build -t "${LOADGEN_IMAGE}" "${ROOT_DIR}/loadgen"
minikube -p "${PROFILE}" image build -t "${FLINK_JOB_IMAGE}" "${ROOT_DIR}/job"

info "Deploying load generator"
kubectl apply -f "${ROOT_DIR}/manifests/loadgen/loadgen.yaml"
wait_for_rollout loadgen deployment/loadgen

info "Deploying Flink job"
kubectl -n flink-jobs create secret generic minio-s3-credentials \
  --from-literal=accessKey="${MINIO_ACCESS_KEY}" \
  --from-literal=secretKey="${MINIO_SECRET_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n flink-jobs create serviceaccount flink --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "${ROOT_DIR}/manifests/flink-jobs/flink-job.yaml"

info "Verifying autoscaler key compatibility for operator ${FLINK_OPERATOR_APP_VERSION}"
if ! kubectl -n flink-operator get deploy flink-kubernetes-operator -o jsonpath='{.spec.template.spec.containers[0].image}' | grep -q "${FLINK_OPERATOR_APP_VERSION}"; then
  warn "Operator image tag does not match expected app version ${FLINK_OPERATOR_APP_VERSION}. Verify autoscaler keys before running scenarios."
fi

if [[ "${WITH_METRICS}" == "true" ]]; then
  info "Deploying optional minimal Prometheus"
  kubectl apply -f "${ROOT_DIR}/manifests/metrics/prometheus.yaml"
  wait_for_rollout metrics deployment/prometheus
fi

if [[ "${WITH_CONSOLE}" == "true" ]]; then
  need_cmd node
  need_cmd npm
  info "Installing and building the web console (first run)"
  npm --prefix "${ROOT_DIR}/console" install
  npm --prefix "${ROOT_DIR}/console" run build
  echo "Start it with: scripts/console.sh (add --operate to allow mutating actions)"
fi

info "Setup complete"
echo "Run scenarios in order: scripts/s1-baseline.sh ... scripts/s6-rescale-cost.sh"
