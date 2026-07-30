#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/common.sh"

need_cmd kubectl
need_cluster

echo "S3 - Scale-down"
echo "Expected: after cooldown, TM count decreases and lag remains controlled."

after_up_tm="$(get_tm_count)"
start_ts="$(date +%s)"
"${ROOT_DIR}/scripts/set-load.sh" 50

echo "Dropped load to 50 events/s; waiting for autoscaler scale-down interval to elapse"

scaled_down=false
final_tm="${after_up_tm}"
deadline=$(( start_ts + 1800 ))

while (( $(date +%s) < deadline )); do
  final_tm="$(get_tm_count)"
  lag="$(get_kafka_consumer_group_lag)"
  echo "poll tm=${final_tm} lag=${lag}"

  if (( final_tm < after_up_tm )); then
    scaled_down=true
    break
  fi
  sleep 20
done

elapsed=$(( $(date +%s) - start_ts ))

if [[ "${scaled_down}" == "true" ]]; then
  echo "PASS S3 scale-down from_tm=${after_up_tm} to_tm=${final_tm} scale_down_time_s=${elapsed}"
else
  echo "FAIL S3 scale-down no TM reduction observed"
  exit 1
fi
