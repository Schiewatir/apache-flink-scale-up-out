#!/usr/bin/env bash
set -euo pipefail

COMMAND_STANDALONE="standalone-job"
COMMAND_HISTORY_SERVER="history-server"

JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS:-$(hostname -f)}

if [[ -d /opt/flink/conf ]]; then
  rm -rf /tmp/flink-conf
  mkdir -p /tmp/flink-conf
  cp -a /opt/flink/conf/. /tmp/flink-conf/
  chmod -R u+w /tmp/flink-conf
  export FLINK_CONF_DIR=/tmp/flink-conf
else
  export FLINK_CONF_DIR="${FLINK_HOME}/conf"
fi

drop_privs_cmd() {
  if [ "$(id -u)" != 0 ]; then
    return
  elif [ -x /sbin/su-exec ]; then
    echo su-exec flink
  else
    echo gosu flink
  fi
}

copy_plugins_if_required() {
  if [ -z "${ENABLE_BUILT_IN_PLUGINS:-}" ]; then
  return 0
  fi

  echo "Enabling required built-in plugins"
  for target_plugin in $(echo "${ENABLE_BUILT_IN_PLUGINS}" | tr ';' ' '); do
  echo "Linking ${target_plugin} to plugin directory"
  plugin_name=${target_plugin%.jar}

  mkdir -p "${FLINK_HOME}/plugins/${plugin_name}"
  if [ ! -e "${FLINK_HOME}/opt/${target_plugin}" ]; then
    echo "Plugin ${target_plugin} does not exist. Exiting."
    exit 1
  else
    ln -fs "${FLINK_HOME}/opt/${target_plugin}" "${FLINK_HOME}/plugins/${plugin_name}"
    echo "Successfully enabled ${target_plugin}"
  fi
  done
}

set_config_options() {
  local config_parser_script="${FLINK_HOME}/bin/config-parser-utils.sh"
  local config_dir="${FLINK_CONF_DIR}"
  local bin_dir="${FLINK_HOME}/bin"
  local lib_dir="${FLINK_HOME}/lib"

  local config_params=()

  while [ $# -gt 0 ]; do
    local key="$1"
    local value="$2"

    config_params+=("-D${key}=${value}")

    shift 2
  done

  if [ "${#config_params[@]}" -gt 0 ]; then
    "${config_parser_script}" "${config_dir}" "${bin_dir}" "${lib_dir}" "${config_params[@]}"
  fi
}

process_flink_properties() {
  local flink_properties_content=$1
  local config_options=()

  local OLD_IFS="$IFS"
  IFS=$'\n'
  for prop in $flink_properties_content; do
    prop=$(echo "$prop" | tr -d '[:space:]')

    if [ -z "$prop" ]; then
      continue
    fi

    IFS=':' read -r key value <<< "$prop"
    value=$(echo "$value" | envsubst)
    config_options+=("$key" "$value")
  done
  IFS="$OLD_IFS"

  if [ "${#config_options[@]}" -ne 0 ]; then
    set_config_options "${config_options[@]}"
  fi
}

prepare_configuration() {
  local config_options=()

  config_options+=("jobmanager.rpc.address" "${JOB_MANAGER_RPC_ADDRESS}")
  config_options+=("blob.server.port" "6124")
  config_options+=("query.server.port" "6125")

  if [ -n "${TASK_MANAGER_NUMBER_OF_TASK_SLOTS:-}" ]; then
    config_options+=("taskmanager.numberOfTaskSlots" "${TASK_MANAGER_NUMBER_OF_TASK_SLOTS}")
  fi

  if [ "${#config_options[@]}" -ne 0 ]; then
    set_config_options "${config_options[@]}"
  fi

  if [ -n "${FLINK_PROPERTIES:-}" ]; then
    process_flink_properties "${FLINK_PROPERTIES}"
  fi
}

maybe_enable_jemalloc() {
  if [ "${DISABLE_JEMALLOC:-false}" == "false" ]; then
    JEMALLOC_PATH="/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so"
    JEMALLOC_FALLBACK="/usr/lib/x86_64-linux-gnu/libjemalloc.so"
    if [ -f "$JEMALLOC_PATH" ]; then
      export LD_PRELOAD="${LD_PRELOAD:-}:$JEMALLOC_PATH"
    elif [ -f "$JEMALLOC_FALLBACK" ]; then
      export LD_PRELOAD="${LD_PRELOAD:-}:$JEMALLOC_FALLBACK"
    fi
  fi
}

maybe_enable_jemalloc
copy_plugins_if_required
prepare_configuration

args=("$@")
if [ "$1" = "help" ]; then
  printf "Usage: %s (jobmanager|%s|taskmanager|%s)\n" "$(basename "$0")" "$COMMAND_STANDALONE" "$COMMAND_HISTORY_SERVER"
  exit 0
elif [ "$1" = "jobmanager" ]; then
  args=("${args[@]:1}")
  exec $(drop_privs_cmd) env FLINK_CONF_DIR="${FLINK_CONF_DIR}" "${FLINK_HOME}/bin/jobmanager.sh" start-foreground "${args[@]}"
elif [ "$1" = ${COMMAND_STANDALONE} ]; then
  args=("${args[@]:1}")
  exec $(drop_privs_cmd) env FLINK_CONF_DIR="${FLINK_CONF_DIR}" "${FLINK_HOME}/bin/standalone-job.sh" start-foreground "${args[@]}"
elif [ "$1" = ${COMMAND_HISTORY_SERVER} ]; then
  args=("${args[@]:1}")
  exec $(drop_privs_cmd) env FLINK_CONF_DIR="${FLINK_CONF_DIR}" "${FLINK_HOME}/bin/historyserver.sh" start-foreground "${args[@]}"
elif [ "$1" = "taskmanager" ]; then
  args=("${args[@]:1}")
  exec $(drop_privs_cmd) env FLINK_CONF_DIR="${FLINK_CONF_DIR}" "${FLINK_HOME}/bin/taskmanager.sh" start-foreground "${args[@]}"
fi

exec $(drop_privs_cmd) env FLINK_CONF_DIR="${FLINK_CONF_DIR}" "${args[@]}"
