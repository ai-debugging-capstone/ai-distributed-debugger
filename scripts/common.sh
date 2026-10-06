#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DOTENV_FILE="${PROJECT_ROOT}/.env"

read_dotenv() {
  local key="$1"
  local fallback="${2:-}"
  local from_environment="${!key-}"

  if [[ -n "${from_environment}" ]]; then
    printf '%s' "${from_environment}"
    return
  fi

  if [[ -f "${DOTENV_FILE}" ]]; then
    local line
    line="$(grep -E "^[[:space:]]*${key}=" "${DOTENV_FILE}" | tail -n 1 || true)"
    if [[ -n "${line}" ]]; then
      line="${line#*=}"
      line="${line%$'\r'}"
      if [[ "${line}" == \"*\" && "${line}" == *\" ]]; then
        line="${line:1:${#line}-2}"
      elif [[ "${line}" == \'*\' && "${line}" == *\' ]]; then
        line="${line:1:${#line}-2}"
      fi
      printf '%s' "${line}"
      return
    fi
  fi

  printf '%s' "${fallback}"
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    printf 'Required command not found: %s\n' "${command_name}" >&2
    exit 1
  fi
}

demo_directory() {
  local configured
  configured="$(read_dotenv OTEL_DEMO_DIR '.runtime/opentelemetry-demo')"
  if [[ "${configured}" = /* ]] || [[ "${configured}" =~ ^[A-Za-z]:[\\/] ]]; then
    printf '%s' "${configured}"
  else
    printf '%s/%s' "${PROJECT_ROOT}" "${configured}"
  fi
}

ensure_shared_network() {
  local network="$1"
  if ! docker network inspect "${network}" >/dev/null 2>&1; then
    docker network create --label telemetry-rca.managed=true "${network}" >/dev/null
    printf 'Created shared Docker network %s\n' "${network}"
  fi
}

configure_demo_environment() {
  local collector_config="${PROJECT_ROOT}/infra/demo/otelcol-rca.yml"
  # Compose consumes this value inside YAML rather than as a command-line path,
  # so convert MSYS/Git-Bash paths explicitly for Docker Desktop on Windows.
  if command -v cygpath >/dev/null 2>&1; then
    collector_config="$(cygpath -w "${collector_config}")"
  fi
  export RCA_SHARED_NETWORK="$(read_dotenv RCA_SHARED_NETWORK 'opentelemetry-demo')"
  export RCA_DEMO_COLLECTOR_CONFIG="${collector_config}"
  export DEMO_VERSION=3.1.0
  export IMAGE_VERSION=3.1.0
  # Preserve the Demo's sanitization and infrastructure receivers. Its
  # collector forwards the processed signals to TraceLens in compose.rca.yaml.
  export OTEL_COLLECTOR_HOST=otel-collector
  export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
  export PUBLIC_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:8080/otlp-http/v1/traces
  export GRAFANA_HOST=rca-grafana
  export GRAFANA_PORT=3000
  export LOCUST_AUTOSTART=true
  # Keep the web listener enabled: upstream's healthcheck probes it on 8089.
  # LOCUST_AUTOSTART still begins traffic without operator interaction.
  export LOCUST_HEADLESS=false
  export LOCUST_USERS="${LOCUST_USERS:-10}"
}

demo_compose_args() {
  local profile="$1"
  local directory="$2"
  DEMO_COMPOSE_ARGS=(
    --project-name telemetry-rca-demo
    --env-file "${directory}/.env"
    --file "${directory}/compose.yaml"
  )
  if [[ "${profile}" == "full" ]]; then
    DEMO_COMPOSE_ARGS+=(--file "${directory}/compose.full.yaml")
  fi
  DEMO_COMPOSE_ARGS+=(--file "${PROJECT_ROOT}/infra/demo/compose.rca.yaml")
}

demo_profile_from_state() {
  local state_file="${PROJECT_ROOT}/.runtime/demo-profile"
  if [[ -f "${state_file}" ]]; then
    tr -d '\r\n' < "${state_file}"
  else
    read_dotenv OTEL_DEMO_PROFILE full
  fi
}

python_command() {
  if command -v python3 >/dev/null 2>&1; then
    printf '%s' python3
  elif command -v python >/dev/null 2>&1; then
    printf '%s' python
  else
    printf 'Python 3 is required for safe feature-flag editing.\n' >&2
    exit 1
  fi
}
