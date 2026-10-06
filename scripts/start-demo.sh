#!/usr/bin/env bash

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

profile="$(read_dotenv OTEL_DEMO_PROFILE full)"
start_platform=true

while (($#)); do
  case "$1" in
    --core) profile=core ;;
    --full) profile=full ;;
    --skip-platform) start_platform=false ;;
    -h|--help)
      printf 'Usage: %s [--core|--full] [--skip-platform]\n' "$0"
      exit 0
      ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done

if [[ "${profile}" != "core" && "${profile}" != "full" ]]; then
  printf 'Profile must be core or full, got: %s\n' "${profile}" >&2
  exit 2
fi

require_command docker
"${SCRIPT_DIR}/fetch-demo.sh"

if [[ "${start_platform}" == true ]]; then
  printf 'Starting the RCA platform...\n'
  docker compose --file "${PROJECT_ROOT}/docker-compose.yml" up --detach --build
fi

configure_demo_environment
ensure_shared_network "${RCA_SHARED_NETWORK}"

directory="$(demo_directory)"
demo_compose_args "${profile}" "${directory}"

printf 'Pulling immutable OpenTelemetry Demo 3.1.0 images...\n'
docker compose "${DEMO_COMPOSE_ARGS[@]}" pull
docker compose "${DEMO_COMPOSE_ARGS[@]}" up --detach --no-build --remove-orphans

mkdir -p "${PROJECT_ROOT}/.runtime"
printf '%s\n' "${profile}" > "${PROJECT_ROOT}/.runtime/demo-profile"

printf 'Waiting for the demo frontend proxy to become healthy'
deadline=$((SECONDS + 300))
while ((SECONDS < deadline)); do
  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' frontend-proxy 2>/dev/null || true)"
  if [[ "${health}" == "healthy" || "${health}" == "running" ]]; then
    printf '\n'
    break
  fi
  printf '.'
  sleep 5
done

if [[ "${health:-}" != "healthy" && "${health:-}" != "running" ]]; then
  printf '\nDemo did not become healthy within five minutes. Inspect with:\n' >&2
  printf 'docker compose %q logs frontend-proxy\n' "${DEMO_COMPOSE_ARGS[*]}" >&2
  exit 1
fi

printf 'OpenTelemetry Demo: http://localhost:8080\n'
printf 'RCA dashboard:      http://localhost:%s/d/rca-overview\n' "$(read_dotenv GRAFANA_PORT 3000)"
printf 'RCA API:            http://localhost:%s/docs\n' "$(read_dotenv RCA_API_PORT 8000)"
printf 'All demo telemetry is routed to otel-gateway on network %s.\n' "${RCA_SHARED_NETWORK}"
