#!/usr/bin/env bash

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_command docker
directory="$(demo_directory)"
if [[ ! -f "${directory}/compose.yaml" ]]; then
  printf 'No fetched demo checkout found; nothing to stop.\n'
  exit 0
fi

configure_demo_environment
profile="$(demo_profile_from_state)"
demo_compose_args "${profile}" "${directory}"
docker compose "${DEMO_COMPOSE_ARGS[@]}" down --remove-orphans

printf 'Stopped the demo. RCA PostgreSQL and Grafana data were preserved.\n'
printf 'Run docker compose -f %s down to stop the product stack too.\n' "${PROJECT_ROOT}/docker-compose.yml"
