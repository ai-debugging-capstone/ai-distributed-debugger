#!/usr/bin/env bash

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

scenario=paymentFailure
variant=50%
seed=42
split=unassigned
warmup=60
duration=120
recovery=60
acceptance_top_k=3

while (($#)); do
  case "$1" in
    --scenario) scenario="$2"; shift ;;
    --variant) variant="$2"; shift ;;
    --seed) seed="$2"; shift ;;
    --split) split="$2"; shift ;;
    --warmup) warmup="$2"; shift ;;
    --duration) duration="$2"; shift ;;
    --recovery) recovery="$2"; shift ;;
    --acceptance-top-k) acceptance_top_k="$2"; shift ;;
    -h|--help)
      printf 'Usage: %s [--scenario paymentFailure] [--variant 50%%] [--seed 42] [--split train|validation|test|holdout|unassigned] [--warmup 60] [--duration 120] [--recovery 60] [--acceptance-top-k 3]\n' "$0"
      exit 0
      ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done

for value in "${warmup}" "${duration}" "${recovery}"; do
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    printf 'Durations must be non-negative integers.\n' >&2
    exit 2
  fi
done
if [[ ! "${seed}" =~ ^(0|[1-9][0-9]{0,9})$ ]] || ((10#${seed} > 2147483647)); then
  printf 'Seed must be an integer from 0 through 2147483647.\n' >&2
  exit 2
fi
case "${split}" in
  train|validation|test|holdout|unassigned) ;;
  *) printf 'Split must be train, validation, test, holdout, or unassigned.\n' >&2; exit 2 ;;
esac
if [[ ! "${acceptance_top_k}" =~ ^[1-9][0-9]*$ ]] || ((10#${acceptance_top_k} > 100)); then
  printf 'Acceptance top-k must be an integer from 1 through 100.\n' >&2
  exit 2
fi

require_command docker
python="$(python_command)"
directory="$(demo_directory)"
flag_file="${directory}/src/flagd/demo.flagd.json"
if [[ ! -f "${flag_file}" ]]; then
  printf 'Demo is not fetched. Run scripts/start-demo.sh first.\n' >&2
  exit 1
fi

configure_demo_environment
profile="$(demo_profile_from_state)"
demo_compose_args "${profile}" "${directory}"

wait_demo_ready() {
  local services_output service ids container_id state
  local -a required_services pending
  if ! services_output="$(docker compose "${DEMO_COMPOSE_ARGS[@]}" config --services)"; then
    printf 'Could not determine the required Demo service set.\n' >&2
    return 1
  fi
  mapfile -t required_services <<<"${services_output}"
  if ((${#required_services[@]} == 0)); then
    printf 'The Demo Compose model contains no services.\n' >&2
    return 1
  fi
  for service in flagd frontend-proxy load-generator; do
    if [[ ! " ${required_services[*]} " =~ " ${service} " ]]; then
      printf 'Required Demo service %q is absent from the Compose model.\n' "${service}" >&2
      return 1
    fi
  done

  local deadline=$((SECONDS + 300))
  while ((SECONDS < deadline)); do
    pending=()
    for service in "${required_services[@]}"; do
      ids="$(docker compose "${DEMO_COMPOSE_ARGS[@]}" ps --quiet "${service}" 2>/dev/null || true)"
      if [[ -z "${ids}" ]]; then
        pending+=("${service}:not-running")
        continue
      fi
      while IFS= read -r container_id; do
        [[ -n "${container_id}" ]] || continue
        state="$(docker inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container_id}" 2>/dev/null || true)"
        if [[ "${state}" != 'running|healthy' && "${state}" != 'running|none' ]]; then
          pending+=("${service}:${state:-inspect-failed}")
        fi
      done <<<"${ids}"
    done
    if ((${#pending[@]} == 0)); then
      printf 'Demo reset complete: all %s required services are running and healthy where health checks exist.\n' "${#required_services[@]}"
      return 0
    fi
    sleep 3
  done
  printf 'Demo did not recover after reset. Pending services: %s\n' "${pending[*]}" >&2
  return 1
}

if ! docker compose "${DEMO_COMPOSE_ARGS[@]}" ps --status running --services | grep -qx flagd; then
  printf 'The demo flagd service is not running. Run scripts/start-demo.sh first.\n' >&2
  exit 1
fi

original_variant="$(${python} "${SCRIPT_DIR}/demo_experiment.py" get-variant --file "${flag_file}" --flag "${scenario}")"
experiment_id="$(${python} -c 'import uuid; print(uuid.uuid4())')"
manifest_directory="${PROJECT_ROOT}/.runtime/experiments"
manifest_file="${manifest_directory}/${experiment_id}.json"
flag_backup_file="${manifest_directory}/${experiment_id}.demo.flagd.json.backup"
mkdir -p "${manifest_directory}"

commit="$(read_dotenv OTEL_DEMO_COMMIT 'dedc0178918e260823323b8d95005a8cb924b007')"
${python} "${SCRIPT_DIR}/demo_experiment.py" start-manifest \
  --file "${manifest_file}" \
  --experiment-id "${experiment_id}" \
  --commit "${commit}" \
  --profile "${profile}" \
  --seed "${seed}" \
  --split "${split}" \
  --scenario "${scenario}" \
  --variant "${variant}" \
  --original-variant "${original_variant}" \
  --warmup "${warmup}" \
  --duration "${duration}" \
  --recovery "${recovery}"

${python} "${SCRIPT_DIR}/demo_experiment.py" snapshot-file \
  --file "${flag_file}" \
  --backup "${flag_backup_file}"

restored=false
completed=false
reset_recorded=false
previous_run_id_set="${RCA_EXPERIMENT_RUN_ID+x}"
previous_run_id="${RCA_EXPERIMENT_RUN_ID-}"
export RCA_EXPERIMENT_RUN_ID="${experiment_id}"
platform_compose=(compose --file "${PROJECT_ROOT}/docker-compose.yml")
api_url="http://localhost:$(read_dotenv RCA_API_PORT 8000)"
cleanup() {
  local exit_code=$?
  local cleanup_failed=0
  trap - EXIT
  trap '' INT TERM

  # Always restore the original file byte-for-byte. A semantic JSON rewrite is
  # insufficient because it may alter line endings or Unix mode bits.
  if ! ${python} "${SCRIPT_DIR}/demo_experiment.py" restore-file \
      --file "${flag_file}" --backup "${flag_backup_file}"; then
    printf 'Cleanup failed: could not restore the exact feature-flag snapshot.\n' >&2
    cleanup_failed=1
  fi
  if [[ "${restored}" != true ]]; then
    if ! docker compose "${DEMO_COMPOSE_ARGS[@]}" restart flagd >/dev/null; then
      printf 'Cleanup failed: could not restart flagd after emergency restoration.\n' >&2
      cleanup_failed=1
    fi
  fi
  if [[ "${completed}" != true ]]; then
    if ! ${python} "${SCRIPT_DIR}/demo_experiment.py" mark-manifest \
        --file "${manifest_file}" --status aborted; then
      printf 'Cleanup failed: could not mark the experiment as aborted.\n' >&2
      cleanup_failed=1
    fi
    if [[ "${reset_recorded}" == true ]]; then
      if ! ${python} "${SCRIPT_DIR}/demo_experiment.py" sync-api \
          --file "${manifest_file}" --api-url "${api_url}"; then
        printf 'Cleanup failed: could not persist the aborted experiment state.\n' >&2
        cleanup_failed=1
      fi
    fi
  fi
  if [[ -n "${previous_run_id_set}" ]]; then
    export RCA_EXPERIMENT_RUN_ID="${previous_run_id}"
  else
    unset RCA_EXPERIMENT_RUN_ID
  fi
  if ! docker "${platform_compose[@]}" up --detach --force-recreate otel-gateway >/dev/null; then
    printf 'Cleanup failed: could not restore the gateway run-id configuration.\n' >&2
    cleanup_failed=1
  fi
  if ((cleanup_failed != 0 && exit_code == 0)); then
    exit_code=1
  fi
  if ((cleanup_failed == 0 && exit_code == 0)) && [[ "${completed}" == true ]]; then
    printf 'Experiment complete. Ground truth: %s\n' "${manifest_file}"
    printf 'Review the incident at http://localhost:%s/d/rca-overview\n' "$(read_dotenv GRAFANA_PORT 3000)"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'Resetting the complete Demo before capture seed %s...\n' "${seed}"
docker compose "${DEMO_COMPOSE_ARGS[@]}" restart
wait_demo_ready
${python} "${SCRIPT_DIR}/demo_experiment.py" mark-reset --file "${manifest_file}"
reset_recorded=true

docker "${platform_compose[@]}" up --detach --force-recreate otel-gateway >/dev/null
${python} "${SCRIPT_DIR}/demo_experiment.py" sync-api --file "${manifest_file}" --api-url "${api_url}"

printf 'Experiment %s: healthy warm-up for %ss...\n' "${experiment_id}" "${warmup}"
sleep "${warmup}"

# Record the intervention boundary before touching flagd so injected failures
# cannot predate their ground-truth interval.
${python} "${SCRIPT_DIR}/demo_experiment.py" mark-manifest --file "${manifest_file}" --status fault_active
${python} "${SCRIPT_DIR}/demo_experiment.py" sync-api --file "${manifest_file}" --api-url "${api_url}"
${python} "${SCRIPT_DIR}/demo_experiment.py" set-variant --file "${flag_file}" --flag "${scenario}" --variant "${variant}"
# flagd watches the mounted file. Restarting it creates a second, unrelated
# outage that can outrank the intended injected cause.
sleep 3
printf 'Injected %s=%s for %ss. Built-in Locust traffic remains active.\n' "${scenario}" "${variant}" "${duration}"
sleep "${duration}"

# The recovery boundary is the beginning of the restoration intervention.
${python} "${SCRIPT_DIR}/demo_experiment.py" mark-manifest --file "${manifest_file}" --status recovering
${python} "${SCRIPT_DIR}/demo_experiment.py" sync-api --file "${manifest_file}" --api-url "${api_url}"
${python} "${SCRIPT_DIR}/demo_experiment.py" set-variant --file "${flag_file}" --flag "${scenario}" --variant "${original_variant}"
sleep 3
restored=true
printf 'Restored %s=%s; observing recovery for %ss...\n' "${scenario}" "${original_variant}" "${recovery}"
sleep "${recovery}"

printf 'Validating persisted telemetry and top-%s root-cause ranking...\n' "${acceptance_top_k}"
${python} "${SCRIPT_DIR}/demo_experiment.py" accept-capture \
  --file "${manifest_file}" \
  --api-url "${api_url}" \
  --top-k "${acceptance_top_k}"
${python} "${SCRIPT_DIR}/demo_experiment.py" mark-manifest --file "${manifest_file}" --status completed
${python} "${SCRIPT_DIR}/demo_experiment.py" sync-api --file "${manifest_file}" --api-url "${api_url}"
completed=true
printf 'Experiment phases complete; restoring the exact pre-run state...\n'
