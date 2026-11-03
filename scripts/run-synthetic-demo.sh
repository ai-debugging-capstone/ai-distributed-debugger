#!/usr/bin/env bash

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

scenario=database_lock
seed=42
severity=1.25
timeout=120

while (($#)); do
  case "$1" in
    --scenario) scenario="$2"; shift ;;
    --seed) seed="$2"; shift ;;
    --severity) severity="$2"; shift ;;
    --timeout) timeout="$2"; shift ;;
    -h|--help)
      printf 'Usage: %s [--scenario database_lock] [--seed 42] [--severity 1.25] [--timeout 120]\n' "$0"
      exit 0
      ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done

python="$(python_command)"
api_port="$(read_dotenv RCA_API_PORT 8000)"
cd "${PROJECT_ROOT}"
exec "${python}" "${SCRIPT_DIR}/synthetic_smoke.py" \
  --api-url "http://localhost:${api_port}" \
  --scenario "${scenario}" \
  --seed "${seed}" \
  --severity "${severity}" \
  --incident-timeout "${timeout}"
