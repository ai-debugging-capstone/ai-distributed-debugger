#!/usr/bin/env bash

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_command git

repository="$(read_dotenv OTEL_DEMO_REPOSITORY 'https://github.com/open-telemetry/opentelemetry-demo.git')"
commit="$(read_dotenv OTEL_DEMO_COMMIT 'dedc0178918e260823323b8d95005a8cb924b007')"
directory="$(demo_directory)"

mkdir -p "$(dirname -- "${directory}")"

if [[ -e "${directory}" && ! -d "${directory}/.git" ]]; then
  printf 'Refusing to overwrite non-Git path: %s\n' "${directory}" >&2
  exit 1
fi

if [[ ! -d "${directory}/.git" ]]; then
  printf 'Cloning OpenTelemetry Demo into %s\n' "${directory}"
  git clone --filter=blob:none --no-checkout "${repository}" "${directory}"
else
  actual_origin="$(git -C "${directory}" remote get-url origin)"
  if [[ "${actual_origin%.git}" != "${repository%.git}" ]]; then
    printf 'Existing checkout has unexpected origin: %s\n' "${actual_origin}" >&2
    exit 1
  fi
  if [[ -n "$(git -C "${directory}" status --porcelain)" ]]; then
    printf 'The demo checkout has local changes; refusing to overwrite them: %s\n' "${directory}" >&2
    exit 1
  fi
fi

git -C "${directory}" fetch --depth=1 origin "${commit}"
git -C "${directory}" checkout --detach "${commit}"

actual_commit="$(git -C "${directory}" rev-parse HEAD)"
if [[ "${actual_commit}" != "${commit}" ]]; then
  printf 'Pin verification failed: expected %s, found %s\n' "${commit}" "${actual_commit}" >&2
  exit 1
fi

printf 'OpenTelemetry Demo is pinned at %s\n' "${actual_commit}"
