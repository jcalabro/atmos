#!/usr/bin/env bash
set -euo pipefail

atproto_sha=9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33
bulletin_sha=0acf237b872c766a60cec597c5fc10f0b1a58b7f
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=$(mktemp -d /tmp/atmos-spaces-interop.XXXXXX)
atproto_dir=${ATMOS_ATPROTO_CHECKOUT:-$run_root/atproto}
bulletin_dir=${ATMOS_BULLETIN_CHECKOUT:-$run_root/bulletin}
pds_pid=
pds_supervisor=
bulletin_pid=
bulletin_supervisor=
started_pid=
started_supervisor=

stop_service() {
  local pid=$1 supervisor=$2 name=$3
  if [[ -n "$pid" ]] && kill -0 -- "-$pid" 2>/dev/null; then
    kill -TERM -- "-$pid" 2>/dev/null || true
    for _ in $(seq 1 40); do
      if ! kill -0 -- "-$pid" 2>/dev/null; then break; fi
      sleep 0.25
    done
    if kill -0 -- "-$pid" 2>/dev/null; then
      printf '%s did not stop after 10 seconds; sending SIGKILL\n' "$name" >&2
      kill -KILL -- "-$pid" 2>/dev/null || true
    fi
  fi
  if [[ -n "$supervisor" ]]; then wait "$supervisor" 2>/dev/null || true; fi
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  stop_service "$bulletin_pid" "$bulletin_supervisor" Bulletin
  stop_service "$pds_pid" "$pds_supervisor" 'atproto multi-PDS network'
  if [[ -z "${ATMOS_KEEP_INTEROP_ROOT:-}" ]]; then rm -rf -- "$run_root"; else printf 'interop artifacts: %s\n' "$run_root"; fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

checkout() {
  local repository=$1 destination=$2 sha=$3
  if [[ ! -d "$destination/.git" ]]; then
    git clone --filter=blob:none --no-checkout "$repository" "$destination"
    git -C "$destination" fetch --depth=1 origin "$sha"
    git -C "$destination" checkout --detach FETCH_HEAD
  fi
  local actual
  actual=$(git -C "$destination" rev-parse HEAD)
  if [[ "$actual" != "$sha" ]]; then
    printf 'wrong checkout at %s: got %s, require %s\n' "$destination" "$actual" "$sha" >&2
    exit 1
  fi
  if [[ -n "$(git -C "$destination" status --porcelain --untracked-files=no)" ]]; then
    printf 'tracked files are modified at %s\n' "$destination" >&2
    exit 1
  fi
}

wait_http() {
  local url=$1 pid=$2 name=$3
  for _ in $(seq 1 120); do
    if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then
      printf '%s exited during startup\n' "$name" >&2
      return 1
    fi
    sleep 0.25
  done
  printf 'timed out waiting for %s at %s\n' "$name" "$url" >&2
  return 1
}

start_service() {
  local working_directory=$1 log=$2 pid_file=$3
  shift 3
  started_pid=
  setsid --fork --wait bash -c '
    pid_file=$1
    working_directory=$2
    shift 2
    printf "%s\n" "$$" >"$pid_file"
    cd "$working_directory"
    exec "$@"
  ' bash "$pid_file" "$working_directory" "$@" >"$log" 2>&1 &
  started_supervisor=$!

  for _ in $(seq 1 100); do
    if [[ -s "$pid_file" ]]; then
      read -r started_pid <"$pid_file"
      break
    fi
    if ! kill -0 "$started_supervisor" 2>/dev/null; then
      wait "$started_supervisor" 2>/dev/null || true
      printf 'service supervisor exited before publishing its process group\n' >&2
      return 1
    fi
    sleep 0.05
  done
  if [[ ! "$started_pid" =~ ^[1-9][0-9]*$ ]]; then
    kill "$started_supervisor" 2>/dev/null || true
    wait "$started_supervisor" 2>/dev/null || true
    printf 'service did not publish a valid process-group ID\n' >&2
    return 1
  fi
  local actual_group
  if ! actual_group=$(ps -o pgid= -p "$started_pid" | tr -d '[:space:]'); then
    kill -TERM -- "-$started_pid" 2>/dev/null || true
    wait "$started_supervisor" 2>/dev/null || true
    printf 'service exited before its process group could be validated\n' >&2
    return 1
  fi
  if [[ "$actual_group" != "$started_pid" ]]; then
    kill -TERM "$started_pid" 2>/dev/null || true
    kill "$started_supervisor" 2>/dev/null || true
    wait "$started_supervisor" 2>/dev/null || true
    printf 'service process %s is in unexpected process group %s\n' "$started_pid" "$actual_group" >&2
    return 1
  fi
}

checkout https://github.com/bluesky-social/atproto.git "$atproto_dir" "$atproto_sha"
checkout https://github.com/bluesky-social/bulletin.git "$bulletin_dir" "$bulletin_sha"

for command in setsid ps; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf '%s is required to manage integration-test service process groups\n' "$command" >&2
    exit 1
  fi
done

(cd "$atproto_dir" && pnpm install --frozen-lockfile)
(cd "$atproto_dir" && pnpm --filter @atproto/dev-env... build)
(cd "$atproto_dir" && pnpm --filter @atproto/pds test:sqlite-only --runInBand tests/space)
(cd "$bulletin_dir" && pnpm install --frozen-lockfile)
(cd "$bulletin_dir" && pnpm codegen:lex)
(cd "$bulletin_dir" && pnpm check)

start_service "$atproto_dir" "$run_root/atproto.log" "$run_root/atproto.pid" pnpm --filter @atproto/dev-env start:multi-pds 2
pds_pid=$started_pid
pds_supervisor=$started_supervisor
wait_http http://localhost:2581/ "$pds_pid" 'atproto multi-PDS network'
start_service "$bulletin_dir" "$run_root/bulletin.log" "$run_root/bulletin.pid" pnpm dev:local
bulletin_pid=$started_pid
bulletin_supervisor=$started_supervisor
wait_http http://127.0.0.1:3001/health "$bulletin_pid" Bulletin

ATMOS_ATPROTO_INTROSPECT_URL=http://localhost:2581 \
ATMOS_BULLETIN_URL=http://127.0.0.1:3001 \
  go test -count=1 -run '^TestPinned(ReferenceStack|BulletinNotificationAuth)$' "$repo_root/space/client"
