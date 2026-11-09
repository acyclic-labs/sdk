#!/usr/bin/env bash
# A real Secret Service, isolated from the build host's credentials. The D-Bus
# name event admits readiness; daemon exit races that event without polling.
set -euo pipefail
if [[ ${1:-} != --session ]]; then
  exec dbus-run-session -- bash "$0" --session "$@"
fi
shift
if [[ $# != 3 ]]; then
  echo 'usage: keychain-session.sh GRAPHCODER MOCK_MODEL CREDENTIAL_FIXTURE' >&2
  exit 1
fi
graphcoder=$1
mock=$2
export E2E_CREDENTIALS_FIXTURE=$3
profile=$(mktemp -d /tmp/graphcoder-keychain.XXXXXXXX)
daemon_pid=
ready_pid=
cleanup() {
  if [[ -n $ready_pid ]]; then kill "$ready_pid" 2>/dev/null || true; wait "$ready_pid" 2>/dev/null || true; fi
  if [[ -n $daemon_pid ]]; then kill "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true; fi
  rm -rf -- "$profile"
}
trap cleanup EXIT
export HOME=$profile
export XDG_DATA_HOME=$profile/data XDG_CONFIG_HOME=$profile/config XDG_CACHE_HOME=$profile/cache
export XDG_RUNTIME_DIR=$profile/runtime
mkdir -m 700 "$XDG_RUNTIME_DIR"
daemon=${E2E_KEYCHAIN_DAEMON:-gnome-keyring-daemon}
"$daemon" --foreground --components=secrets --unlock </dev/null >"$profile/daemon.log" 2>&1 &
daemon_pid=$!
gdbus wait --session org.freedesktop.secrets &
ready_pid=$!
completed=
if ! wait -n -p completed "$daemon_pid" "$ready_pid"; then
  echo 'Secret Service failed before readiness; install an unlocked native credential service' >&2
  cat "$profile/daemon.log" >&2
  exit 1
fi
if [[ $completed == "$daemon_pid" ]]; then
  echo 'Secret Service exited before readiness' >&2
  cat "$profile/daemon.log" >&2
  exit 1
fi
ready_pid=
node scripts/graphcoder-e2e/run.mjs "$graphcoder" "$mock" doctor-all-missing doctor-ok pty-first-run
