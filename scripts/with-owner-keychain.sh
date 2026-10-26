#!/usr/bin/env bash
# A real isolated Secret Service session for native authority fixtures.
# Never uses the logged-in user's collection, HOME or session bus.
set -Eeuo pipefail
step=arguments
original=("$@")
refuse() {
  local result=$1
  printf 'owner-keychain fixture step=%s rc=%s; recovery:' "$step" "$result" >&2
  printf ' %q' bash "$0" "${original[@]}" >&2
  printf '\n' >&2
  exit "$result"
}
trap 'refuse $?' ERR
fail() {
  step=$1
  printf 'owner-keychain fixture: %s\n' "$2" >&2
  refuse 2
}
if [[ ${1:-} == --session ]]; then
  [[ $# -ge 3 ]] || fail arguments 'requires --session <directory> <command> [arguments...]'
  fixture=$2; shift 2
  original=("$@")
  step=prepare-session
  export HOME="$fixture/home" XDG_DATA_HOME="$fixture/data" XDG_CONFIG_HOME="$fixture/config"
  export XDG_RUNTIME_DIR="$fixture/runtime"
  export GNOME_KEYRING_CONTROL="$fixture/control"
  mkdir -m 700 -p "$HOME" "$XDG_DATA_HOME" "$XDG_CONFIG_HOME" "$XDG_RUNTIME_DIR"
  password=$(head -c 32 /dev/urandom | base64)
  if ! printf '%s' "$password" | gnome-keyring-daemon --unlock --components=secrets \
      --control-directory="$fixture/control" >"$fixture/startup.log" 2>&1; then
    unset password
    fail start-service 'install gnome-keyring, then run scripts/with-owner-keychain.sh <original command>'
  fi
  unset password
  if ! gnome-keyring-daemon --start --components=secrets >"$fixture/ready.log" 2>&1; then
    fail register-service 'run scripts/with-owner-keychain.sh <original command> with a working Secret Service installation'
  fi
  cat "$fixture/startup.log" "$fixture/ready.log"
  step=command
  "$@"
  exit
fi
[[ $# -gt 0 ]] || fail arguments 'scripts/with-owner-keychain.sh <command> [arguments...]'
[[ $(uname -s) == Linux ]] || exec "$@"
command -v dbus-run-session >/dev/null || fail missing-dbus-run-session \
  'install dbus in the build image, then run scripts/with-owner-keychain.sh <original command>'
# Keep toolchain lookup independent of the fixture HOME.
export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}" RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
step=create-private-directory
fixture=$(mktemp -d "${TMPDIR:-/tmp}/acyclic-owner-keychain.XXXXXXXX") || \
  fail create-private-directory 'free space in the job scratch directory and rerun the original command'
trap 'rm -rf -- "$fixture"' EXIT
if ! command -v gnome-keyring-daemon >/dev/null; then
  # The build image is Ubuntu. Extract the actual distribution packages into
  # this job's private fixture; never install packages or write a user's HOME.
  for command in apt-get dpkg-deb; do
    command -v "$command" >/dev/null || fail "missing-$command" \
      'install gnome-keyring in the build image, then run scripts/with-owner-keychain.sh <original command>'
  done
  mkdir -m 700 "$fixture/packages"
  if ! (cd "$fixture/packages" &&
      apt-get download gnome-keyring libgcr-base-3-1 libgck-1-0 &&
      for package in *.deb; do dpkg-deb -x "$package" root || exit; done); then
    fail prepare-service-packages 'install gnome-keyring in the build image, then run scripts/with-owner-keychain.sh <original command>'
  fi
  export PATH="$fixture/packages/root/usr/bin:$PATH"
  export LD_LIBRARY_PATH="$fixture/packages/root/usr/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH):${LD_LIBRARY_PATH:-}"
fi
step=isolated-session
if dbus-run-session -- bash "$0" --session "$fixture" "$@"; then
  exit 0
else
  result=$?
  refuse "$result"
fi
