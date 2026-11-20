#!/bin/bash
# The run wrapper of every privileged isolated_test target: `privileged.sh <binary> <arguments>` runs one case holding
# only the privileges ACYCLIC_TEST_PRIVILEGES names (the target's `requires-<privilege>` tags). A privilege the host
# cannot grant fails the case, so no case is skipped. It is bash, never sh: dash drops every variable whose name is no
# shell identifier (Cargo's CARGO_BIN_EXE_<name> of a hyphenated binary), and bash, sudo through `env -i`, systemd-run,
# unshare and setpriv hand the case every variable, whatever its name.
# Each privilege but fs-verity, machines-native and host-root runs the case in its own user and network namespace, inside the
# sandbox and never as real host root:
#   root            uid 0 inside the user namespace, with that namespace's capabilities
#   net-admin, nft  CAP_NET_ADMIN inside the namespace (nftables needs nothing more)
#   seccomp         CAP_SYS_ADMIN inside the namespace, which installs a filter without no_new_privs
#   user-namespace  nothing beyond the namespace: the case creates its own nested namespaces
#   cgroup          a scope of the caller's systemd user manager (addressed through the host bus, so it works
#                   from a private PID namespace) delegating its subtree: the case runs in a leaf, and ACYCLIC_TEST_CGROUP
#                   names its sibling parent, with every delegated controller enabled for children
#   kvm, local-cell granted outside the wrapper: the host's /dev/kvm ACL, and the local-cell leg's cell
# fs-verity, machines-native and host-root check owners as host root sees them, which no user namespace can show: linux-sandbox
# maps only the caller's uid, so root:root reads as the overflow id. So each is a target's only privilege, and
# isolated_test runs it outside linux-sandbox (no-sandbox):
#   fs-verity       a fresh case directory of the verity-capable one the leg provisions, as ACYCLIC_TEST_VERITY_DIR. The
#                   case runs as the leg's own host user with no namespace and no capability: it gains nothing a plain
#                   process of that user lacks, and loses net isolation (the host's network namespace, not a private
#                   loopback) and mount isolation (it sees and can write whatever that user can).
#   machines-native real host root, through the leg user's sudo: the build box's jobs already hold passwordless root,
#                   and nothing sensitive is on it. The case runs as uid 0 of the initial user namespace, in its own
#                   network namespace (loopback up) and mount namespace, in a leaf of a delegated system scope
#                   (ACYCLIC_TEST_CGROUP, as for cgroup), with exactly CAP_SETUID, CAP_SETGID and CAP_NET_ADMIN
#                   effective. Its securebits are noroot, so an executable's file capabilities still apply. Each line
#                   `<variable> <binary> [<uid>:<gid> <mode> [<file capabilities>]]` of ACYCLIC_TEST_SEALED (by default
#                   0:0 0555, no capabilities) names one of the target's CARGO_BIN_EXE_<binary>: the case's <variable>
#                   is a root-owned fs-verity-sealed copy of it in a fresh case directory under
#                   ACYCLIC_TEST_VERITY_FIXTURES/sealed, whose every ancestor is root's and unwritable to others.
#   host-root       machines-native with every capability, for a case that also needs host root's other powers
#                   (CAP_CHOWN to give a directory to another uid).
# A case directory is the case's for its whole life: it holds an flock on it, which the kernel releases however the case
# ends, and each new case first removes every sibling no live case holds. So a case killed at its limit, or a runner
# killed with it, leaves nothing past the next case.
set -eu
fail() { echo "privileged.sh: $*" >&2; exit 1; }
has() { case " $ACYCLIC_TEST_PRIVILEGES " in *" $1 "*) return 0 ;; esac; return 1; }
# Whether the case runs as real host root (machines-native, host-root).
native() { has machines-native || has host-root; }
# Claims a fresh case directory under $1 as $claimed, locked on descriptor 9, which every exec to the case inherits.
claim() {
  { mkdir -p -m 0755 "$1" && exec 8>>"$1/.lock" && flock 8; } || fail "cannot lock $1"
  for held in "$1"/case-*; do
    if [ -d "$held" ] && flock -n "$held" true; then
      { chmod -R u+rwX "$held" && rm -rf "$held"; } || fail "cannot remove the abandoned $held"
    fi
  done
  { claimed=$(mktemp -d "$1/case-XXXXXXXX") && exec 9<"$claimed" && flock -n 9; } || fail "cannot claim a case directory in $1"
  exec 8>&-
}
stage=${ACYCLIC_TEST_PRIVILEGE_STAGE:-}
unset ACYCLIC_TEST_PRIVILEGE_STAGE
if [ -z "$stage" ]; then
  # The one list of privileges, which the generator reads too.
  known=$(dirname -- "$0")/privileges.txt
  for privilege in $ACYCLIC_TEST_PRIVILEGES; do
    grep -qx -- "$privilege" "$known" || fail "unknown privilege $privilege (not in $known)"
  done
  if has fs-verity; then
    [ -d "${ACYCLIC_TEST_VERITY_DIR:-}" ] || fail "fs-verity: the leg provisions no ACYCLIC_TEST_VERITY_DIR"
    # Owners read true only in the initial user namespace: refuse any other view instead of admitting by it.
    [ "$(tr -s ' ' </proc/self/uid_map)" = " 0 0 4294967295" ] ||
      fail "fs-verity: the case runs in a user namespace (uid_map $(tr -s ' ' </proc/self/uid_map)), where host root reads as overflow"
    claim "$ACYCLIC_TEST_VERITY_DIR"
    ACYCLIC_TEST_VERITY_DIR=$claimed exec "$@"
  fi
  if native; then
    [ -d "${ACYCLIC_TEST_VERITY_FIXTURES:-}" ] || fail "$ACYCLIC_TEST_PRIVILEGES: the leg provisions no ACYCLIC_TEST_VERITY_FIXTURES"
    # sudo resets the environment; `env -i` restores exactly this one.
    mapfile -d '' environment < <(env -0)
    exec sudo -n -- env -i -- "${environment[@]}" ACYCLIC_TEST_PRIVILEGE_STAGE=root /bin/bash "$0" "$@"
  fi
  if has cgroup; then
    # A test action starts with a clean environment; the user manager's bus lives in its runtime directory.
    XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)} ACYCLIC_TEST_PRIVILEGE_STAGE=scope exec systemd-run --user --machine="$(id -un)@.host" --scope --quiet --collect -p Delegate=yes -- /bin/bash "$0" "$@"
  fi
  stage=scope
fi
if [ "$stage" = root ]; then
  claim "$ACYCLIC_TEST_VERITY_FIXTURES/sealed"
  chmod 0755 "$claimed"
  while read -r variable binary owner mode capabilities; do
    [ -n "$variable" ] || continue
    source=$(printenv "CARGO_BIN_EXE_$binary") || fail "machines-native: ACYCLIC_TEST_SEALED names $binary, no CARGO_BIN_EXE_ of this target"
    sealed=$claimed/$binary
    { cp -- "$source" "$sealed" && chown -- "${owner:-0:0}" "$sealed" && chmod -- "${mode:-0555}" "$sealed"; } ||
      fail "machines-native: cannot copy $binary into $claimed"
    # chown clears file capabilities, so they are set after it; fs-verity seals last.
    if [ -n "$capabilities" ]; then setcap "$capabilities" "$sealed" || fail "machines-native: setcap $capabilities $sealed"; fi
    fsverity enable "$sealed" || fail "machines-native: cannot seal $sealed (is $claimed on an fs-verity filesystem?)"
    export "$variable=$sealed"
  done <<<"${ACYCLIC_TEST_SEALED:-}"
  unset ACYCLIC_TEST_SEALED
  ACYCLIC_TEST_PRIVILEGE_STAGE=scope exec systemd-run --scope --quiet --collect -p Delegate=yes -- /bin/bash "$0" "$@"
fi
if [ "$stage" = scope ]; then
  if has cgroup || native; then
    scope=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
    # A cgroup that enables controllers for its children holds no process itself: the case runs in the `case` leaf,
    # and its delegated parent is the sibling `work`, with every delegated controller enabled for its children.
    controllers=$(sed 's/[^ ][^ ]*/+&/g' "$scope/cgroup.controllers")
    { mkdir "$scope/case" && echo $$ >"$scope/case/cgroup.procs" && echo "$controllers" >"$scope/cgroup.subtree_control" &&
      mkdir "$scope/work" && echo "$controllers" >"$scope/work/cgroup.subtree_control"; } ||
      fail "cgroup: the delegated scope $scope refused its case leaf or work parent"
    export ACYCLIC_TEST_CGROUP="$scope/work"
  fi
  if native; then
    set -- --mount -- /bin/bash "$0" "$@"
  elif has root; then
    set -- --user --map-root-user -- /bin/bash "$0" "$@"
  else
    set -- --user --map-current-user --keep-caps -- /bin/bash "$0" "$@"
  fi
  ACYCLIC_TEST_PRIVILEGE_STAGE=namespace exec unshare --net "$@"
fi
# The namespace's loopback is down until a holder of CAP_NET_ADMIN raises it.
PATH=$PATH:/usr/sbin:/sbin ip link set lo up || fail "the namespace's loopback did not come up"
if has root || has host-root; then exec "$@"; fi
if has machines-native; then
  caps=-all,+setuid,+setgid,+net_admin
  exec setpriv --securebits=+noroot,+noroot_locked --inh-caps="$caps" --ambient-caps="$caps" -- "$@"
fi
caps=-all
if has net-admin || has nft; then caps=$caps,+net_admin; fi
if has seccomp; then caps=$caps,+sys_admin; fi
exec setpriv --inh-caps="$caps" --ambient-caps="$caps" --bounding-set="$caps" -- "$@"
