#!/bin/sh
# The run wrapper of every privileged isolated_test target: `privileged.sh <binary> <arguments>` runs one case in its
# own user and network namespace, holding only the privileges ACYCLIC_TEST_PRIVILEGES names (the target's
# `requires-<privilege>` tags). It never gains real host root and never leaves the sandbox: a privilege the host
# cannot grant fails the case, so no case is skipped.
#   root            uid 0 inside the user namespace, with that namespace's capabilities
#   net-admin, nft  CAP_NET_ADMIN inside the namespace (nftables needs nothing more)
#   seccomp         CAP_SYS_ADMIN inside the namespace, which installs a filter without no_new_privs
#   user-namespace  nothing beyond the namespace: the case creates its own nested namespaces
#   cgroup          a scope of the caller's systemd user manager (addressed through the host bus, so it works
#                   from a private PID namespace) delegating its subtree: the case runs in a leaf, and ACYCLIC_TEST_CGROUP
#                   names its sibling parent, with every delegated controller enabled for children
#   fs-verity       the verity-capable directory the leg provisions, ACYCLIC_TEST_VERITY_DIR. Its cases check that a
#                   fixture is owned by host root, which no user namespace can show: linux-sandbox maps only the
#                   caller's uid, so root:root reads as the overflow id. So fs-verity is a target's only privilege, and
#                   isolated_test runs it outside linux-sandbox (no-sandbox), with no namespace from this wrapper
#                   either: the case runs as the leg's own host user, with that user's identity view and no
#                   capability. It gains nothing a plain process of that user lacks. It loses net isolation (the
#                   host's network namespace, not a private loopback) and mount isolation (it sees and can write
#                   whatever that user can, not only its sandbox's writable paths).
#   kvm, local-cell granted outside the wrapper: the host's /dev/kvm ACL, and the local-cell leg's cell
set -eu
fail() { echo "privileged.sh: $*" >&2; exit 1; }
has() { case " $ACYCLIC_TEST_PRIVILEGES " in *" $1 "*) return 0 ;; esac; return 1; }
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
    [ "$ACYCLIC_TEST_PRIVILEGES" = fs-verity ] || fail "fs-verity is a target's only privilege, not with: $ACYCLIC_TEST_PRIVILEGES"
    # Owners read true only in the initial user namespace: refuse any other view instead of admitting by it.
    [ "$(tr -s ' ' </proc/self/uid_map)" = " 0 0 4294967295" ] ||
      fail "fs-verity: the case runs in a user namespace (uid_map $(tr -s ' ' </proc/self/uid_map)), where host root reads as overflow"
    exec "$@"
  fi
  if has cgroup; then
    # A test action starts with a clean environment; the user manager's bus lives in its runtime directory.
    XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)} ACYCLIC_TEST_PRIVILEGE_STAGE=scope exec systemd-run --user --machine="$(id -un)@.host" --scope --quiet --collect -p Delegate=yes -- /bin/sh "$0" "$@"
  fi
  stage=scope
fi
if [ "$stage" = scope ]; then
  if has cgroup; then
    scope=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
    # A cgroup that enables controllers for its children holds no process itself: the case runs in the `case` leaf,
    # and its delegated parent is the sibling `work`, with every delegated controller enabled for its children.
    controllers=$(sed 's/[^ ][^ ]*/+&/g' "$scope/cgroup.controllers")
    { mkdir "$scope/case" && echo $$ >"$scope/case/cgroup.procs" && echo "$controllers" >"$scope/cgroup.subtree_control" &&
      mkdir "$scope/work" && echo "$controllers" >"$scope/work/cgroup.subtree_control"; } ||
      fail "cgroup: the delegated scope $scope refused its case leaf or work parent"
    export ACYCLIC_TEST_CGROUP="$scope/work"
  fi
  if has root; then set -- --map-root-user -- /bin/sh "$0" "$@"; else set -- --map-current-user --keep-caps -- /bin/sh "$0" "$@"; fi
  ACYCLIC_TEST_PRIVILEGE_STAGE=namespace exec unshare --user --net "$@"
fi
# The namespace's loopback is down until a holder of CAP_NET_ADMIN raises it.
PATH=$PATH:/usr/sbin:/sbin ip link set lo up || fail "the namespace's loopback did not come up"
if has root; then exec "$@"; fi
caps=-all
if has net-admin || has nft; then caps=$caps,+net_admin; fi
if has seccomp; then caps=$caps,+sys_admin; fi
exec setpriv --inh-caps="$caps" --ambient-caps="$caps" --bounding-set="$caps" -- "$@"
