#!/bin/bash
# bb2 fresh-host bootstrap, phase 2 (root, run by relaunch.py over ssh):
#   host-install.sh SCRIPTS_BUILDBOX_DIR TOOLS_SNAPSHOT_TAR [HOOK_DIR]
# Installs the tracked host plumbing (host/), the janitor, and the exact live
# tool generation captured by `relaunch.py snapshot`, restored whole by one
# rename before any job can run. HOOK_DIR/*.sh (e.g. Cloud's Docker data-root
# and prepull) run last, as root.
set -euo pipefail
src=$(realpath "$1") snapshot=$2 hooks=${3:-}
[ "$(id -u)" = 0 ] || { echo "host-install: run as root"; exit 1; }
[ -e /var/lib/buildbox/bootstrap.done ] || { echo "host-install: phase 1 has not finished"; exit 1; }
cd "$src/host"
find . -type f | sort | while read -r f; do
  case $f in ./usr/local/*bin/*) mode=0755;; ./etc/sudoers.d/*) mode=0440;; *) mode=0644;; esac
  install -D -m "$mode" -o root -g root "$f" "/${f#./}"
done
visudo -cq
sysctl -q --system
if [ -f "$src/bb-janitor.py" ]; then
  install -D -m 0755 -o root -g root "$src/bb-janitor.py" /usr/local/lib/buildbox/bb-janitor.py
  install -m 0644 -o root -g root "$src/systemd/bb-janitor.service" "$src/systemd/bb-janitor.timer" /etc/systemd/system/
fi
systemctl daemon-reload
systemctl enable -q bb-nvme.service
systemctl enable -q --now bbjobs.slice bbsync.slice bb-sshd-watchdog.timer bb-target-prune.timer
[ ! -f /etc/systemd/system/bb-janitor.timer ] || systemctl enable -q --now bb-janitor.timer

# Tool generation: verify every manifest hash, then expose the store by one rename.
if [ ! -e /opt/buildbox/bin/installed-tools.json ]; then
  install -d -m 0755 -o root -g root /opt/buildbox
  staged=$(mktemp -d /opt/buildbox/.restore-XXXXXX)
  tar -C "$staged" -xf "$snapshot"
  python3 -B - "$staged" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1]);manifest=json.loads((root/'installed-tools.json').read_text())['sha256']
bad=[n for n,h in manifest.items() if h is not None and (not (root/n).is_file() or hashlib.sha256((root/n).read_bytes()).hexdigest()!=h)]
if bad:raise SystemExit('host-install: snapshot does not match its manifest: '+' '.join(bad))
PY
  chown -R root:root "$staged"; find "$staged" -type d -exec chmod 0755 {} +; find "$staged" -type f -exec chmod 0555 {} +
  chmod 0755 "$staged"; mv -T "$staged" /opt/buildbox/bin
fi
install -d -m 0755 -o root -g root /home/var/bin
systemctl enable -q --now home-var-bin.mount
mount -o remount,bind,ro /home/var/bin
[ "$(stat -c %d:%i /home/var/bin)" = "$(stat -c %d:%i /opt/buildbox/bin)" ] || { echo "host-install: /home/var/bin is not the store"; exit 1; }

if [ -n "$hooks" ] && [ -d "$hooks" ]; then
  for hook in "$hooks"/*.sh; do [ -e "$hook" ] || continue; echo "host-install: hook ${hook##*/}"; bash "$hook"; done
fi
touch /var/lib/buildbox/host-install.done
echo "host-install: done"
