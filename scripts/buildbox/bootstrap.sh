#!/bin/bash
# bb2 fresh-host bootstrap, phase 1: EC2 user-data on a stock Ubuntu 24.04 AMI.
# relaunch.sh substitutes the @...@ placeholders. Public sources only: the
# private SDK checkout, host plumbing and the tool generation arrive in phase 2
# (host-install.sh over ssh), so nothing here needs a repository credential.
# Done marker: /var/lib/buildbox/bootstrap.done (relaunch.sh waits for it).
set -euo pipefail
exec > >(tee -a /var/log/bb-bootstrap.log) 2>&1
echo "bb-bootstrap phase 1 start $(date -u +%FT%TZ)"
export DEBIAN_FRONTEND=noninteractive
mkdir -p /var/lib/buildbox

# Fixed host identity carried by the client, so HostKeyAlias bb2 stays valid on
# every relaunch without trusting a first-seen key.
umask 077
base64 -d > /etc/ssh/ssh_host_ed25519_key <<'KEY'
@HOST_KEY_B64@
KEY
ssh-keygen -y -f /etc/ssh/ssh_host_ed25519_key > /etc/ssh/ssh_host_ed25519_key.pub
umask 022
rm -f /etc/ssh/ssh_host_rsa_key* /etc/ssh/ssh_host_ecdsa_key*
cat > /etc/ssh/sshd_config.d/90-bb.conf <<'EOF'
HostKey /etc/ssh/ssh_host_ed25519_key
PasswordAuthentication no
MaxSessions 512
MaxStartups 200:30:400
ClientAliveInterval 30
ClientAliveCountMax 40
TCPKeepAlive yes
EOF

# Job account (uid 1000, as on every earlier box) with passwordless sudo.
id var >/dev/null 2>&1 || useradd -m -u 1000 -s /bin/bash var
install -d -m 700 -o var -g var /home/var/.ssh
printf '%s\n' '@AUTHORIZED_KEY@' > /home/var/.ssh/authorized_keys
chown var:var /home/var/.ssh/authorized_keys; chmod 600 /home/var/.ssh/authorized_keys
echo 'var ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/90-var; chmod 440 /etc/sudoers.d/90-var
systemctl restart ssh

# Data disk: the one non-root EBS volume becomes xfs LABEL=bbdata at /mnt/bb.
root=$(lsblk -no PKNAME "$(findmnt -no SOURCE /)")
data=$(lsblk -dno NAME,TYPE | awk -v r="$root" '$2=="disk" && $1!=r && $1 ~ /^nvme/ {print $1; exit}')
[ -n "$data" ] || { echo "bb-bootstrap: no data disk"; exit 1; }
apt-get update -q
apt-get install -y -q xfsprogs
blkid -L bbdata >/dev/null || mkfs.xfs -f -L bbdata "/dev/$data"
grep -q 'LABEL=bbdata' /etc/fstab || echo 'LABEL=bbdata /mnt/bb xfs defaults,noatime,nofail 0 2' >> /etc/fstab
mkdir -p /mnt/bb; mountpoint -q /mnt/bb || mount /mnt/bb
mkdir -p /mnt/bb/src /mnt/bb/targets /mnt/bb/sccache /mnt/bb/tmp /mnt/bb/locks /mnt/bb/jobs /mnt/bb/admit /mnt/bb/log /mnt/bb/toolchains
chown var:var /mnt/bb /mnt/bb/*; chmod 1777 /mnt/bb/tmp

# Packages (the box's manual set, minus desktop/VM extras and Cloud's Docker setup).
apt-get install -y -q build-essential clang clang-tools-18 libclang-dev lld mold musl-tools mingw-w64 \
  gcc-mingw-w64-x86-64 gcc-aarch64-linux-gnu pkg-config libssl-dev libelf-dev libseccomp-dev libfuse3-dev fuse3 \
  protobuf-compiler cmake bison flex git rsync jq pigz zstd unzip squashfs-tools mdadm nvme-cli keyutils \
  bubblewrap uidmap python3 python3-jsonschema shellcheck sysstat gdb heaptrack qemu-system-x86 qemu-utils ovmf \
  postgresql postgresql-contrib chrony acl curl ca-certificates gnupg
# Docker engine packages only; its data-root and prepull are Cloud's phase-2 hook.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" > /etc/apt/sources.list.d/docker.list
apt-get update -q && apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
usermod -aG docker var

# Toolchains as var: pinned versions mirror the box of 2026-10-27.
sudo -u var -H bash -euo pipefail <<'TOOLS'
cd ~
curl -fsSL https://sh.rustup.rs | sh -s -- -y -q --profile minimal --default-toolchain stable
. ~/.cargo/env
rustup toolchain install 1.98.1 --profile minimal -c clippy -c rustfmt -c llvm-tools \
  -t aarch64-apple-darwin -t aarch64-unknown-linux-gnu -t wasm32-unknown-unknown -t x86_64-pc-windows-gnu \
  -t x86_64-pc-windows-msvc -t x86_64-unknown-linux-musl
rustup toolchain install nightly-2026-07-01 --profile minimal -c llvm-tools -c miri -c rust-src -c rustc-dev \
  -c rustc-codegen-cranelift -t aarch64-apple-darwin -t wasm32-unknown-unknown -t x86_64-pc-windows-gnu
rustup toolchain install nightly-2026-08-21 --profile minimal
rustup toolchain install nightly-2026-10-12 --profile minimal -c clippy -c rustfmt -c rust-src \
  -c rustc-codegen-cranelift -t x86_64-pc-windows-msvc
rustup toolchain install nightly --profile minimal
for spec in sccache@0.18.0 cargo-deny@0.20.2 cargo-fuzz@0.13.2 cargo-xwin@0.23.1 cargo-zigbuild@0.23.4 \
            cargo-nextest@0.9.146 kani-verifier@0.68.0 wasm-bindgen-cli@0.2.117 cargo-llvm-cov; do
  cargo +1.98.1 install --locked -q "$spec"
done
cargo kani setup
mkdir -p ~/tools/node24 ~/tools/zig
curl -fsSL https://nodejs.org/dist/v24.3.0/node-v24.3.0-linux-x64.tar.xz | tar -C ~/tools/node24 -xJ
curl -fsSL https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz | tar -C ~/tools/zig -xJ
ln -sf ~/tools/zig/zig-x86_64-linux-0.16.0/zig ~/.cargo/bin/zig
curl -fsSL https://bun.sh/install | bash -s bun-v1.4.2
curl -fsSL https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz | tar -C ~/.cargo/bin -xz actionlint
PATH=~/tools/node24/node-v24.3.0-linux-x64/bin:$PATH npm install -q -g --prefix ~/tools/codex-cli @openai/codex@0.164.0
(cd ~/tools && PATH=~/tools/node24/node-v24.3.0-linux-x64/bin:$PATH npx -y @puppeteer/browsers install chrome-headless-shell@131 --path ~/tools/chrome-headless-shell-131)
mkdir -p ~/mirror ~/log/buildbox
for repo in sdk cloud; do [ -d ~/mirror/$repo.git ] || git init -q --bare ~/mirror/$repo.git; done
TOOLS
touch /var/lib/buildbox/bootstrap.done
echo "bb-bootstrap phase 1 done $(date -u +%FT%TZ)"
