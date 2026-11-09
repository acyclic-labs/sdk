#!/usr/bin/env bash
set -euo pipefail
if [ "$#" = 0 ]; then
    exec unshare --user --map-root-user --mount --propagation private --fork -- bash "$0" --isolated
fi
test "$#" = 1 && test "$1" = --isolated
# A private user/mount namespace hides the actual FUSE device for every owner
# and client. Keep only the ordinary devices Node and the native runtime use.
saved=$(mktemp -d)
trap 'rmdir "$saved"' EXIT
mount -t tmpfs -o mode=755 tmpfs "$saved"
for name in null zero random urandom; do
    touch "$saved/$name"
    mount --bind "/dev/$name" "$saved/$name"
done
ln -s /proc/self/fd "$saved/fd"
mount --move "$saved" /dev
node --test --test-concurrency=1 "$(dirname -- "$0")/headless-nofuse.test.mjs"
