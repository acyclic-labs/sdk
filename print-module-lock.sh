#!/bin/sh
set -eu
bazel mod deps --lockfile_mode=update
printf 'MODULE_LOCK_BASE64='
base64 -w0 MODULE.bazel.lock
printf '\n'
