#!/bin/sh
set -eu
cd "$TEST_SRCDIR/$TEST_WORKSPACE"
exec "$NODE" --test toolchains/cargo-deny/advisory-ranges.test.mjs
