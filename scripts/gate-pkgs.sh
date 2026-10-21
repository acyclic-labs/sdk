#!/bin/bash
# Derive gate roots and their local dependency closure from this checkout.
set -euo pipefail
checkout=$(git -C "${1:-.}" rev-parse --show-toplevel)
exec python "$checkout/scripts/gate-packages.py" "$checkout"
