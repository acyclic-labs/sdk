#!/usr/bin/env bash
# Offline only. Supply a Python with scripts/benchmarks/requirements.lock installed.
set -euo pipefail
python="${1:-python3}"
export PYTHONDONTWRITEBYTECODE=1
"$python" -m unittest scripts.benchmarks.test_contracts scripts.benchmarks.test_harbor -v
"$python" -m scripts.benchmarks.negative_controls
