#!/usr/bin/env bash
# Agent control kernel proofs: every production harness verifies and every
# negative-control harness finds a counterexample.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
kani agent_control.rs --output-format terse
negative="$(kani agent_control_negative.rs --output-format terse 2>&1 || true)"
printf '%s\n' "$negative"
grep -q 'Complete - 0 successfully verified harnesses, 6 failures, 6 total.' <<<"$negative"
