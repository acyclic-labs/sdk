#!/usr/bin/env bash
set -euo pipefail

# Run from the external prototype root. KANI_HOME and cargo-kani are supplied
# by the caller so this script never installs tools or writes into the source
# foundation checkout.
: "${CARGO_KANI:?set CARGO_KANI to an installed cargo-kani binary}"
: "${KANI_HOME:?set KANI_HOME to an installed Kani bundle}"

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
mkdir -p proof-receipts
log="proof-receipts/kani.log"
set +e
KANI_HOME="$KANI_HOME" "$CARGO_KANI" \
  --manifest-path "$root/Cargo.toml" \
  --package rust-handle-proof-prototype \
  --harness proofs::stale_and_unissued_handles_reject \
  --harness proofs::distinct_registry_domains_reject_cross_registry_handles \
  --harness proofs::copied_token_is_idempotent_and_drops_once \
  --harness proofs::moves_and_clones_have_distinct_release_tokens \
  --harness proofs::generation_exhaustion_retires_without_reuse \
  --harness proofs::generation_neighbor_boundaries_retire_without_reuse \
  --harness proofs::lease_id_exhaustion_does_not_mutate_live_state \
  --harness proofs::clone_capacity_failure_is_atomic \
  --harness proofs::clone_active_count_overflow_is_atomic \
  --harness proofs::drop_witness_emits_once_at_final_release \
  --exact --default-unwind 8 -j1 --output-format terse 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
set -e
printf 'CARGO_KANI_EXIT=%s\n' "$status" | tee -a "$log"
if [[ "$status" == 0 ]]; then
  kani_version=$("$CARGO_KANI" --version 2>&1 | sed -n '1p')
  cbmc_version=$("$CARGO_KANI" --version 2>&1 | sed -n 's/^CBMC //p' | sed -n '1p')
  source_hash=$(sha256sum "$root/src/lib.rs" | awk '{print $1}')
  manifest_hash=$(sha256sum "$root/Cargo.toml" | awk '{print $1}')
  lock_hash=$(sha256sum "$root/Cargo.lock" | awk '{print $1}')
  cat > "proof-receipts/kani-receipt.json" <<EOF
{
  "schema": "rust-handle-proof-prototype/kani-receipt-v1",
  "status": "verified",
  "source_sha256": "$source_hash",
  "manifest_sha256": "$manifest_hash",
  "lock_sha256": "$lock_hash",
  "kani": "$kani_version",
  "cbmc": "$cbmc_version",
  "jobs": 1,
  "default_unwind": 8,
  "harnesses": [
    "proofs::stale_and_unissued_handles_reject",
    "proofs::distinct_registry_domains_reject_cross_registry_handles",
    "proofs::copied_token_is_idempotent_and_drops_once",
    "proofs::moves_and_clones_have_distinct_release_tokens",
    "proofs::generation_exhaustion_retires_without_reuse",
    "proofs::generation_neighbor_boundaries_retire_without_reuse",
    "proofs::lease_id_exhaustion_does_not_mutate_live_state",
    "proofs::clone_capacity_failure_is_atomic",
    "proofs::clone_active_count_overflow_is_atomic",
    "proofs::drop_witness_emits_once_at_final_release"
  ],
  "log": "proof-receipts/kani.log"
}
EOF
fi
exit "$status"
