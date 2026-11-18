#!/usr/bin/env bash
# Run on a disposable checkout: each source mutation must fail the same oracle.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
printf 'RESCUED_FS_SOURCE %s\n' "$(git rev-parse HEAD)"

regressions=(
  every_writer_shape_round_trips_with_exact_completed_work
  source_callbacks_share_the_real_cache_without_lock_inversion
  reopening_waits_for_the_retiring_native_owner
  journal_write_bound_exhausts_a_narrow_index_domain_and_rejects_truncation
  persisted_lineage_names_require_exact_canonical_bytes
  foreign_work_is_refused_before_activating_a_real_source
  shared_access_oracle_detects_a_corrupted_decision
)
positive=(
  journal_write_bounds_preserve_wide_budget_allowances
  core_state::owners::tests::
  lifecycle_operations_retain_the_source_invocation_and_finite_budget
  access::roundtrip::
  native_mount::fuse::tests::retirement::
  physical_source_cleanup_joins_last_lease_after_caller_cancellation
  physical_driver_packet_joins_last_lease_after_caller_cancellation
  physical_lazy_source_joins_last_lease_after_caller_cancellation
  physical_fuse_loop_joins_last_lease_after_caller_cancellation
)
oracle=(bazel test //rust/crates/filesystem:acyclic_fs_test
  //rust/crates/filesystem:main --test_output=all --nocache_test_results)
filters=("${regressions[@]/#/--test_arg=}")

patches=(
  rust/crates/filesystem/fuzz/mutations/file-table-writer-admission.patch
  rust/crates/filesystem/proofs/cache-callback-order/lower-cache-rank.patch
  rust/crates/filesystem/proofs/owner-retirement/missing-handles.patch
  rust/crates/filesystem/proofs/write-bound/truncated-allowance.patch
  rust/crates/filesystem/fuzz/mutations/lineage_noncanonical_name.patch
  models/mutations/native-work-activation-original-omitted.patch
  rust/crates/filesystem/fuzz/mutations/access/unchecked_publication.patch
)
sources=(
  rust/crates/filesystem/src/kernel/file_table/page_codec.rs
  rust/crates/filesystem/src/lock_order.rs
  rust/crates/filesystem/src/core_state/owners.rs
  rust/crates/filesystem/src/core_state/native_io.rs
  rust/crates/filesystem/src/workspace/identity.rs
  rust/crates/filesystem/src/native_mount/shared/metering.rs
  rust/crates/filesystem/src/access/view.rs
)
for patch in "${patches[@]}"; do
  git apply --check "$patch"
done
cargo fmt -p acyclic-fs --check
"${oracle[@]}" "${filters[@]}" "${positive[@]/#/--test_arg=}"

negative=$(mktemp)
cleanup() {
  git restore -- "${sources[@]}"
  rm -f "$negative"
}
trap cleanup EXIT
for patch in "${patches[@]}"; do
  git apply "$patch"
done

# One mutant compilation, then all seven independent regressions. A compiler
# failure cannot count as a detected semantic regression.
set +e
"${oracle[@]}" "${filters[@]}" --test_env=ACYFS_WRITER_OMIT_GUARD=leaf-order 2>&1 | tee "$negative"
result=${PIPESTATUS[0]}
set -e
if [ "$result" -ne 3 ]; then
  printf 'Expected failing tests from source mutations, got exit %s\n' "$result" >&2
  exit 1
fi
for name in "${regressions[@]}"; do
  if ! grep -E "FAIL.*::$name([[:space:]]|$)" "$negative"; then
    printf 'Mutation did not reach regression oracle: %s\n' "$name" >&2
    exit 1
  fi
  printf 'RESCUED_FS_NEGATIVE_CONTROL_PASS %s\n' "$name"
done
