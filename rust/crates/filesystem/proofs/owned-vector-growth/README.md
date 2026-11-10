# Owned vector growth

Run the production growth control:

```sh
cargo test --locked -p acyclic-fs --lib --no-default-features --features memory,native-mount kernel::persistent_point::tests::owned_growth_admits_the_original_and_replacement_before_allocating -- --exact --nocapture
```

`delta-only.patch` restores the original `Owned<Vec<T>>::ensure_for_push`
body from cd534dc1eb0, including its delta-only claim and actual reallocation.
The same control must fail with `growth admitted replacement while original
backing was still live`. Reverse the patch and require the control to pass.
The mutation leaves the shared reserve method available to other consumers.

The control exercises the production owned vector and logical vector adapters
on standalone and native ledgers. Refused peak capacity, refused copy credit
and cancellation allocate nothing and preserve the original value. Successful
growth allocates one replacement, charges its full capacity alongside the
original, copies the original payload once, and retires physical storage before
its credit. Final owners return the original ledger to its baseline.

Growth checks a constant number of accounting fields and has no waits or
history scan. Moving N live elements costs O(N); geometric push growth is
amortized O(1) per element, not worst-case constant time. This control does not
qualify merge algorithm intermediates or serializer output admission.

The blob builder uses the same growth operation, preserving its ordinary
allocator error and the original native allocator cause. Its existing bound
control also refuses simultaneous backing before any physical allocation:

```sh
cargo test --locked -p acyclic-fs --lib --no-default-features --features memory,native-mount kernel::blob::index::tests::small_blob_reservations_do_not_depend_on_page_width -- --exact --nocapture
```

`blob-delta-only.patch` restores the original `reserve_blob_vec` from
1224312c3eb5ece84674a95ce66790169b002954. The same control must fail with
`blob growth admitted replacement while original backing was still live`.
Reverse the patch and require the unchanged control to pass. The existing
`kernel::blob::tests::impossible_blob_buffers_fail_before_allocation_and_preserve_accounting`
control retains the actual ordinary allocator-error contract.

The existing real tree-diff control also checks that growing the change vector
keeps every moved key and value name funded on both accounting paths:

```sh
cargo test --locked -p acyclic-fs --lib --no-default-features --features memory,native-mount kernel::persistent_diff::tests::leaf_diff_reports_sorted_add_delete_modify_and_exact_truncation -- --exact --nocapture
```

`element-credit.patch` restores the growth body that released element funding
with the original vector backing. The control must fail with `vector growth
released moved name capacity`. Reverse the patch and require the same control
to pass. The oracle counts the actual returned vector and name capacities,
checks live credit before dropping them, and requires final retirement to
restore the original baseline. Moving element funding uses constant-work
guard splits and joins, with no physical allocation or payload copy.

The same replacement operation supports `VecDeque` and adapts an existing
native guard without creating another ledger. The watch queue, materializer
buffers and retained projection handles share it. The queue control covers
wrapped entries, refused replacement with zero physical allocations, admitted
replacement with one allocation, preserved order, and final backing retirement:

```sh
cargo test --locked -p acyclic-fs --lib --no-default-features --features memory,native-mount watch_queue::tests::native_queue_custody_tracks_backing_and_retries_denied_growth -- --exact --nocapture
```

`queue-delta-only.patch` restores the original queue growth body from
1366b0461a80e2211db798d546ea8ec6be509075. The control must fail with `queue
growth admitted replacement while original backing was still live`. Reverse
the patch and require the unchanged control to pass. Its 4,096-byte allowance
is unchanged; correct overlap admission refuses growth to 256 `u64` entries
while the 128-entry backing is still live. The smaller wrapped-byte cases
independently distinguish final capacity from simultaneous live capacity.

The existing `materializer::tests::buffer_growth_funds_both_live_backings`
control covers the native vector adapter and the actual image writer. Standard
channel differential properties continue to cover queue bounds and disconnects.
No growth operation adds a syscall, durability barrier, round trip or wait.
