# Original closure work and output custody

The closure walker borrows the original accounting frame for physical reads and
incremental diffs. Returned proof allocations keep independent leases on that
same ledger. A withdrawn future drops its authority frame before releasing the
shared accounting lock; the frame restores the original counters and scratch.
Re-entry fails with the typed accounting-handoff error instead of waiting.

`minimal_generation_has_a_complete_authenticated_closure` delegates reads to the
real memory store, checks prior work at every physical admission, adds seven codec
units per read and requires all seven at the outer receipt. It checks that proof
output and independent live backing retain their original funding after the
meter retires, including retirement after cancellation. The existing frame and
retained-allocation controls cover withdrawal and shared backing retirement.

The kept `mutations/closure_original_frame.patch` restores the former production
read's replacement-frame construction. The same real-store relation must reject
it before the first physical read. Restore the production source after applying
the patch, then require the relation to pass:

```sh
cargo test -p acyclic-fs --no-default-features --features local,memory,distributed --lib kernel::closure::tests::minimal_generation_has_a_complete_authenticated_closure -- --exact
```

Lease transfer, combination and frame borrowing take constant work and introduce
no waits, reads, copies or durability barriers. An ordinary shared meter admits
its value and funding controls plus promotion of the original ledger, each once;
native meters retain the existing original work without these allocations.
Closure traversal remains proportional to admitted objects and records. The
membership relation keeps its linear copy and consuming-cursor bounds and the
old quadratic-move and repeated-seek negative controls at 512 entries, with
complete identity-bit coverage retained separately in the same relation.

The same relation exercises the shared single-object and batch adapters used by
incremental walks. Both borrow the original frame. The batch retains its actual
container capacity and producer-reported owned bytes until physical retirement;
temporary scratch retires at scope exit. The transfer uses one checked addition
and one live-byte comparison, with no allocation, scan, wait, copy or admission.
Reading a batch still takes work proportional to its requested objects.

The kept `mutations/object_read_custody.patch` plants three independent omissions
in the actual shared read and scope producers. Only the planted source reads
`ACYCLIC_READ_CUSTODY_OMISSION`; select one omission per run of the same command:

- `single` replaces the single-read frame with remaining-budget work.
- `batch` replaces the batch-read frame with remaining-budget work.
- `retirement` retires the batch output at scope exit.

The first two must fail the original-frame admission assertion. The third must
fail `batch output funding retired at scope exit`. All three use one compiled
planted binary; restore the source and require the positive run afterward.
The removed
`late_backend_retention_refusal_releases_only_returned_backing` and
`backend_merge_failure_does_not_release_unregistered_returned_backing` examples
exercised the deleted receipt adapter with fabricated receipts. The surviving
real-store relation checks original funding and retirement instead;
`object_store_verifies_identity_bounds_and_original_work` retains actual-store
budget, cancellation and suspended-custody refusal coverage.

The generated `wrapper_checks_preserve_all_limits_receipts_and_cancellation_prefixes`
relation keeps its original inputs, accounting modes and independent failure
prefix checks. Its removed `prefix_oracle_rejects_missing_completed_charges`
example skipped completion inside the test driver. Missing copy completion is
instead planted in the real producer by `mutations/decoder_copy_completion.patch`
and checked by the retained canonical decoder relation.

Historical payload aliases use the same retained owner as publication payloads.
`distributed::accounting::tests::historical_reads_preserve_the_complete_original_receipt`
delegates to the actual stream authority and verifies both ordinary and native
funding after replay scope exit, record destruction, cancellation and the last
payload alias. The independent original allocation remains live throughout.
`mutations/payload_alias_custody.patch` drops the producer's actual retained
lease before returning the physical `Bytes`; it must fail
`historical replay retired funding before its returned payload`.
Restore the source before requiring the same relation to pass.

The conversion admits one exact shared owner header before allocation, copies no
payload bytes and combines its lease with the existing payload lease. An
ordinary ledger promotes once when its first independent lease is needed.
Native stream requests retain the existing stream `OwnedBytes` adapter. No
provider read, wait, syscall or durability barrier is added. This payload owner
does not supply the separate public accounting page or record-vector contract.

Creation, retention, fork and deletion commits use this same owner. Their
encoded capacity stays on its original ledger until the last payload alias
retires. The former manual capacity totals and post-append releases are removed;
the native path still allocates one shared header, and the ordinary path now
admits that exact header before allocation. No payload copy or hash is added.

The minimal-generation relation also creates canonical objects through the
public logical store over `LocalObjects::open` with its real default settings.
The existing observer delegates to that store; the read must report nonzero
owned backing, keep it funded outside the adapter scope and retire exactly
that backing while the independent allocation remains live. The same relation
then checks batch and whole-closure custody against that local provider.
`mutations/single_owned_read_retirement.patch` removes the actual single-read
output transfer. Shared-memory reads still pass; the local owned read must fail
`single owned output funding retired at scope exit`. This closes the shared-only
coverage gap without an artificial owned-read provider.

Content comparison and publication projection retire only their own scratch
through the shared allocation scope. Changed-path collection carries its actual
owned allocation through publication instead of reconstructing it from total
live bytes; its failure path retires that same allocation without rescanning
paths. These transfers introduce no copies, I/O, durability barriers or waits.

The existing `native_range_restoration_compares_bytes_across_payload_layouts`
property also compares its actual committed file records while an observer
retires an independent owned buffer on the first delegated store read. Cleanup
must leave the other independent buffer fully funded, then retire exactly to
zero after that buffer is destroyed. Apply
`mutations/content_comparison_scratch.patch` to restore the old total-live-byte
cleanup in the production comparison with `ACYCLIC_FS_SCRATCH_OMISSION=1`.
The property must reject it at
`comparison custody` or `comparison cleanup retired independent allocation`;
restore the patch before requiring the same property to pass.

Creation, fork and retention proofs borrow the caller's original authority
frame through the same `ProofMeter::from_accounting` constructor. The proof
guard moves with the authenticated object vector, including cached publication
pins; no receipt merge, replacement claim or second proof ledger is needed.
The root-parent buffer retires when its decoded root is destroyed. This
handoff adds no object read, copy, hash, wait or durability barrier.

The existing minimal-generation relation also creates a volume through the
public `Fs::new` composition over the shipped logical store. Its observer
requires the original completed codec work at each delegated proof read.
`mutations/publication_proof_frame.patch`, with
`ACYCLIC_FS_PROOF_FRAME_OMISSION=1`, replaces the actual proof producer's meter;
the relation must fail `closure read replaced its original frame`. Both patches
select their omissions independently in one compiled producer. Restore both
before running the positive relations against the unmodified source.
