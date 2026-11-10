# Logical read scratch retirement

`logical_objects::sealing::tests::ordinary_opening_retains_original_work_and_live_backing_on_every_exit`
exercises the public scalar and batch reads with both a shared plaintext view and
a separately allocated plaintext buffer. It retains an independent original-ledger
allocation through success, refusal and exhausted credit, and checks that retiring
the returned buffer and its aliases leaves only the earlier live allocations.

The production mutation restores the former blanket retention of discarded input
and temporary buffers. Apply it with
`git apply rust/crates/filesystem/fuzz/mutations/logical_read_scratch.patch`, then run:

```sh
cargo test --locked -p acyclic-fs --no-default-features --features local,memory,distributed --lib logical_objects::sealing::tests::ordinary_opening_retains_original_work_and_live_backing_on_every_exit -- --exact
```

The mutated source must fail `opening scratch did not retire`. Restore with
`git apply -R rust/crates/filesystem/fuzz/mutations/logical_read_scratch.patch`.
The same command must then pass. No alternate sealer or accounting implementation
is selected by the mutation.
