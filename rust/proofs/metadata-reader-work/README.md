# Metadata reader work refinement

Run `node rust/proofs/qualify.mjs metadata-reader-work`.

The oracle creates metadata through the public in-memory filesystem, reads its actual stored object, and records the canonical decoder's completed work. It compares that original trace with the public detached-file read at every copy-credit boundary. Insufficient credit must return the original typed work refusal without charging beyond the admitted credit; complete credit must preserve the metadata and exact copy/item receipt.

The retained production mutation restores the observation-discarding decoder invocation from the merge reader before commit 5ece7b74ab58dd2a802cff3db99c162a63495524. It retains the real object read and fixed item charge. The oracle must reject the resulting uncharged primitive copies. The qualifier restores the exact original production bytes and requires the same oracle to pass.

Ordinary reads and merge reads share one implementation. Per metadata read, before and after: one object read and one canonical decode; zero additional allocations, payload copies, syscalls, durability barriers, round trips, tasks or timers. The fixed twelve-field format bounds decoder work independently of namespace size and history. The original object-read producer, cancellation and allocation scope retain progress and retirement custody; this refinement adds no wait. Existing object-read failure and cancellation controls cover that shared producer. The existing decoder_work fuzzer retains malformed metadata coverage; this change does not introduce another decoder or format.
