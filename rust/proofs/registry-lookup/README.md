# Borrowed registry lookup

The retained allocation tests use the real registry at 1, 64 and 4096 entries,
with explicit, inline and selected revisions. Each checks a first lookup and
its repeat; elapsed-time sampling is unnecessary for exact allocation counts.

Apply `owned-key.patch` with `git apply rust/proofs/registry-lookup/owned-key.patch`.
Run `cargo test -p acyclic-harness-core --features filesystem --lib tool::registry::tests::lookup::resolution_`.
The control must fail on temporary lookup allocations. Reverse the patch with
`git apply -R rust/proofs/registry-lookup/owned-key.patch`; the same tests must pass.
