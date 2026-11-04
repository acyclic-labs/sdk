The retained control restores the original per-pair ordering validation in
`writes_disjoint`. It changes the production classifier used by all three
fixed-proposal cohorts; it does not substitute a model or a test counter.

From the repository root:

```
git apply rust/crates/filesystem/src/kernel/rebase/tests/repeated_order_validation.patch
cargo nextest run --manifest-path rust/Cargo.toml --locked -p acyclic-fs --lib -E 'test(=kernel::rebase::footprint::tests::footprint_query_charges_the_exact_bounded_prefix)'
git apply -R rust/crates/filesystem/src/kernel/rebase/tests/repeated_order_validation.patch
```

The mutated producer must fail with `fixed proposal ordering must be validated
once per cohort`: its visit curve is `[63, 287, 2079]`, versus `[32, 39, 95]`
for the production classifier. The same property checks every insufficient
allowance on the original read-conflict prefix and the final refused visit of
each cohort. No credit refusal may become an independence certificate.

The restored block comes from the original `writes_disjoint` implementation in
commit `a36e0a00060d31bc6df047037583eb3069167a7c`.

The same property covers the shared ordered merge at the first and last file
conflict, with exact original costs of 33 and 64 examinations and every
insufficient allowance. Apply
`rust/crates/collections/src/tests/continued_conflict.patch` instead to remove
the actual shared producer's early-conflict refusal. The unchanged property
must fail at `shared merge must stop at the original write conflict`. Restore
that patch with `git apply -R` before running the positive control.
