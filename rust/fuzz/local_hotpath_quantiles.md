# Report order-statistic validation

The report and `local_hotpath_quantiles` property include the same constructor.
The property replaces fixed example cohorts with generated lengths, finite
nonnegative floating-point bit patterns, both signed zeros, and invalid values
at generated positions. Empty, negative and nonfinite cohorts must be refused.

For each accepted percentile, the independent oracle counts original values
strictly below and at or below the result. Those counts must bracket its required
percentile rank. It does not reproduce the constructor's sorting or rank-index
calculation. Generated inputs are typed measurements, not provider wire fixtures.
This validates report arithmetic only; it does not measure a deployed service.

Run from the repository root:

```sh
cargo test --locked -p acyclic-sdk-fuzz --no-default-features --features local-hotpath-quantiles --test local_hotpath_quantiles
```

The retained `floor-rank.patch` changes the actual constructor from ceiling to
floor division. Apply it with `git apply rust/fuzz/local_hotpath_quantiles/floor-rank.patch`
and run the same property. The unchanged oracle must reject that source mutation.
Restore it with `git apply -R rust/fuzz/local_hotpath_quantiles/floor-rank.patch`
before the positive check. Execution qualification is pending.
