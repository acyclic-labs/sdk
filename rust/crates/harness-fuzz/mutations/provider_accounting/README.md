# Separate accounting allocation control

Apply `separate-usage-owner.patch`, then run:

```sh
cargo nextest run --locked -p acyclic-harness-providers --lib -E 'test(accounting_properties::calibration_only_tightens_and_matches_the_minimum_observed_ratio)'
```

The mutation gives the usage anchor its own heap allocation again. The retained
property uses the same accounting constructor as `Registry::resolve`, measures
its allocation cost, and exercises the real calibration state over generated
observations. Its `selection accounting allocated separate owners` assertion
must fail. Undo the patch before retaining a candidate.

Inputs are generated accounting observations, not provider response fixtures.
No provider decoder, transport, or external wire fixture is replaced.
