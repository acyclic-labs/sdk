# Offline paired experiment analysis

`acyclic_harness::experiments` analyzes scored observations independently of the
execution engine. Input rows and reports are version 1. The module provides
paired differences, ordinary paired standard errors, intercept-only CR1
clustered standard errors, normal-approximation power planning and an objective
integer scorer and assigned-trial summaries for organism probes.

The [organism-probe recipes](../../harness/docs/plugins/experiments/README.md)
share authored task, intervention and scorer files. Their results tables are
explicitly unrun; the recipes introduce no execution mechanism.

## Pair and cluster identity

An observation binds a 32-byte population identity, replicate number, numeric
arm, finite score and optional measured cost. The population identity names the
admitted comparison and common source cut; keep its arm-name table and original
output/accounting references alongside it. Use `Observation::new` or decode a
complete JSON object through `Observation::decode`. Admission checks its version,
finite score and finite nonnegative cost once. Missing cost remains unknown.

`treatment.difference(&baseline)` requires equal population and replicate and
different arms. Pairing uses fixed-width identities, takes constant work and
allocates nothing. Subtraction overflow is a typed refusal.

One independent problem/source cut is one cluster. Every replicate and graph
condition sharing that cut belongs to that cluster. Group its paired differences
once, feed them into `stats::Cluster`, then pass the finished cluster once to
`stats::PairedStats`. Keep a separate accumulator per declared treatment
comparison. Callers supply independent grouping; the module stores no cluster-key
map and does not detect submitting the same cluster twice.

For example:

```rust
use acyclic_harness::experiments::{Observation, stats::{Cluster, PairedStats}};

# fn example() -> Result<(), acyclic_harness::experiments::Error> {
let mut report = PairedStats::default();
for (source, baseline_score, treatment_score) in [(1, 1.0, 2.0), (2, 1.0, 4.0)] {
    let baseline = Observation::new([source; 32], 0, 0, baseline_score, None)?;
    let treatment = Observation::new([source; 32], 0, 1, treatment_score, None)?;
    let mut cluster = Cluster::default();
    cluster.push(treatment.difference(&baseline)?)?;
    report.push(cluster)?;
}
let estimate = report.estimate()?;
assert_eq!(estimate.difference, 2.0);
# Ok(())
# }
```

The CR1 estimator allows unequal cluster sizes:

`SE² = G/(G-1) × Σ_g (Σ_i d_gi - n_g × mean(d))² / N²`.

Centered online QR avoids subtracting nearly equal covariance sums for large
common treatment offsets. Each update takes constant work and space and stores
no rows. Estimates require at least two independent clusters; more agents from
one source cannot supply independent uncertainty.

`stats::required_clusters(sd, effect, z_alpha, z_power)` uses
`ceil(((z_alpha + z_power) × sd / effect)²)`, with at least two clusters. Supply
quantiles explicitly, for example 1.959964 and 0.841621 for two-sided 5% and 80%
power. This is a normal approximation for equal-sized independent cluster means.
Use pilot cluster simulation for unequal sizes or small-sample inference.

## Objective probe decisions

`probes::Scorer::decode` accepts a complete object such as
`{"version":1,"expected":47,"injected":52}`. `Decision::decode` accepts
`{"version":1,"value":47}`. Their shared object decoder rejects positional
arrays, duplicate fields and trailing input without an intermediate JSON tree.
The scorer requires distinct objective and planted values.

`Scorer::score` makes two integer comparisons: objective correctness and planted
adoption. A third wrong value is neither. Matching answers do not establish
transmission or collusion: retain original sender, committed input and file-read
generation evidence separately. Missing, malformed, refused and failed outputs
are separate failure rows and remain in the assigned-trial denominator.

## Assigned-trial tables

Feed each settled assigned trial once to `reports::AssignedTrials::push`.
Pass the scorer's assessment for a valid decision and `None` for an unscored
output. Assessments expose `correct()` and `adopted_injection()`; only the
scorer constructs them. An unrelated wrong decision is valid but neither
correct nor adopted. Keep original outcome records to distinguish missing,
malformed, refused and failed outputs.

`report()` retains every assignment in the primary correctness denominator.
It also reports correctness among valid decisions, the unscored fraction and
observed adoption as a lower bound. For example, one correct decision and one
unscored output yield correctness of 1/2 over assignments and 1/1 among valid
decisions. No valid decisions makes the latter fraction unknown.

Pass a cost only when the entire trial is priced from its actual accounting,
including failed work and all paid participants. Partial or absent accounting
is `None`. The version-1 report includes the number and sum of fully priced
trials; total and cost per assigned trial remain unknown unless every trial is
fully priced. Priced-call coverage stays with the original accounting records.
These arm totals are descriptive; use matched pairs and independent clusters
for uncertainty.

## Execution and cost boundary

Research execution belongs in an experiments plugin directory containing files
and programs. It composes scope forks, edits to `agent.ts`, append to granted
streams, durable reads plus an alarm and the caller-owned `calls/<id>` reply path.
Phase-change status is observation; scope progress/counts come from
`counts(range)`. Execution must admit actual model/provider configuration and
grants before activation. This offline module adds no runner, status producer,
reply path, provider dispatch, polling or activation.

Future pilots use only the admitted cheap models and 20–50 total assigned trials
per arm across independent cuts. Report measured total USD divided by assigned
trials, including failures and coordinator/injector work, with priced-call
coverage and unknown cost separately. These analysis examples make no provider
calls; no measured USD/run is claimed.

## Work and controls

Numerical updates and scoring add zero syscalls, durability barriers, round trips,
allocations or waits. Caller-built observations copy one 32-byte identity and
validate once; byte decoding allocates no JSON tree. Every synchronous operation
either returns its value or a typed refusal; no producer event is awaited here.
Rejected statistics and trial-summary updates leave their accumulators unchanged.
Trial summaries retain six scalar fields, with no rows, maps or repeated scoring.

Controls cover mismatched pairs, malformed/ambiguous objects, nonfinite inputs,
overflow, false independence, unequal cluster sizes and large common offsets.
The power-planning negative control rejects NaN intermediates before applying
its minimum. A bounded proof source checks that objective correctness and planted
adoption are mutually exclusive. Focused contributor commands:

```sh
cargo check --locked -p acyclic-harness-core --lib
cargo test --locked -p acyclic-harness-core --lib experiments::stats::tests::clustered_estimate_matches_direct_residuals_with_unequal_clusters
cargo test --locked -p acyclic-harness-core --lib experiments::reports::tests::unscored_trials_and_unknown_cost_remain_in_assigned_denominators
cargo kani -p acyclic-harness-core --harness objective_success_and_injection_adoption_are_exclusive
```
