# Offline paired experiment analysis

`acyclic_harness::experiments` analyzes trial observations independently of the
execution engine. Pair assignments are version 1. Observation rows and paired analysis reports are version 2;
assigned-trial reports, arm cost summaries, scorers and decisions remain version 1.
The module provides paired differences, ordinary paired
standard errors, intercept-only CR1
clustered standard errors, normal-approximation power planning and an objective
integer scorer and assigned-trial summaries for organism probes.

The [organism-probe recipes](README.md)
share authored task, intervention and scorer files. Their results tables are
explicitly unrun; the recipes introduce no execution mechanism.

## Pair and cluster identity

`plan::AssignmentPlan::new(arms, replicates)` admits a positive arm count and a
representable total once. It plans one source population, assigning every arm
once per replicate. Arm numbers index the caller's declared arm/configuration
table. Its lazy iterator rotates execution order by one arm each replicate;
over a complete block of `arms` replicates, every arm occupies every position
once. Partial blocks remain unbalanced. Zero replicates is an empty plan.

Direct `assignment(index)` access takes constant work even near the end of a
large plan. Two integer fields retain the declaration; no array, outcomes or
history are stored. Out-of-range selection finishes with `None`. Outcomes never
change assignments: failed work stays in the declared denominator. This helper
neither selects a saved cut nor dispatches work, admits grants, enforces a model
or replaces the ordinary fork/replay entrance. Configuration remains in operator
files. Execution order differs from analysis row order: emit each matched pair
baseline first even when treatment executed first.

Declare every paired key before outcomes with
`plan::PairAssignment::new(population, replicate)`. Serialize these as version-1
objects containing `version`, the 32-byte `population` array and `replicate`.
The shared strict object decoder rejects changed versions, unknown or duplicate
fields and positional arrays. Keep this original declaration independently of
result collection; rebuilding it from completed rows would hide omitted work.
It declares identities, not proof that the cuts or providers actually match.

An observation binds a 32-byte population identity, replicate number, numeric
arm, optional finite score and optional complete measured cost. The population
identity names the admitted comparison and common source cut; keep its arm-name
table and original output/accounting references alongside it. Use `Observation::new` or decode a
complete JSON object through `Observation::decode`. Admission checks its version,
finite score and finite nonnegative cost once. Score and cost are independent:
an unscored output can retain its fully accounted cost. Missing values remain
unknown; the decoder rejects other versions without a migration path.

`treatment.difference(&baseline)` requires equal population and replicate and
different arms. It returns `None` if either score is missing.
`treatment.cost_difference(&baseline)` uses the same identity check and returns
`None` if either cost is missing, regardless of score validity. Identity checks
still reject mismatched rows when values are missing. Pairing uses fixed-width
identities, takes constant work and allocates nothing. Score subtraction overflow
is a typed refusal; finite nonnegative costs cannot overflow on subtraction.

One independent problem/source cut is one cluster. Every replicate and graph
condition sharing that cut belongs to that cluster. Group its paired differences
once, feed them into `stats::Cluster`, then pass the finished cluster once to
`stats::PairedStats`. Keep a separate accumulator per declared treatment
comparison. Callers supply independent grouping; the module stores no cluster-key
map and does not detect submitting the same cluster twice.

For example:

```rust
use acyclic_harness::experiments::{Observation, analysis::Contrast, plan::PairAssignment};

# fn example() -> Result<(), acyclic_harness::experiments::Error> {
let rows = [
    (1, None, None),
    (2, Some(1.0), Some(2.0)),
    (3, Some(1.0), Some(4.0)),
].into_iter().flat_map(|(source, baseline, treatment)| [
    Observation::new([source; 32], 0, 0, baseline, None),
    Observation::new([source; 32], 0, 1, treatment, None),
]);
let expected = (1..=3).map(|source| Ok(PairAssignment::new([source; 32], 0)));
let report = Contrast::new(0, 1)?.analyze(rows, expected)?;
assert_eq!(report.assigned_pairs, 3);
assert_eq!(report.assigned_clusters, 3);
assert_eq!(report.score.pairs, 2);
assert_eq!(report.score.estimate.map(|estimate| estimate.difference), Some(2.0));
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
for uncertainty. Feed known `cost_difference` values into a separate
`stats::Cluster`/`stats::PairedStats` accumulator, including fully priced trials
whose outputs were unscored. Report fully priced matched-pair coverage alongside
the estimate. Unknown costs are never zero, and uncertainty over a measured
subset does not describe all assigned trials. Score-pair coverage is separate;
the assigned-trial report retains the primary correctness denominator.

## Analyze an observation file

The `experiment-analysis` contributor program reads version-2 observation objects
from one file, accepting JSONL without retaining its rows:

```sh
cargo run --locked -p acyclic-harness-core --example experiment-analysis -- 0 1 assignments.jsonl observations.jsonl > analysis.json
```

For each assigned `(population, replicate)` write the declared baseline arm first
and treatment arm second. Pair keys must strictly increase lexicographically;
this catches duplicate assignments and a closed cut appearing again using one
retained key. The input producer supplies that order. No sorting or history scan
is performed. Supply the original version-1 assignment file declared before
outcomes, independently of the version-2 observation file. Each assignment needs
its exact population and replicate, not merely an equal row count. All assigned
pairs need rows even when both values are unknown. Missing, surplus or replaced
pairs are refused. A declaration rebuilt from completed rows proves no coverage.
Original source independence, fork/provider evidence and numeric-score semantics
remain the producer's responsibility. A population digest alone proves none of
those facts. For primary correctness, predeclare unscored outputs as failures;
conditional score estimates with missing rows do not replace assigned-trial rates.

The version-2 output counts all assigned pairs and cuts, with separate known-pair
and contributing-cut counts for score and cost. A cut with no known values for a
metric stays in assigned coverage and does not enter that metric's estimator.
Fewer than two contributing cuts yields an unknown estimate, even when many
assigned cuts were empty. Manual consumers must likewise skip empty score and
cost clusters and retain their assignments in coverage counts.

`baseline_cost` and `treatment_cost` each contain a version-1 `CostReport`:
assigned trials, fully priced trials, the known-cost sum, complete total and USD
per assigned trial. All assignments contribute, regardless of scoring or matched
cost availability. If baseline accounting is incomplete, its total and mean stay
unknown even when treatment is fully priced. A treatment cost without its matched
baseline cost still contributes to the treatment summary. It does not enter the
paired cost estimate. Empty input has a zero total and unknown mean.

`AssignedTrials` and this pipeline share one cost accumulator and complete-cost
rule. The existing flat version-1 assigned-trial report is unchanged. The paired
analysis report advances to version 2 for its two new arm summaries; no format
migration path is provided. An overflowing known-cost sum refuses the report
instead of publishing a nonfinite total or silently treating it as missing.

`analysis::Contrast::new` admits distinct arms before opening input;
`Contrast::analyze` shares the pipeline with other callers. It checks a pair once
for both contrasts and reuses `Cluster`/`PairedStats`. It returns no partial report
on a malformed row, missing partner, changed arm, duplicate or decreasing key,
declared assignment-key mismatch or numerical refusal. The program buffers
input and output, writes one report at the end and flushes once. It adds no model
call, executor, replay or scheduling.

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

The shared file pipeline reduces two identity checks per score/cost pair to one.
Its numeric state allocates nothing and retains two observations, one ordering
key, the current assignment and fixed accumulators. Each pair consumes one
declared key and compares its fixed-width identity once; EOF checks for one
leftover declaration. The ordering key copies 32 population bytes only when a
new cut starts; later replicates update one integer. The contributor program
opens two input files, reads each through one buffer and writes through one output buffer,
with one final flush and zero durability barriers or network round trips. Read
syscalls depend on input bytes; JSON decoding can reuse scratch space for the
current token. Neither buffer nor parser retains earlier rows. EOF with a partial
row or pair is an error. Every observed key must match the original declaration,
and both inputs must finish together. The extra open and input buffer buy exact
assignment coverage: counts alone cannot reject a substituted failed trial.

Each arm summary adds only a priced count and known-cost sum: four scalar fields
for the comparison, independent of trial count. It reuses the checked assignment
count. A known cost adds one sum/overflow check and one counter increment; unknown
cost adds neither. Cost admission is shared and occurs once at each public input
boundary. The assigned-trial summary still retains six scalar fields.

Controls cover mismatched pairs even with missing values, unscored but fully
priced trials, unknown versus zero costs, malformed/ambiguous objects, nonfinite
inputs, overflow, false independence, unequal cluster sizes and large common offsets.
The power-planning negative control rejects NaN intermediates before applying
its minimum. A bounded proof source checks that objective correctness and planted
adoption are mutually exclusive; another checks that admitted paired costs stay
finite even without scores. Focused contributor commands:

```sh
cargo check --locked -p acyclic-harness-core --lib
cargo test --locked -p acyclic-harness-core --lib experiments::stats::tests::clustered_estimate_matches_direct_residuals_with_unequal_clusters
cargo test --locked -p acyclic-harness-core --lib experiments::reports::tests::unscored_trials_and_unknown_cost_remain_in_assigned_denominators
cargo test --locked -p acyclic-harness-core --lib experiments::tests::paired_cost_retains_unscored_trials_without_imputing_unknown_values
cargo test --locked -p acyclic-harness-core --lib experiments::analysis::tests::empty_cuts_and_unscored_paid_trials_keep_separate_coverage
cargo test --locked -p acyclic-harness-core --lib experiments::analysis::tests::arm_costs_keep_unmatched_and_unscored_assignments_without_inventing_totals
cargo kani -p acyclic-harness-core --harness objective_success_and_injection_adoption_are_exclusive
cargo kani -p acyclic-harness-core --harness admitted_cost_difference_is_finite_even_without_scores
cargo kani -p acyclic-harness-core --harness ordered_pair_admission_rejects_reuse_without_changing_the_last_key
cargo kani -p acyclic-harness-core --harness incomplete_cost_coverage_never_claims_a_full_total_or_mean
cargo kani -p acyclic-harness-core --harness assignment_indices_are_bijective_within_the_declared_population
cargo kani -p acyclic-harness-core --harness a_complete_pair_is_accepted_only_for_its_declared_assignment
```
