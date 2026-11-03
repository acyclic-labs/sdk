# Experiment file validation

`experiment_files` invokes the public `Observation::decode`, `Scorer::decode`
and `Decision::decode` entries, their original shared object decoder and the
production scorer/report fold. It covers the offline file formats already
exposed by the library, without an experiment runner or provider calls.

The oracle independently requires supported versions, required/known fields,
object shape, no duplicate fields and no trailing input. Every declared field
is repeated independently, including absent optional fields supplied as null.
Each admitted object
also produces rejected field mutations. Objective decisions check truth,
injection adoption and an unrelated answer; absent decisions remain assigned,
with unknown cost. Round trips alone cannot establish admission correctness.
Input work scales with bytes; no historical rows survive a fuzz callback.
Observation identity and numeric fields are compared with the original JSON,
including omitted optional values. An idempotent decoder corruption can survive
a round trip; `observation-identity` plants one by clearing the decoded arm.
Each fuzz input also constructs one valid file for each public decoder, with
varying fixed-width identities, integer labels and finite or absent scores/costs.
These files must be admitted even when the original bytes are malformed.
`observation-refusal` makes the actual shared observation admission reject every
row; success-only decoder properties would silently skip that defect.
The generated decoded observation also enters both public pair projections.
Original input coordinates generate a distinct arm, a foreign population, a
foreign replicate and the same arm, including wrapping u64 boundaries. Four
independent score/cost availability combinations check forward and reverse
matched projections against original measurements and require foreign pairs to
be refused even when measurements are missing. This replaces the fixed
three-mismatch example loop. The kept `pair-population`, `pair-replicate` and
`pair-arm` controls each remove one actual production identity guard. This
checks offline pairing only; it establishes no provider or cut authenticity.
The same generated scorer drives an assignment history on every callback.
Input bytes select correct, injected, unrelated and unscored assignments, with
known or unknown prices. Independent category counts check every report rate
and cost denominator. Multiple integer prices are compared with an independent
u128 batch sum, using an explicit numerical precision budget rather than another
floating-point accumulation. Four category controls contribute prices 1, 2, 3
and 4, so failed work is included in every known-cost sum. Parallel public
histories with rejected negative, NaN and infinite prices must yield identical
subsequent reports. The retained `report-denominator` mutation replaces the
assigned denominator with valid decisions; `report-accumulation` substitutes
maximum for addition; `report-refusal` commits the assignment before validation.
The fractional-cost, empty/all-unscored and aggregate/counter-overflow unit
controls remain because this generated history does not subsume them.
Clustered statistics also run through the public `Cluster` and `PairedStats`
transitions. Generated integer clusters are compared with independent batch
sums and direct residuals, rather than another online QR implementation.
Parallel histories with refused nonfinite observations and an empty cluster
must yield identical subsequent public estimates. The retained `statistics-mean`
mutation drops the centered mean in the actual production estimate.
The same fold observes empty and single-cluster prefixes and requires the
public independent-cluster admission refusal. A late nonfinite error cannot
stand in for that refusal. After refusing an overflowing finite cluster, the
entire subsequent public estimate must still equal the untouched history.
`statistics-clusters` removes the actual independent-cluster guard;
`statistics-refusal` commits the actual next state before finite validation.
These properties replace the fixed hundred-replicate/generic-error example and
its partial private-field refusal assertions. The nonfinite planning
intermediate remains a separate control.
This bounded numerical domain does not replace large-offset, overflow or
arbitrary floating-point input controls. Comparisons use a numerical precision
budget scaled by input length; it is not a formal rounding-error proof.

The public `required_clusters` planning entry also receives bounded integer
parameters derived from the first four input bytes. An exact u128 rational
ceiling checks its result without repeating the floating-point implementation.
Each argument separately receives nonfinite and negative inputs, which must be
refused. The retained `planning-ceiling` mutation rounds down instead of up in
the actual production calculation. No new corpus is needed: `decision` already
selects a nonintegral result. General overflow and the nonzero-baseline score
example remain separate; zero-baseline pairing does not subsume subtraction
between two nonzero scores. The planning oracle covers these bounded integer
parameters, not arbitrary floating-point planning inputs or statistical design.

Run a campaign with an explicit budget using cargo-fuzz's supported toolchain:

```sh
cd rust
cargo +nightly fuzz run --no-default-features --features experiments experiment_files -- -max_total_time=30
cargo +nightly fuzz coverage --no-default-features --features experiments experiment_files
```

For a deterministic corpus replay, from `rust`:

```sh
cargo run --locked -p acyclic-sdk-fuzz --bin experiment_files --no-default-features --features experiments -- fuzz/corpus/experiment_files/*
```

From the repository root, plant one actual production defect and require the
unchanged fuzz oracle to reject it:

```sh
python3 rust/fuzz/experiment-negative-control.py decision-version
```

The same kept control accepts `scorer-version`, `observation-version`,
`scorer-distinct`, `trailing-input` or `decision-duplicate-value`.
The last mutation normalizes decision values through a JSON tree while keeping
the original typed version admission; repeated versions still fail, so a
version-only duplicate oracle cannot detect it. Multiple names share one original corpus
replay, then each changes exactly one production check, requires a failure from
the original admission oracle and restores the original source bytes before
the next control, even on failure. Select only controls still requiring a result.
A compiler error or arbitrary nonzero exit does not count as a detected defect.
The verdict requires a runtime panic at the unchanged target's source location
with the selected admission invariant, and rejects compiler diagnostics.
`scoring-truth` and `scoring-adoption` instead change the original production
comparison and require the same target's independent scoring assertion to fail.
The retained scoring property replaces the redundant three-answer example loop;
the separate conflicting-value decision refusal remains a unit control.
The same property subsumes fixed decoder examples for unsupported versions,
indistinguishable scorer labels, missing or unknown fields, positional arrays,
repeated identical fields and trailing input. Constructor admission, arithmetic overflow and fractional/maximum-cost examples
remain separate because the generated property does not subsume them.
Run controls on an isolated checkout; the script temporarily edits production
source. Original runtime output, coverage and negative-control results must be
recorded separately; target presence and fixed corpus replay are not a campaign.

## Corpus provenance

These are authored files for the library's own versioned offline input formats,
not recordings of external providers. Five seeds (`observation-known`,
`observation-unknown`, `decision-correct`, `decision-injected`,
`decision-malformed`) are byte-identical to the existing experiment corpus at
commit `52590337f0`; they exercise the same public decoders and preserve its
scorer-newline-decision protocol. `scorer` and `decision` extract its two original
objects. Four seeds change only version or scorer distinguishability at that same
format boundary. `observation-sparse` omits the original observation's two optional
fields, exercising duplicate-null mutations even when they were absent on input.
`scorer-wrapped-adjacent` supplies the two adjacent signed-integer endpoints.
It exercises both wrapping and the collision adjustment when choosing an
unrelated answer, so that answer remains distinct from truth and injection.
`trials-priced` is the single authored byte `04`, selecting a correct, priced
assignment before the four category controls. Every cost is known; the other
original JSON seeds start with an unpriced assignment. This is target input,
not a provider recording or a new persisted product format.
Prices are the upper five bits of each input byte, when its price-known bit is
set. The four appended category bytes are `0c 15 1e 27`, ensuring distinct
nonzero prices even for empty or unpriced input. No extra corpus is needed.
`statistics-baseline` is empty target input: the numerical property then checks
exactly the original unequal-cluster case `[1,3]`, `[5]`, `[-2,0,2]`. Nonempty
inputs add signed-byte integer clusters in chunks of three. The property replaces
the unit test's duplicate direct-residual assertions; its large common-offset
and power-planning checks remain.
No provider payload is fabricated or relabeled.

The target also retains the version-1 `PairAssignment` admission boundary.
Assignments use the same object schema property for version, required/unknown
fields, duplicate fields and surplus input, followed by canonical re-admission.
The original assignment corpus contains authored declarations produced with
ordered PowerShell hash tables and `ConvertTo-Json -Compress`, using population
bytes 0 through 31 and replicate zero. The original observation seeds include
unknown values and known zero cost. These are authored inputs, not provider
recordings. Streaming analysis remains separate. The library remains the single
parser and scorer; the target implements no decoder.
The original scorer-newline-decision inputs use the same scorer and decision
properties as standalone files. The former two-assignment report fold is
subsumed by the generated public trial histories, which cover all four outcomes,
known and unknown costs, rates and refusal state. No second report fold or
additional scorer decode is needed for that corpus protocol.

The generated observation's score and cost availability use independent bits
(`02` and `04`) of the first input byte. All four availability combinations
reach the original public decoder and the existing bidirectional pair property;
nonfinite generated measurements remain absent. The original `trials-priced`
input (`04`) supplies known cost without a score. The kept `pair-unscored-cost`
mutation wrongly drops known costs when both scores are missing. This subsumes
the fixed zero-valued availability examples, while nonzero-baseline subtraction
and maximum-cost controls remain distinct. No additional corpus file is needed.

Every decoded observation used by the generated pairing property first projects
its original population, replicate and arm through the same identity comparison
as standalone observation input. Identity corruption must fail that comparison
before pair projections can fail. The retained `observation-identity` mutation
still requires the original arm-field assertion, keeping source identity and
pair admission controls distinct without accepting an unrelated earlier panic.

The same pair property also receives a decoded signed-byte score and byte cost,
with a baseline in 1..=256. Expected forward differences use exact i16
subtraction before conversion to f64; reverse projections negate that reference.
This reaches two nonzero operands, which the arbitrary finite row's zero
baseline cannot expose even with reverse projections. The retained
`pair-nonzero-baseline-cost` and `pair-nonzero-baseline-score` mutations drop the
baseline only when both operands are nonzero. They use the existing
`trials-priced` and `decision` seeds. Measurement availability still uses the
independent original bits. Fixed nonzero score and unscored-cost subtraction
examples are subsumed; maximum-cost and nonfinite/overflow controls remain.

Original scalar types are checked before comparing decoded measurements:
observations admit only absent/null or numeric score/cost fields, and decisions
require an i64 value. A wrong type coerced to None must not compare equal to a
missing measurement. Three retained mutations change the actual private field
deserializer while preserving the original object/version/duplicate checks.
Their three authored seeds change only score, cost or decision value to a JSON
string in the existing own-format observation/decision files. They are not
external-provider fixtures. The two observation fields share one type property;
the scorer already checks its original fields with as_i64.

For an admitted observation, removing score, cost, or both must retain admission.
These three variants cover every omission combination of the two optional
fields, including when the original raw sparse input would be skipped by an
over-refusing decoder. Scalar type/projection checks precede the field's omission
check so the retained coercion controls still fail at their intended assertions.
Three retained omission mutations reuse the original observation-known seed;
no additional corpus or parser is needed.

Assignment admission uses the same constructive valid-file property on every
callback. Original population and replicate values must survive decoding, using
the same field comparison as observations. Admitted raw assignment files also
retain their original identities before canonical re-admission. This replaces
the fixed assignment schema example; the shared object property covers each
required, unknown, duplicate, version, sequence and trailing-input refusal.
The retained `assignment-refusal`, `assignment-population` and
`assignment-replicate` controls reject all valid declarations, erase the cut,
or erase the replicate in the actual production deserializer. Existing corpus
inputs select nonzero identities; no new input format or corpus is needed.

The same target also constructs three predeclared, ordered pairs across two
populations, including two replicates of one population. Both streams enter
through the buffered reader and public serde deserializers used by the
`experiment-analysis` example, then `Contrast::analyze`. Every population and
replicate declaration is independently substituted without changing row count;
missing/surplus declarations and actual malformed declaration tails must return
the original typed refusal. This replaces fixed declaration-substitution and
source-error examples. Three retained production mutations remove either
assignment-key guard or final declaration consumption. They reuse the original
assignment input. Inputs are authored own-format JSON; no provider or filesystem
IO conformance is inferred. Ordering, metric arithmetic and allocation controls
remain distinct, and source coverage still requires separate qualification.

The streamed property also keeps declarations and outcomes matched while
reusing a key, decreasing a replicate, decreasing a population immediately, or
returning to a closed population. These transformations preserve pair counts;
only strict order is broken. All population bytes and the starting replicate
come from the input, with two ordered population keys and nonoverflowing
replicates constructed before transformation. Two retained actual-source
mutations admit repeated replicates or decreasing populations. They reuse the
original assignment input and replace the fixed repeated/decreased-key loop.
No history map or alternate ordering implementation is introduced.

Declared arm orientation is checked independently at both members of all three
pairs: each replaced arm stays individually valid and distinct from its partner.
The shared pair check therefore cannot hide an omitted orientation guard. Three
retained source controls remove either arm guard or discard a failed baseline
row. Actual buffered observation JSON is truncated before every row position and
after all complete rows; the original typed source error must survive every valid
prefix. These properties replace the fixed orientation/observation-error examples;
the prior streamed declaration property also subsumes the fixed count examples.
No external-provider fixture or alternate parser is introduced.

Streamed metric reports use an independent integer batch reference over the same
three assigned pairs. All sixteen score/cost availability masks are rotated
across pairs, followed by one history with independent input-derived masks.
Matched pair counts, contributing population counts, estimate availability and
means are checked separately for scores and costs. Each arm's known-cost sum,
priced count, complete total and per-assignment mean include known costs even
when its partner is unpriced or both scores are missing. Empty streams retain
zero complete cost and no mean. This subsumes the two fixed streamed coverage
and arm-cost examples; maximum-cost overflow and allocation controls remain.
Five retained actual-source controls omit either metric, omit final cluster
completion or wrongly exclude either arm's unmatched costs. They reuse the
original assignment input. These bounded integer references do not establish
arbitrary floating-point accuracy; the separate statistics precision controls
and source-coverage obligation remain.

Short inputs give the streamed baseline and treatment distinct integer values,
so zeroing the shared metric mean cannot hide behind zero differences in the
retained corpus. The integer-reference precision budget scales with operand
magnitude, including cancellation. `analysis-metric-mean` zeros only the actual
streamed report projection; the standalone statistics implementation is unchanged.
To compare both unchanged oracles against that same defect in one invocation:

```sh
python3 rust/fuzz/experiment-negative-control.py --previous-oracle d0ecdc60266374b4981f71194fce72b813c1e6db analysis-metric-mean
```

The runner restores both production and oracle bytes, and accepts only a runtime
panic at the named metric-mean assertion as the repaired negative control.
