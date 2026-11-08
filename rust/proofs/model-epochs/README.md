# Epoch proof scope

`src/lib.rs` includes the production epoch kernel and swap reducer directly.
Run a named harness with, for example:

```sh
cargo kani --manifest-path rust/proofs/model-epochs/Cargo.toml --harness cuts_only_advance_at_complete_contiguous_exchanges
```

The cut harness checks three successive pushes from an arbitrary `u64` cursor.
An independent `u128` counter checks contiguous observation, including exhaustion;
the selected cut advances only at closed exchanges. Every refused push must leave
the entire production state unchanged. This covers retained open exchanges and
refusal after an open exchange, which a single initial push cannot check. It is
a bounded trace proof, not an induction over arbitrary history.

The arithmetic harnesses check full-width scalar inputs. The swap harnesses check
selected reducer transitions with supplied terminal outcomes. They do not check
renderer input provenance, storage publication, conditional-commit recovery,
producer-loss notification, funding, or physical resource release. The kernel
proofs alone do not establish that the executor uses these functions.

The current reducer harnesses have the following source-level coverage. This
table describes their assertions, rather than claiming an executed proof result.

| Production operation | Checked cases | Missing cases |
| --- | --- | --- |
| `State::restore` | Arbitrary full-width cursor/head/generation, absent or arbitrary pending value; exact range/stale refusal and preserved admitted fields | Storage provenance |
| `State::prepare` | New valid preparation | Duplicate intent, competing intent and invalid range |
| `State::complete` | Ready/Failed and contradictory terminal result | Absent or foreign pending identity, identical terminal replay |
| `State::demand` | Unprepared, Await, Swap and Failed from every admitted restored state; Unprepared after swap | Producer observation |
| `State::swap` | Arbitrary restored state and request: absent/foreign identity, incomplete/failed terminal, stale/exhausted generation, regressed head and successful exact cut; every refusal leaves the complete state unchanged | Storage conditional commit |
| `State::fork` | Pending, settled inherited prefix and prefix exclusion | Failed pending and empty state |

The fan-out harness checks a retained `Pending` value. It does not observe the
producer itself, so its name refers to pending identity rather than producer
lifetime.

The single `restore_and_swap_preserve_the_exact_prepared_identity` harness
replaces the narrower ready-state stale/exhaustion harness. It checks the actual
restore, demand and swap functions with arbitrary scalar and identity inputs,
without constructing a second reducer. Cover goals require each restore refusal,
absent and foreign swap identities, every terminal refusal and successful swap
to be reachable. Run it with:

```sh
cargo kani --manifest-path rust/proofs/model-epochs/Cargo.toml --harness restore_and_swap_preserve_the_exact_prepared_identity
```

The retained production mutation admits an unfinished summary as ready. The
same harness must fail while this mutation is applied; restore it before the
positive run:

```sh
git apply rust/proofs/model-epochs/mutations/admit-unfinished-summary.patch
git apply -R rust/proofs/model-epochs/mutations/admit-unfinished-summary.patch
```

These commands describe the qualification procedure, not an executed result.

`epochs.qnt` is a separate design model with bounded agents, heads and proposals.
It has no checked refinement to the Rust implementation. Its `omitSummary`
negative control checks that model's safety predicate, rather than a planted
defect in production Rust. Keep its results separate from production guarantees.
