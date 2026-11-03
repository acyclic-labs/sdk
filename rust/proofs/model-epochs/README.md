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

`epochs.qnt` is a separate design model with bounded agents, heads and proposals.
It has no checked refinement to the Rust implementation. Its `omitSummary`
negative control checks that model's safety predicate, rather than a planted
defect in production Rust. Keep its results separate from production guarantees.
