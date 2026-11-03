# Experiment file validation

`experiment_files` invokes the public `Observation::decode`, `Scorer::decode`
and `Decision::decode` entries, their original shared object decoder and the
production scorer/report fold. It covers the offline file formats already
exposed by the library, without an experiment runner or provider calls.

The oracle independently requires supported versions, required/known fields,
object shape, no duplicate fields and no trailing input. Each admitted object
also produces rejected field mutations. Objective decisions check truth,
injection adoption and an unrelated answer; absent decisions remain assigned,
with unknown cost. Round trips alone cannot establish admission correctness.
Input work scales with bytes; no historical rows survive a fuzz callback.

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
`scorer-distinct` or `trailing-input`. It first replays the original corpus,
then changes exactly one production check, requires a failure from the original
admission oracle and restores the original source bytes even on failure.
A compiler error or arbitrary nonzero exit does not count as a detected defect.
The verdict requires a runtime panic at the unchanged target's source location
with the selected admission invariant, and rejects compiler diagnostics.
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
objects. The four other seeds change only version or scorer distinguishability
at that same format boundary. No provider payload is fabricated or relabeled.

The target excludes the separate assignment/streaming analysis formats and
implements no decoder. The library remains the single parser and scorer.
