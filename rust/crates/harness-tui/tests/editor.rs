//! Prompt editor properties, regression corpus, and rendering snapshots.

#[path = "editor/behavior.rs"]
mod behavior;
#[path = "editor/model.rs"]
mod model;
#[cfg(feature = "tui")]
#[path = "editor/render.rs"]
mod render;
#[path = "support/sequences.rs"]
mod sequences;

#[test]
fn checked_in_fuzz_corpus() {
    for seed in [
        include_bytes!("../fuzz/corpus/edit_sequences/unicode").as_slice(),
        include_bytes!("../fuzz/corpus/edit_sequences/paste").as_slice(),
        include_bytes!("../fuzz/corpus/edit_sequences/motions").as_slice(),
    ] {
        sequences::exercise(seed);
    }
}
