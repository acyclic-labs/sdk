//! Prompt editor properties, regression corpus, and rendering snapshots.

#[path = "editor/behavior.rs"]
mod behavior;
#[path = "editor/entry.rs"]
mod entry;
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
        include_bytes!("../fuzz/corpus/edit_sequences/continuation").as_slice(),
        include_bytes!("../fuzz/corpus/edit_sequences/kill-ring").as_slice(),
        include_bytes!("../fuzz/corpus/edit_sequences/history").as_slice(),
    ] {
        sequences::exercise(seed);
    }
}

#[test]
fn fuzz_round_trip_checks_leave_consecutive_kills_and_yanks_live() {
    let mut data = vec![0];
    data.extend_from_slice(b"one two");
    for op in [13, 13, 14, 15] {
        data.push(op);
        data.extend_from_slice(&[0; 7]);
    }
    assert_eq!(sequences::exercise(&data).text(), "one two");
}
