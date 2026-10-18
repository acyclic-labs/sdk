//! Typed prompt events, persistence and presentation checks.
#![cfg(feature = "tui")]

#[path = "input/args.rs"]
mod args;

#[path = "input/input.rs"]
mod input;
#[path = "input/keys.rs"]
mod keys;
#[path = "input/native.rs"]
mod native;
#[path = "input/properties.rs"]
mod properties;
#[path = "input/render.rs"]
mod render;
#[path = "support/input_sequences.rs"]
mod sequences;

#[test]
fn input_regression_corpus() {
    for seed in [
        include_bytes!("../fuzz/corpus/input_events/keys").as_slice(),
        include_bytes!("../fuzz/corpus/input_events/ime").as_slice(),
        include_bytes!("../fuzz/corpus/input_events/config").as_slice(),
        include_bytes!("../fuzz/corpus/input_events/history").as_slice(),
        include_bytes!("../fuzz/corpus/input_events/super").as_slice(),
        include_bytes!("../fuzz/corpus/input_events/windows-editor").as_slice(),
    ] {
        sequences::exercise(seed);
    }
}

#[test]
#[should_panic(expected = "committed text replaces the selection")]
fn fuzz_negative_control_rejects_broken_selection_replacement() {
    sequences::exercise_broken(b"negative control");
}

#[path = "input/routing_allocations.rs"]
mod routing_allocations;
