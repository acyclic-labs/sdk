//! Goal file state, per-settle continuation identities and finite loop policies.

use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::VolumeClass,
    goal::{GoalContinuation, GoalContinuationPolicy, GoalStatus, GoalView},
};
use serde::Deserialize;

#[path = "../goal_files/continuations.rs"]
mod continuations;
#[path = "../goal_files/parsing.rs"]
mod parsing;
#[path = "../goal_files/writes.rs"]
mod writes;

#[path = "../../../harness-fuzz/fuzz_targets/goal_parser_input.rs"]
mod goal_parser_input;

#[test]
fn checked_in_fuzz_corpus() -> Result<()> {
    let directory = std::path::Path::new(acyclic_harness::build_metadata::MANIFEST_DIR)
        .join("../harness-fuzz/corpus/goal_parser");
    let mut seeds = 0;
    for entry in std::fs::read_dir(directory).map_err(|error| Error::Storage(error.to_string()))? {
        let entry = entry.map_err(|error| Error::Storage(error.to_string()))?;
        let bytes =
            std::fs::read(entry.path()).map_err(|error| Error::Storage(error.to_string()))?;
        goal_parser_input::check(&bytes)?;
        seeds += 1;
    }
    assert!(seeds >= 14);
    Ok(())
}
