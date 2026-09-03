//! Emits one canonical subset of the deterministic workload corpus.

use acyclic_conformance::filesystem::{
    SelectionLimits, parse_workload_selector_arguments, selected_workload_corpus_json,
};
use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let selectors = parse_workload_selector_arguments(env::args().skip(1))?;
    println!(
        "{}",
        selected_workload_corpus_json(&selectors, SelectionLimits::default())?
    );
    Ok(())
}
