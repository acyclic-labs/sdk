//! The shipping filesystem descriptor reader, including workspace authority.
#![no_main]

use acyclic_harness::local::host_files::validation::WorkspaceInput;
use libfuzzer_sys::fuzz_target;

thread_local! {
    static INPUT: WorkspaceInput = WorkspaceInput::new().unwrap();
}

fuzz_target!(|bytes: &[u8]| {
    INPUT.with(|input| input.endpoint(bytes).unwrap());
});
