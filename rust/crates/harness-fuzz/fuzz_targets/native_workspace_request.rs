//! Original request admission, dispatch-once and physical reply retirement.
#![no_main]

use acyclic_harness::local::host_files::validation::WorkspaceInput;
use libfuzzer_sys::fuzz_target;

thread_local! {
    static INPUT: WorkspaceInput = WorkspaceInput::new().unwrap();
}

fuzz_target!(|bytes: &[u8]| {
    INPUT.with(|input| input.request(bytes).unwrap());
});
