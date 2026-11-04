//! Original request admission, dispatch-once and physical reply retirement.
#![no_main]

use acyclic_harness::local::host_files::validation::WorkspaceInput;
use libfuzzer_sys::fuzz_target;

thread_local! {
    static INPUT: WorkspaceInput = WorkspaceInput::new().unwrap();
}

fuzz_target!(|bytes: &[u8]| {
    acyclic_harness::local::host_owner::check_native_session_storage(bytes);
    INPUT.with(|input| input.request(bytes).unwrap());
});
