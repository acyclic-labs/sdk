#![no_main]

use acyclic_harness::{Error, Result};
use libfuzzer_sys::fuzz_target;

// Exercise the production boundary without adding a public verification API.
#[path = "../../harness/src/local/owner/directory.rs"]
mod directory;

fuzz_target!(|bytes: &[u8]| {
    let checkout = tempfile::tempdir().unwrap();
    let mut state = checkout.path().to_path_buf();
    for byte in bytes.iter().take(8) {
        state.push(format!("missing{byte}"));
        if byte & 1 == 0 {
            state.push("..");
        }
    }
    state.push("state");
    assert!(directory::separated_directory(&state, Some(checkout.path())).is_err());
    assert_eq!(std::fs::read_dir(checkout.path()).unwrap().count(), 0);
});
