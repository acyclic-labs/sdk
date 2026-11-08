#![no_main]

use acyclic_harness::test_support::separated_local_state_directory;
use libfuzzer_sys::fuzz_target;

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
    assert!(separated_local_state_directory(&state, Some(checkout.path())).is_err());
    assert_eq!(std::fs::read_dir(checkout.path()).unwrap().count(), 0);
});
