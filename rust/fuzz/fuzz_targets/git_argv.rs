//! Checks accepted facade commands retain their complete typed wire representation.
#![no_main]

use acyclic_fs::{GitCommand, git_compat::parse_git_argv};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let argv: Vec<_> = text.split('\0').map(str::to_owned).collect();
    if let Ok(command) = parse_git_argv::<std::convert::Infallible>(&argv, "agent", 0) {
        let restored = serde_json::to_vec(&command)
            .ok()
            .and_then(|wire| serde_json::from_slice::<GitCommand>(&wire).ok());
        assert_eq!(restored.as_ref(), Some(&command));
    }
});
