//! Checks accepted facade commands retain their complete typed wire representation.
#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_fs::{GitCommand, git_compat::parse_git_argv};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(command) = fuzz_support::FuzzInput::parse_text(fuzz_input, |text| {
        let argv: Vec<_> = text.split('\0').map(str::to_owned).collect();
        parse_git_argv::<std::convert::Infallible>(&argv, "agent", 0)
    }) {
        let encoded = serde_json::to_vec(&command)
            .unwrap_or_else(|error| unreachable!("command encoding failed: {error}"));
        let _ = fuzz_support::round_trip(&command, serde_json::from_slice::<GitCommand>(&encoded));
    }
});
