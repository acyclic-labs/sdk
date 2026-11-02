//! Fuzz the original native version and installed hook ownership decoders.
#![no_main]

use acyclic_harness::host_hooks::{
    CLAUDE_EVENTS, check_codex_version, claude_hook_executable, claude_hooks,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = check_codex_version(text);
    if let Ok(group) = serde_json::from_str(text) {
        for event in CLAUDE_EVENTS {
            if let Some(executable) = claude_hook_executable(event, &group) {
                // Ownership must survive reconstruction by the one manifest
                // producer, including its original shell argument decoder.
                let hooks = claude_hooks(&executable);
                assert_eq!(
                    claude_hook_executable(event, &hooks[event][0]),
                    Some(executable)
                );
            }
        }
    }
});
