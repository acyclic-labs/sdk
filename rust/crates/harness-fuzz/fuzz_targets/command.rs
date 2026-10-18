#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{Error, Result};

#[path = "../../harness/src/tools/command.rs"]
mod command;
#[path = "../../harness/src/tools/fs_command.rs"]
mod fs_command;
use command::parse_command;
use fs_command::parse_fs_command;

fuzz_support::fuzz_target!(|input: &str, fuzz_input| {
    if let Some(arguments) = fuzz_support::FuzzInput::parse_text(fuzz_input, parse_command) {
        assert!(!arguments.is_empty());
        let encoded = arguments
            .iter()
            .map(|argument| {
                assert!(!argument.contains('\0'));
                format!(
                    "\"{}\"",
                    argument.replace('\\', "\\\\").replace('"', "\\\"")
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let restored = fuzz_support::round_trip(&arguments, parse_command(&encoded));
        assert_eq!(
            fs_command::agent_diff(&restored),
            fs_command::agent_diff(&arguments)
        );
    }
    if let Some(parsed) = fuzz_support::FuzzInput::parse_text(fuzz_input, parse_fs_command) {
        let mut arguments = parsed.arguments.clone();
        if let Some(volume) = &parsed.volume {
            arguments.splice(0..0, ["--volume".into(), volume.clone()]);
        }
        let encoded = arguments
            .iter()
            .map(|argument| format!("'{}'", argument.replace('\'', "'\\''")))
            .collect::<Vec<_>>()
            .join(" ");
        let _ = fuzz_support::round_trip(&parsed, parse_fs_command(&format!("git {encoded}")));
    }
});
