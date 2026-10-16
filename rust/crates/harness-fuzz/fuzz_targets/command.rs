#![no_main]

use acyclic_harness::{Error, Result};
use libfuzzer_sys::fuzz_target;

#[path = "../../harness/src/tools/command.rs"]
mod command;
use command::{parse_command, parse_fs_command};

fuzz_target!(|input: &str| {
    if let Ok(arguments) = parse_command(input) {
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
        let restored = parse_command(&encoded).unwrap();
        assert_eq!(restored, arguments);
        assert_eq!(
            command::agent_diff(&restored),
            command::agent_diff(&arguments)
        );
    }
    if let Ok(parsed) = parse_fs_command(input) {
        let mut arguments = parsed.arguments.clone();
        if let Some(volume) = &parsed.volume {
            arguments.splice(0..0, ["--volume".into(), volume.clone()]);
        }
        let encoded = arguments
            .iter()
            .map(|argument| format!("'{}'", argument.replace('\'', "'\\''")))
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(parse_fs_command(&format!("git {encoded}")).unwrap(), parsed);
    }
});
