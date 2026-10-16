#![no_main]

use acyclic_harness::tools::parse_command;
use libfuzzer_sys::fuzz_target;

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
        assert_eq!(parse_command(&encoded).unwrap(), arguments);
    }
});
