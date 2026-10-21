#![no_main]

use acyclic_harness::{Error, Result};
use libfuzzer_sys::fuzz_target;

#[path = "../../harness-core/src/tools/agent_selection.rs"]
mod agent_selection;
#[path = "../../harness-core/src/tools/command.rs"]
mod command;
#[path = "../../harness-core/src/tools/fs_command.rs"]
mod fs_command;
use agent_selection::{AgentTarget, selection};
use command::parse_command;
use fs_command::parse_fs_command;

// Independent spelling of the agent namespace rule: split on '/'.
fn agent_reference(input: &str) -> Option<AgentTarget> {
    let parsed = parse_fs_command(input).ok()?;
    if parsed.volume.is_some() || parsed.arguments.len() != 2 {
        return None;
    }
    let parts: Vec<&str> = parsed.arguments[1].split('/').collect();
    let inside = parts.len() > 2 && parts[0] == ".volumes" && parts[1] == "agents";
    match (parsed.arguments[0].as_str(), parts.as_slice()) {
        ("ls", [".volumes", "agents"]) => Some(AgentTarget::Children),
        ("ls", _) if inside => Some(AgentTarget::Child),
        ("cat", _) if inside => Some(AgentTarget::File),
        _ => None,
    }
}

fuzz_target!(|input: &str| {
    // Selection is total, matches the reference and owns exactly the operand.
    let selected = selection(input);
    assert_eq!(
        selected.as_ref().map(|inspection| inspection.target),
        agent_reference(input)
    );
    if let Some(inspection) = &selected {
        assert_eq!(
            parse_fs_command(input).unwrap().arguments[1],
            inspection.path
        );
        assert!(inspection.path.ends_with(inspection.operand()));
    }
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
            fs_command::agent_diff(&restored),
            fs_command::agent_diff(&arguments)
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
