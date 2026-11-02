//! Process argument admission shared by the executable and its fuzz target.
use std::ffi::OsString;

use acyclic_harness::{agent::AgentPath, local::host_owner::HostAction};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The original argv for the agent parser, or decoded native-host arguments.
pub enum Input {
    /// Retains the program name and OS-native arguments for clap.
    Agent(Vec<OsString>),
    /// Native-host commands accept UTF-8 and omit the program name.
    Native(Vec<String>),
}

#[derive(Debug)]
/// A native-host argument could not be decoded without changing its bytes.
pub struct InvalidNativeArgument;

impl std::fmt::Display for InvalidNativeArgument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("step=decode native command arguments: expected UTF-8; recovery: run acyclic --help with UTF-8 arguments")
    }
}

impl std::error::Error for InvalidNativeArgument {}

/// Routes one owned argv without opening a workspace or copying its payloads.
pub fn parse(arguments: Vec<OsString>) -> std::result::Result<Input, InvalidNativeArgument> {
    let first = arguments.get(1).and_then(|argument| argument.to_str());
    if matches!(
        first,
        Some("run" | "serve" | "attach" | "exec" | "sessions" | "history")
    ) || first.is_some_and(|argument| {
        argument.starts_with('-')
            && !matches!(argument, "--help" | "-h" | "--version" | "-V" | "--task")
    }) {
        return Ok(Input::Agent(arguments));
    }
    arguments
        .into_iter()
        .skip(1)
        .map(|argument| argument.into_string().map_err(|_| InvalidNativeArgument))
        .collect::<std::result::Result<_, _>>()
        .map(Input::Native)
}

/// Admits a native workspace command before resolving its selected agent.
///
/// `resolve` runs at most once, only for a valid command needing the current
/// agent. Reopen already names its agent and never invokes the resolver.
pub fn workspace_action(
    arguments: &[String],
    resolve: impl FnOnce() -> Result<AgentPath>,
) -> Result<HostAction> {
    let (command, rest) = arguments
        .split_first()
        .ok_or("step=admit native command: expected COMMAND; recovery: run acyclic --help")?;
    Ok(match (command.as_str(), rest) {
        ("fs" | "discard", [_, ..]) => HostAction::Fs {
            task: resolve()?,
            command: arguments
                .iter()
                .skip(usize::from(command == "fs"))
                .map(|arg| format!("'{}'", arg.replace('\'', "'\"'\"'")))
                .collect::<Vec<_>>()
                .join(" "),
        },
        ("agents", []) => HostAction::Agents { task: resolve()? },
        ("log", []) => HostAction::Fs {
            task: resolve()?,
            command: "log".into(),
        },
        ("reopen", [task]) => HostAction::Reopen {
            task: AgentPath::try_from(task.clone()).map_err(|_| {
                "step=admit native command: invalid agent path; recovery: run acyclic reopen harness/agents/AGENT_ID"
            })?,
        },
        ("capture", []) => HostAction::Capture { task: resolve()? },
        _ => return Err("step=admit native command: expected fs COMMAND..., discard REF..., agents, log, capture or reopen ID; recovery: run acyclic --help".into()),
    })
}
