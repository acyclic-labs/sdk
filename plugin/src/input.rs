//! Native stdin and selected-task admission, before any workspace operation.
use acyclic_harness::{agent::AgentPath, local::host_owner::HostAction};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::io::{self, Read as _};

/// Admission result retaining the original parser or operating-system error.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Parse a selected task through the shared agent path contract.
///
/// # Errors
/// Returns the original agent path admission error for an invalid task.
pub fn parse_task(value: &str) -> Result<AgentPath> {
    Ok(serde_json::from_value(json!(value))?)
}

/// Admit a selected task without treating invalid native encoding as absence.
///
/// # Errors
/// Refuses non-UTF-8 identities and preserves canonical task admission errors.
pub fn parse_native_task(value: &std::ffi::OsStr) -> Result<AgentPath> {
    parse_task(value.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "step=admit selected native task: native task identity is not UTF-8; recovery: set ACYCLIC_TASK to a UTF-8 agent identity or unset it",
        )
    })?)
}

/// Read native JSON before workspace acquisition, with an optional frame bound.
///
/// # Errors
/// Preserves read and JSON errors, and refuses oversized hook frames.
pub fn read_json<T: DeserializeOwned>(input: impl io::Read, maximum: Option<u64>) -> Result<T> {
    let mut bytes = Vec::new();
    let mut input = input.take(maximum.map_or(u64::MAX, |bound| bound.saturating_add(1)));
    input.read_to_end(&mut bytes)?;
    if maximum.is_some_and(|bound| bytes.len() as u64 > bound) {
        return Err(
            "step=admit native hook: input exceeds its frame bound; recovery=acyclic --help".into(),
        );
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Debug)]
enum InvalidWorkspaceCommand {
    Usage,
    Agent(acyclic_harness::agent::Error),
}

impl std::fmt::Display for InvalidWorkspaceCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("step=admit native command: ")?;
        match self {
            Self::Usage => formatter.write_str("expected fs COMMAND..., discard REF..., agents, log, capture or reopen ID; recovery: run acyclic --help"),
            Self::Agent(cause) => write!(formatter, "{cause}; recovery: run acyclic reopen harness/agents/AGENT_ID"),
        }
    }
}

impl std::error::Error for InvalidWorkspaceCommand {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Usage => None,
            Self::Agent(cause) => Some(cause),
        }
    }
}

/// Admits a native workspace command before resolving its selected agent.
///
/// `resolve` runs at most once, only for a valid command needing the current
/// agent. Reopen already names its agent and never invokes the resolver.
///
/// # Errors
/// Refuses invalid command shapes and retains the original task or resolver error.
pub fn workspace_action(
    arguments: &[String],
    resolve: impl FnOnce() -> Result<AgentPath>,
) -> Result<HostAction> {
    let (command, rest) = arguments
        .split_first()
        .ok_or(InvalidWorkspaceCommand::Usage)?;
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
            task: AgentPath::try_from(task.clone()).map_err(InvalidWorkspaceCommand::Agent)?,
        },
        ("capture", []) => HostAction::Capture { task: resolve()? },
        _ => return Err(InvalidWorkspaceCommand::Usage.into()),
    })
}

/// Map native hook fields once, before the shared workspace boundary.
/// A non-idle Stop still belongs to Antigravity's running background work.
///
/// # Errors
/// Refuses unknown events and missing native retirement, workspace or session fields.
pub fn agy_hook(
    event: &str,
    input: &mut Value,
    workspace: Option<&str>,
) -> Result<Option<&'static str>> {
    let mapped = match event {
        "SessionStart" => "SessionStart",
        // The awaited admission must retry the shared capture even if the
        // native host ignored an earlier PostToolUse failure.
        "PreInvocation" | "PostToolUse" => "ToolBoundary",
        "Stop" => match input.get("fullyIdle").and_then(Value::as_bool) {
            Some(false) => return Ok(None),
            Some(true) => "Stop",
            None => return Err("step=admit Antigravity retirement: fullyIdle absent; recovery=update agy to a version reporting native background retirement".into()),
        },
        _ => return Err("step=admit Antigravity hook: unknown event; recovery=acyclic install agy".into()),
    };
    let cwd = workspace.filter(|path| !path.is_empty()).or_else(|| {
        let paths = input.get("workspacePaths")?.as_array()?;
        if paths.len() != 1 { return None; }
        paths.first()?.as_str().filter(|path| !path.is_empty())
    }).ok_or("step=admit Antigravity workspace: exact workspace absent; recovery=acyclic host agy -- --sandbox --mode accept-edits")?.to_owned();
    let session = input.get("conversationId").and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or("step=admit Antigravity session: conversationId absent; recovery=acyclic host agy -- --sandbox --mode accept-edits")?.to_owned();
    let object = input
        .as_object_mut()
        .ok_or("native hook input must be an object")?;
    object.insert("cwd".into(), json!(cwd));
    object.insert("session_id".into(), json!(session));
    Ok(Some(mapped))
}
