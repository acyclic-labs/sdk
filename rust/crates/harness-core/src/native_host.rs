//! Native-host workspace commands shared by clients and their runtime owner.
use crate::{
    OperationId, Outcome,
    agent::{AgentPath, IntentId},
    executor::TurnOutput,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

/// Workspace operations only. Native hosts retain every agent and tool process.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostAction {
    /// Validation snapshot of the view owner, using the shared OS sampler and
    /// N/2N oracle. Requires runtime validation support; otherwise refused.
    /// It never samples or executes a native child process.
    ResourceProbe {
        /// Agent authorizing the query.
        task: AgentPath,
        /// Retained child population at this measurement.
        agents: u64,
        /// Prior N snapshot: population, descriptors, threads and mounts.
        previous: Option<[u64; 4]>,
    },
    /// The parent's open spawned children, by parent-local name.
    Sessions {
        /// Owning parent agent.
        parent: AgentPath,
    },
    /// Native session lifecycle as the child agent's inbox records.
    Session {
        /// Owning parent agent.
        parent: AgentPath,
        /// Parent-local child name.
        reference: String,
        /// Session operation; never executes the host.
        action: SessionAction,
        /// Also claim the session's next input under the physical fence of
        /// this same exchange, returned as `next`: null when the fence is
        /// held or nothing is claimable. A claim the fence proves unrun is
        /// handed out again under its exact identity.
        #[serde(default)]
        next: bool,
    },
    /// Translate a native lifecycle event into workspace coordination only.
    Hook {
        /// Native host name.
        host: String,
        /// Native lifecycle event.
        event: String,
        /// Native event payload, without transcripts or credentials.
        input: Value,
    },
    /// Resolve an attached agent by its native working directory.
    Locate {
        /// Native tool working directory.
        cwd: PathBuf,
    },
    /// Create or reopen the native root's stable agent.
    Open {
        /// Native host name.
        host: String,
        /// Native root session identity.
        session: String,
    },
    /// Reattach an existing native agent's lazy view.
    Reopen {
        /// Existing native agent.
        task: AgentPath,
    },
    /// Capture completed native writes.
    Capture {
        /// Quiesced native agent.
        task: AgentPath,
    },
    /// Invoke the shared workspace git facade, including merge.
    Fs {
        /// Agent selecting the workspace and grants.
        task: AgentPath,
        /// Ordinary shared filesystem command.
        command: String,
    },
    /// The agent's open children and their sessions.
    Agents {
        /// Parent agent.
        task: AgentPath,
    },
}

/// Coordination only. Native session processes are owned by the plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionAction {
    /// Spawn the child with its initial input; the reply names the prompt.
    Spawn {
        /// Initial prompt.
        prompt: String,
    },
    /// Queue a follow-up input; the reply names the prompt.
    Send {
        /// Message body.
        prompt: String,
    },
    /// Claim the next input, after the preceding process is quiescent. The
    /// accepting request may replay its claim; another request requires a
    /// fenced handoff. A reply always names the current physical view.
    Next,
    /// Capture and settle a process that has exited, including failed writes.
    /// The native resume id is derived from the turn's own custodied event
    /// log, so a running turn never spends an exchange recording it.
    Complete {
        /// Exact original execution claim, returned by Next.
        claim: TurnClaim,
        /// Native outcome.
        outcome: Outcome<TurnOutput>,
    },
    /// Request a native-session lifecycle state.
    Control {
        /// Requested lifecycle state.
        control: Control,
    },
    /// Read the session and its native resume reference.
    Observe,
    /// Where prompt `id`, accepted at inbox sequence `inbox`, stands: the
    /// shared committed reply, with the outcome its settled turn recorded.
    /// The response carries that outcome once in `reply`, without `results`.
    Reply {
        /// The prompt's message identity, returned by Spawn or Send.
        id: IntentId,
        /// The prompt's inbox sequence, returned by Spawn or Send.
        inbox: u64,
    },
    /// After fencing the old process custody, settle an orphaned turn.
    Recover,
}

/// Requested lifecycle of a native session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    /// Run inputs as they arrive.
    #[default]
    Resume,
    /// Stop the running input; later inputs may resume the session.
    Interrupt,
    /// Stop the running input and drop queued inputs.
    Cancel,
    /// Stop and close the session.
    Archive,
}

/// The exact claim a native process executes under; returned by `Next` and
/// presented again to settle it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnClaim {
    /// The session's agent.
    pub agent: AgentPath,
    /// Claim operation.
    pub operation: OperationId,
    /// One-based turn number.
    pub turn: u64,
}
