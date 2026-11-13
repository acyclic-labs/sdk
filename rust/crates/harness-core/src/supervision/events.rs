//! Versioned lifecycle and supervision feed shared by every harness client.

use super::{Action, Kind, Signal};
use crate::{OperationId, agent::AgentPath};
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

/// Immutable source record establishing one feed event. No observer clock,
/// guessed time or current filesystem generation replaces the original cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    /// Original stream, admitted under the owner's ordinary authority.
    pub stream: StreamPath,
    /// Original zero-based source sequence.
    pub sequence: u64,
}

/// One JSONL value on the general lifecycle/supervision event stream. The same
/// schema is available in process, through a remote session, and to file views.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Schema version, always one. Consumers refuse unsupported versions.
    pub version: u32,
    /// Agent owning the event; its tree root identifies the local session.
    pub agent: AgentPath,
    /// Original run identity; a later run never reuses its counters or signals.
    /// Null for explicit owner controls outside any particular run.
    pub run: Option<OperationId>,
    /// Original record establishing the event.
    pub source: Position,
    /// Lifecycle or query observation.
    pub event: Lifecycle,
}

/// General event vocabulary, independent of any CLI, deployment or consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Lifecycle {
    /// The original run was admitted.
    Started {
        /// Original accepted inbox input, allowing clients to correlate a run
        /// before its first turn settles.
        input: u64,
    },
    /// An original turn settled.
    TurnDone {
        /// Original turn identity.
        turn: OperationId,
    },
    /// A successful original PR publication.
    #[serde(rename = "pr_opened")]
    PullRequestOpened {
        /// Original PR URL; never inferred merely from a command's text.
        url: String,
    },
    /// An incremental supervision query fired.
    Signal {
        /// Exact value projected as the signal's status file.
        signal: Signal,
        /// Owner policy at the same original source cut.
        action: Action,
    },
    /// The execution owner durably parked this agent.
    Paused {
        /// Signal causing the transition; null means an explicit control file.
        signal: Option<Kind>,
    },
    /// The owner reopened admission after an explicit control change.
    Resumed {},
    /// A committed file revision could not replace its admitted control state.
    ControlRejected {
        /// Public file name, without any rejected content.
        path: String,
    },
    /// The execution owner durably stopped this agent.
    Stopped {
        /// Signal causing the transition; null means an explicit control file.
        signal: Option<Kind>,
    },
}
