//! Child membership comes from the same settled status files as gc-agents.
use super::{Component, Error, Result};
use serde::{Deserialize, Serialize};

/// Harness acknowledgement of a child's execution state.
/// Results remain in the named task's ordinary completion and chunk files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct ChildStatus {
    /// Current or most recently settled turn.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub task: crate::TaskId,
    /// Lifecycle state reached by the worker.
    pub state: ChildState,
    /// All effects of the current turn have stopped or completed.
    pub settled: bool,
    /// Immutable request/inbox files included in this turn.
    pub inputs: u64,
}

impl ChildStatus {
    /// Decodes a status acknowledgement; terminal and interrupted workers must
    /// acknowledge quiescence before reporting those states.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let status: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidSegment)?;
        if status.inputs == 0
            || (matches!(
                status.state,
                ChildState::Cancelled | ChildState::Archived | ChildState::Interrupted
            ) && !status.settled)
        {
            return Err(Error::InvalidSegment);
        }
        Ok(status)
    }
}

/// Files owned by a parent below `agents/<ref>/`.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub enum ChildFile {
    /// Immutable spawn prompt with optional model and tool frontmatter.
    Request,
    /// Immutable message, identified by the sender's operation identity.
    Inbox(Component),
    /// Latest lifecycle intent. Publication is fenced by the task-volume cut.
    Control,
    /// Harness-owned execution status, followed by the parent and UI.
    Status,
    /// Immutable outcome of one completed mailbox input.
    Result(u64),
}

impl ChildFile {
    pub(super) fn parse(parts: &[&str]) -> Result<Self> {
        match parts {
            ["request.md"] => Ok(Self::Request),
            ["inbox", name] => Ok(Self::Inbox(Component::new(
                name.strip_suffix(".md").ok_or(Error::InvalidPath)?,
            )?)),
            ["control"] => Ok(Self::Control),
            ["status.json"] => Ok(Self::Status),
            ["results", name] => {
                let number = name.strip_suffix(".json").ok_or(Error::InvalidPath)?;
                let turn: u64 = number.parse().map_err(|_| Error::InvalidPath)?;
                if turn == 0 || turn.to_string() != number {
                    return Err(Error::InvalidPath);
                }
                Ok(Self::Result(turn))
            }
            _ => Err(Error::InvalidPath),
        }
    }

    pub(super) fn path(&self) -> String {
        match self {
            Self::Request => "request.md".into(),
            Self::Inbox(id) => format!("inbox/{}.md", id.as_str()),
            Self::Control => "control".into(),
            Self::Status => "status.json".into(),
            Self::Result(turn) => format!("results/{turn}.json"),
        }
    }
}

/// Desired execution state, folded from accepted control-file publications.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ChildState {
    /// Child may start or continue work.
    #[default]
    Running,
    /// Child retains work but may not advance it.
    Paused,
    /// Current turn has been interrupted.
    Interrupted,
    /// Child has been cancelled permanently.
    Cancelled,
    /// Child is frozen permanently and omitted from active lists.
    Archived,
}

impl ChildStatus {
    /// Terminal quiescence removes the child from active lists, retaining its files.
    pub fn active(self) -> bool {
        !self.settled || !matches!(self.state, ChildState::Archived | ChildState::Cancelled)
    }
}
