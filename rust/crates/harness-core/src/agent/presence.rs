//! Public session metadata, projected without private control or file authority.
use super::{Error, Phase, WorkspaceEntry, WorkspaceRole};
use acyclic_actors::route::NodeId;
use serde::{Deserialize, Serialize};

mod registry;
pub use registry::{SessionPublication, SessionRegistry};

/// Public phase only; approval bindings and host-local paths stay on their node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    /// No queued input.
    Idle,
    /// Waiting for user input.
    WaitingUser,
    /// Executing or holding accepted input.
    Running,
    /// Waiting for an approval.
    WaitingApproval,
    /// Waiting for the host.
    WaitingHost,
    /// Waiting for another event.
    Waiting,
    /// Terminal.
    Done,
}

impl From<&Phase> for SessionPhase {
    fn from(phase: &Phase) -> Self {
        match phase {
            Phase::Idle => Self::Idle,
            Phase::WaitingUser { .. } => Self::WaitingUser,
            Phase::Running => Self::Running,
            Phase::WaitingApproval { .. } => Self::WaitingApproval,
            Phase::WaitingHost { .. } => Self::WaitingHost,
            Phase::Waiting { .. } => Self::Waiting,
            Phase::Done { .. } => Self::Done,
        }
    }
}

/// The change-sensitive part of an account session row. Constructed by the
/// owning host from its already admitted metadata, never by reading peer files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionProfile {
    /// Public phase, without the underlying approval or control record.
    pub phase: SessionPhase,
    /// Current display title.
    pub title: String,
    /// User workspaces in primary-first order; harness-owned entries are absent.
    /// These references convey no file access or session-control authority.
    pub workspaces: Vec<WorkspaceEntry>,
    /// UTF-8 prefix bounded by the publisher's explicit display budget.
    pub goal_preview: String,
}

impl SessionProfile {
    /// Project one admitted metadata change. The preview budget bounds display
    /// work, not the underlying goal; changes past that prefix publish nothing.
    pub fn project(
        phase: &Phase,
        title: String,
        workspaces: &[WorkspaceEntry],
        goal: &str,
        preview_bytes: usize,
    ) -> Self {
        let mut end = goal.len().min(preview_bytes);
        while !goal.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            phase: phase.into(),
            title,
            workspaces: workspaces
                .iter()
                .filter(|entry| entry.role == WorkspaceRole::User)
                .cloned()
                .collect(),
            goal_preview: goal[..end].into(),
        }
    }
}

/// Version-one account registry head. The owning node joins its changes to the
/// original metadata publication; discovering a row grants no actor authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRow {
    /// Immutable root location.
    pub node: NodeId,
    /// Public metadata selected on change, or a removal after opt-out/archive.
    pub profile: Option<SessionProfile>,
    /// One-based publication revision; unchanged metadata spends no revision.
    pub rev: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RowWire {
    version: u32,
    node: Vec<u8>,
    phase: Option<SessionPhase>,
    title: Option<String>,
    workspaces: Option<Vec<WorkspaceEntry>>,
    goal_preview: Option<String>,
    rev: u64,
    #[serde(default)]
    removed: bool,
}

impl Serialize for SessionRow {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct as _;
        let mut wire = serializer
            .serialize_struct("SessionRow", if self.profile.is_some() { 7 } else { 4 })?;
        wire.serialize_field("version", &1_u32)?;
        wire.serialize_field("node", self.node.as_bytes())?;
        if let Some(profile) = &self.profile {
            wire.serialize_field("phase", &profile.phase)?;
            wire.serialize_field("title", &profile.title)?;
            wire.serialize_field("workspaces", &profile.workspaces)?;
            wire.serialize_field("goal_preview", &profile.goal_preview)?;
        } else {
            wire.serialize_field("removed", &true)?;
        }
        wire.serialize_field("rev", &self.rev)?;
        wire.end()
    }
}

impl SessionRow {
    /// Initial membership row, joined to the original root creation.
    pub fn new(node: NodeId, profile: SessionProfile) -> Self {
        Self {
            node,
            profile: Some(profile),
            rev: 1,
        }
    }

    /// Decode one committed row. Other versions are refused, never migrated.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let wire: RowWire =
            serde_json::from_slice(bytes).map_err(|_| Error::Record("invalid session row"))?;
        if wire.version != 1 || wire.rev == 0 {
            return Err(Error::Record("unsupported session row"));
        }
        let profile = match (
            wire.removed,
            wire.phase,
            wire.title,
            wire.workspaces,
            wire.goal_preview,
        ) {
            (false, Some(phase), Some(title), Some(workspaces), Some(goal_preview)) => {
                Some(SessionProfile {
                    phase,
                    title,
                    workspaces,
                    goal_preview,
                })
            }
            (true, None, None, None, None) => None,
            _ => return Err(Error::Record("inconsistent session removal")),
        };
        Ok(Self {
            node: NodeId::new(&wire.node).map_err(|_| Error::Record("invalid session node"))?,
            profile,
            rev: wire.rev,
        })
    }

    /// Select a publication only when visible metadata changes. Root location
    /// stays fixed, and no stream observation, task or timer is created here.
    /// `None` removes membership and discards the previous public metadata.
    pub fn changed(&self, profile: Option<SessionProfile>) -> Result<Option<Self>, Error> {
        if self.profile == profile {
            return Ok(None);
        }
        let rev = self
            .rev
            .checked_add(1)
            .ok_or(Error::Record("session row revision overflow"))?;
        Ok(Some(Self {
            node: self.node.clone(),
            profile,
            rev,
        }))
    }

    /// Join one head update to the caller's original commit. The row owns this
    /// stream; a stale projection cannot overwrite a later metadata revision.
    /// The caller must authorize the owning node and the root's membership.
    pub fn effect(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::CommitEffect, Error> {
        use acyclic_stream::{AppendRecord, CommitCondition, CommitEffect, CommitMutation};
        let previous = self
            .rev
            .checked_sub(1)
            .ok_or(Error::Record("invalid session row revision"))?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| Error::Record("session row cannot encode"))?;
        let record = AppendRecord::latest(bytes, "session", false).map_err(Error::Stream)?;
        Ok(CommitEffect::hard(
            vec![CommitCondition::Tail {
                path: path.clone(),
                expected: previous,
            }],
            vec![CommitMutation::Append {
                path,
                records: vec![record],
            }],
        ))
    }
}
