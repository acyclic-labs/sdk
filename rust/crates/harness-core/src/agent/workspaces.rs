//! Version-one workspace data and its pure, ordered inbox fold.
use super::{AgentPath, Error};
pub use crate::contract::EntryId;
use crate::conversation::VolumeRef;
use serde::{Deserialize, Serialize};

/// Harness features select a role, while user entries have no reserved name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceRole {
    /// A user repository or directory; the first one is primary.
    User,
    /// Agent configuration, goals, plugins and skills.
    Home,
    /// Model-visible UI state.
    Private,
    /// Harness-only interaction state, never announced or granted to tools.
    Internal,
    /// Harness-written outputs, read-only to the model.
    Output,
    /// Writable background-job controls, separate from read-only outputs.
    Jobs,
    /// Session-tree board with independent conditional writes.
    Shared,
    /// Authorised lazy hierarchy view.
    Tree,
}

/// The only optional path-aware storage policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceFilter {
    /// Every path is content.
    None,
    /// Git-compatible capture, merge and materialisation semantics.
    Git,
}

/// How a workspace is served, independent of its model-facing name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceKind {
    /// An immutable-generation filesystem volume.
    Volume,
    /// A lazy, authorised hierarchy of agents and their published workspaces.
    Tree,
}

/// Original source identity. A subtree binding retains one root, never peers.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceSource {
    /// A filesystem volume, including its provider and owner.
    Volume(VolumeRef),
    /// The root of an authorised stream subtree, traversed by pages and follow.
    Subtree(AgentPath),
}

/// The access mode compiled into each target's workspace policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceMode {
    /// Read-only access.
    #[serde(rename = "ro")]
    ReadOnly,
    /// Read and write access.
    #[serde(rename = "rw")]
    ReadWrite,
}

/// How adoption derives a child's source from its parent's binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceInherit {
    /// Fork the immutable generation.
    Fork,
    /// Retain the same source.
    Share,
    /// Create the child's own source.
    Own,
}

/// One workspace in the version-one table; names and sources may change, ids do not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceEntry {
    /// Identity minted at Add and retained across rename, re-root and fork.
    pub id: EntryId,
    /// Stable harness-feature selection, independent of the name.
    pub role: WorkspaceRole,
    /// Unique lowercase word, using the target roster's name contract.
    pub name: String,
    /// Serving mechanism; no path or name selects it implicitly.
    pub kind: WorkspaceKind,
    /// Original volume or authorised stream subtree.
    pub source: WorkspaceSource,
    /// Normalised relative source path; empty selects the whole source.
    pub root: String,
    /// Read-only or read-write policy.
    pub mode: WorkspaceMode,
    /// Child adoption policy.
    pub inherit: WorkspaceInherit,
    /// Optional git semantics; harness-owned entries use `none`.
    pub filter: WorkspaceFilter,
}

impl WorkspaceEntry {
    /// Mint the initial stable slot for a composition's explicitly selected
    /// volume and role. Replays of the same initial binding retain its id;
    /// rename, re-root and spawn preserve that id by updating this entry.
    pub fn initial_volume(role: WorkspaceRole, name: String, volume: VolumeRef) -> Self {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.workspace.initial-slot.v1");
        let mut part = |text: &str| {
            hash.update(&(text.len() as u64).to_be_bytes());
            hash.update(text.as_bytes());
        };
        part(volume.provider().namespace());
        part(volume.provider().family());
        part(volume.provider().version());
        part(volume.id());
        part(match volume.class() {
            crate::conversation::VolumeClass::Project => "project",
            crate::conversation::VolumeClass::AgentPrivate => "private",
            crate::conversation::VolumeClass::SessionShared => "shared",
        });
        part(&name);
        match volume.owner() {
            crate::conversation::VolumeOwner::Project(owner) => {
                part("project");
                part(owner);
            }
            crate::conversation::VolumeOwner::Session(owner) => {
                part("session");
                part(owner);
            }
            crate::conversation::VolumeOwner::Agent(owner) => {
                part("agent");
                part(&owner.to_string());
            }
        }
        let digest = hash.finalize();
        let mut id = [0_u8; 16];
        for (destination, source) in id.iter_mut().zip(digest.as_bytes()) {
            *destination = *source;
        }
        Self {
            id: EntryId::from_bytes(id),
            role,
            name,
            kind: WorkspaceKind::Volume,
            source: WorkspaceSource::Volume(volume),
            root: String::new(),
            mode: if matches!(role, WorkspaceRole::Output | WorkspaceRole::Tree) {
                WorkspaceMode::ReadOnly
            } else {
                WorkspaceMode::ReadWrite
            },
            inherit: match role {
                WorkspaceRole::User => WorkspaceInherit::Fork,
                WorkspaceRole::Shared => WorkspaceInherit::Share,
                _ => WorkspaceInherit::Own,
            },
            filter: WorkspaceFilter::None,
        }
    }

    /// The volume source, if this is a volume binding.
    pub fn as_volume(&self) -> Option<&VolumeRef> {
        match &self.source {
            WorkspaceSource::Volume(volume) => Some(volume),
            WorkspaceSource::Subtree(_) => None,
        }
    }

    /// Require a volume for a consumer that does not serve subtree views.
    pub fn volume(&self) -> crate::Result<&VolumeRef> {
        self.as_volume()
            .ok_or_else(|| crate::Error::Invalid("workspace is a subtree view".into()))
    }

    /// Whole-volume adapters must not discard a view's access restrictions.
    /// Scoped adapters retain the complete entry instead.
    pub(crate) fn whole_volume(&self) -> crate::Result<&VolumeRef> {
        if !self.root.is_empty() || self.mode != WorkspaceMode::ReadWrite {
            return Err(crate::Error::FilesystemAccessConfiguration(
                "restricted workspace requires a scoped adapter",
            ));
        }
        self.volume()
    }

    /// Check the complete binding without reading its source.
    pub fn check(&self) -> Result<(), Error> {
        super::word::check_name(&self.name)?;
        if !self.root.is_empty() {
            crate::conversation::validate_content_path(&self.root, &mut None)
                .map_err(|_| Error::Record("invalid workspace root"))?;
        }
        let valid_role = match self.role {
            WorkspaceRole::User => true,
            WorkspaceRole::Home
            | WorkspaceRole::Private
            | WorkspaceRole::Internal
            | WorkspaceRole::Jobs => {
                self.mode == WorkspaceMode::ReadWrite && self.inherit == WorkspaceInherit::Own
            }
            WorkspaceRole::Output | WorkspaceRole::Tree => {
                self.mode == WorkspaceMode::ReadOnly && self.inherit == WorkspaceInherit::Own
            }
            WorkspaceRole::Shared => {
                self.mode == WorkspaceMode::ReadWrite && self.inherit == WorkspaceInherit::Share
            }
        };
        if !valid_role || (self.role != WorkspaceRole::User && self.filter != WorkspaceFilter::None)
        {
            return Err(Error::Record("workspace role and policy disagree"));
        }
        match (&self.source, self.kind) {
            (WorkspaceSource::Volume(_), WorkspaceKind::Volume)
                if self.role != WorkspaceRole::Tree =>
            {
                Ok(())
            }
            (WorkspaceSource::Subtree(_), WorkspaceKind::Tree)
                if self.role == WorkspaceRole::Tree
                    && self.mode == WorkspaceMode::ReadOnly
                    && self.inherit == WorkspaceInherit::Own =>
            {
                Ok(())
            }
            _ => Err(Error::Record("workspace kind, source or policy disagree")),
        }
    }
}

/// One delta, addressed by the entry's stable id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceOp {
    /// Append a new binding, preserving the primary and existing order.
    Add,
    /// Remove exactly this binding.
    Remove,
    /// Rename the binding identified by the entry's id.
    Rename,
    /// Replace a source or root in place, preserving table order.
    Reroot,
    /// Replace a tree binding after a new approval. Its scope is immutable for
    /// the lifetime of its id; the replacement receives a distinct id.
    Rebind {
        /// Exact tree slot being replaced.
        previous: EntryId,
    },
}

/// A version-one inbox delta; immutable history remains the table's authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceChanged {
    /// Record format, exactly one.
    pub version: u32,
    /// Change to apply.
    pub op: WorkspaceOp,
    /// Complete resulting entry (or the exact removed entry).
    pub entry: WorkspaceEntry,
}

impl WorkspaceChanged {
    /// Check the record before applying it or admitting persisted bytes.
    pub fn check(&self) -> Result<(), Error> {
        if self.version != 1 {
            return Err(Error::Record("unsupported workspace change version"));
        }
        self.entry.check()
    }
}

/// Ordered version-one table. Actor checkpoints retain its immutable snapshot
/// identity; source chunks are durable before that checkpoint publishes.
/// The first user entry is primary, falling back to home when there is none.
/// There is no configured workspace-count limit.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "TableWire")]
pub struct WorkspaceTable(Vec<WorkspaceEntry>);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableWire {
    version: u32,
    entries: Vec<WorkspaceEntry>,
}

impl TryFrom<TableWire> for WorkspaceTable {
    type Error = Error;

    fn try_from(wire: TableWire) -> Result<Self, Error> {
        if wire.version != 1 {
            return Err(Error::Record("unsupported workspace table version"));
        }
        Self::new(wire.entries)
    }
}

impl Serialize for WorkspaceTable {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct as _;
        let mut wire = serializer.serialize_struct("WorkspaceTable", 2)?;
        wire.serialize_field("version", &1_u32)?;
        wire.serialize_field("entries", &self.0)?;
        wire.end()
    }
}

impl WorkspaceTable {
    /// The single fold over the actor's admitted table view. Restoration loads
    /// its committed snapshot and never reads inbox history.
    pub(crate) fn apply_entries(
        entries: &mut Vec<WorkspaceEntry>,
        change: &WorkspaceChanged,
    ) -> Result<(), Error> {
        let mut table = Self(std::mem::take(entries));
        let result = table.apply(change);
        *entries = table.0;
        result
    }
    /// Admit a complete table once, without consulting history or peer agents.
    pub fn new(entries: Vec<WorkspaceEntry>) -> Result<Self, Error> {
        check_entries(entries.iter())?;
        Ok(Self(entries))
    }

    /// Every binding in primary-first order.
    pub fn entries(&self) -> &[WorkspaceEntry] {
        &self.0
    }

    /// Default cwd belongs to the first user entry, independent of role order.
    pub fn primary(&self) -> Option<&WorkspaceEntry> {
        Self::primary_in(&self.0)
    }

    /// Select the primary of an already admitted table without cloning entries.
    pub fn primary_in(entries: &[WorkspaceEntry]) -> Option<&WorkspaceEntry> {
        entries
            .iter()
            .find(|entry| entry.role == WorkspaceRole::User)
            .or_else(|| {
                entries
                    .iter()
                    .find(|entry| entry.role == WorkspaceRole::Home)
            })
    }

    /// Fold one admitted delta. Failure leaves the entire table unchanged.
    pub fn apply(&mut self, change: &WorkspaceChanged) -> Result<(), Error> {
        change.check()?;
        let previous = match change.op {
            WorkspaceOp::Rebind { previous } => previous,
            _ => change.entry.id,
        };
        let position = self.0.iter().position(|entry| entry.id == previous);
        if change.op == WorkspaceOp::Add {
            if position.is_some() {
                return Err(Error::Record("workspace id already bound"));
            }
        } else if position.is_none() {
            return Err(Error::Record("workspace id is not bound"));
        }
        if position
            .and_then(|index| self.0.get(index))
            .is_some_and(|entry| {
                entry.role != WorkspaceRole::User
                    && !matches!(change.op, WorkspaceOp::Rebind { .. })
            })
        {
            return Err(Error::Record("harness role entries cannot be changed"));
        }
        if self.0.iter().enumerate().any(|(index, entry)| {
            Some(index) != position
                && (entry.id == change.entry.id
                    || entry.name == change.entry.name
                    || (entry.role != WorkspaceRole::User && entry.role == change.entry.role))
        }) {
            return Err(Error::Record(
                "workspace name or harness role already bound",
            ));
        }
        match (position, &change.op) {
            (None, WorkspaceOp::Add) => self.0.push(change.entry.clone()),
            (Some(index), WorkspaceOp::Remove) => {
                if self.0.get(index) != Some(&change.entry) {
                    return Err(Error::Record("removed workspace changed"));
                }
                self.0.remove(index);
            }
            (Some(index), WorkspaceOp::Rename) => {
                let slot = self
                    .0
                    .get_mut(index)
                    .ok_or(Error::Record("workspace position vanished"))?;
                let mut renamed = slot.clone();
                renamed.name.clone_from(&change.entry.name);
                if renamed != change.entry {
                    return Err(Error::Record("rename changes more than a name"));
                }
                *slot = renamed;
            }
            (Some(index), WorkspaceOp::Reroot) => {
                let before = self
                    .0
                    .get_mut(index)
                    .ok_or(Error::Record("workspace position vanished"))?;
                if before.name != change.entry.name
                    || before.mode != change.entry.mode
                    || before.kind != change.entry.kind
                    || before.inherit != change.entry.inherit
                    || before.role != change.entry.role
                    || before.filter != change.entry.filter
                {
                    return Err(Error::Record("reroot changes binding policy"));
                }
                *before = change.entry.clone();
            }
            (Some(index), WorkspaceOp::Rebind { previous }) => {
                let slot = self
                    .0
                    .get_mut(index)
                    .ok_or(Error::Record("workspace position vanished"))?;
                if slot.role != WorkspaceRole::Tree
                    || change.entry.role != WorkspaceRole::Tree
                    || change.entry.id == *previous
                    || slot.name != change.entry.name
                {
                    return Err(Error::Record("rebind requires a new tree identity"));
                }
                *slot = change.entry.clone();
            }
            _ => return Err(Error::Record("invalid workspace operation")),
        }
        Ok(())
    }
}

pub(super) fn check_entries<'a>(
    entries: impl Iterator<Item = &'a WorkspaceEntry>,
) -> Result<(), Error> {
    let mut names = std::collections::BTreeSet::new();
    let mut ids = std::collections::BTreeSet::new();
    let mut roles = std::collections::BTreeSet::new();
    for entry in entries {
        entry.check()?;
        if !names.insert(&entry.name)
            || !ids.insert(entry.id)
            || (entry.role != WorkspaceRole::User && !roles.insert(entry.role))
        {
            return Err(Error::Record("workspace name, id or harness role repeats"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
