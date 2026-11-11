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
    /// Harness-written outputs, read-only to the model.
    Output,
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
    /// Check the complete binding without reading its source.
    pub fn check(&self) -> Result<(), Error> {
        super::targets::check_name(&self.name)?;
        if !self.root.is_empty() {
            crate::conversation::validate_content_path(&self.root, &mut None)
                .map_err(|_| Error::Record("invalid workspace root"))?;
        }
        let valid_role = match self.role {
            WorkspaceRole::User => true,
            WorkspaceRole::Home | WorkspaceRole::Private => {
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// Folded version-one table, retained directly by an actor checkpoint in O(W).
/// The first user entry is primary. There is no configured workspace-count limit.
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
    /// Admit a complete table once, without consulting history or peer agents.
    pub fn new(entries: Vec<WorkspaceEntry>) -> Result<Self, Error> {
        let mut names = std::collections::BTreeSet::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut roles = std::collections::BTreeSet::new();
        for entry in &entries {
            entry.check()?;
            if !names.insert(&entry.name)
                || !ids.insert(entry.id)
                || (entry.role != WorkspaceRole::User && !roles.insert(entry.role))
            {
                return Err(Error::Record("workspace name, id or harness role repeats"));
            }
        }
        Ok(Self(entries))
    }

    /// Every binding in primary-first order.
    pub fn entries(&self) -> &[WorkspaceEntry] {
        &self.0
    }

    /// Default cwd belongs to the first user entry, independent of role order.
    pub fn primary(&self) -> Option<&WorkspaceEntry> {
        self.0
            .iter()
            .find(|entry| entry.role == WorkspaceRole::User)
    }

    /// Fold one admitted delta. Failure leaves the entire table unchanged.
    pub fn apply(&mut self, change: &WorkspaceChanged) -> Result<(), Error> {
        change.check()?;
        let position = self.0.iter().position(|entry| entry.id == change.entry.id);
        if change.op == WorkspaceOp::Add {
            if position.is_some() {
                return Err(Error::Record("workspace id already bound"));
            }
        } else if position.is_none() {
            return Err(Error::Record("workspace id is not bound"));
        }
        if position
            .and_then(|index| self.0.get(index))
            .is_some_and(|entry| entry.role != WorkspaceRole::User)
        {
            return Err(Error::Record("harness role entries cannot be changed"));
        }
        if self.0.iter().enumerate().any(|(index, entry)| {
            Some(index) != position
                && (entry.name == change.entry.name
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
            _ => return Err(Error::Record("invalid workspace operation")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
