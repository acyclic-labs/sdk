//! Shared policy and capability decisions for filesystem views.
//!
//! Paths are canonical, volume-relative UTF-8. Globs support `*` and `?` within
//! a component and `**` as a complete component (zero or more components).
//! A compiled view pins all inputs except action and path. Rebuild it before a
//! new open after a policy change, generation change, or credential expiry.

mod capability;
mod compilation;
mod decision;
mod glob;
mod mount;
mod names;
mod trie;

pub use capability::{
    Capability, DIRECTORY_READ_PREFIX, FILE_READ_PREFIX, Grant, SCOPE_PREFIX, VolumeOperation,
};
pub use decision::{Decision, Denial, decide};
pub use mount::MountMode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub use trie::{PathTrie, PreparedPath};

/// Current policy encoding, stored in volume metadata rather than user files.
pub const POLICY_SCHEMA_VERSION: u32 = 1;

/// Independently grantable filesystem operations.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Action {
    /// Names and metadata, without file contents.
    List,
    /// File contents at the selected generation.
    Read,
    /// New entries, without replacing existing entries.
    Create,
    /// Replace or modify existing bytes.
    Write,
    /// Add bytes at the end of an existing file.
    Append,
    /// Remove an entry.
    Delete,
    /// Move an entry; both source and destination must be checked.
    Rename,
    /// Fork a view.
    Fork,
    /// Apply another view's changes to this view.
    MergeInto,
    /// Inspect historical generations, in addition to read/list permission.
    ReadHistory,
    /// Acquire an exclusive file lock.
    Lock,
    /// Change policy or administer the volume.
    Admin,
}

impl Action {
    /// Every action, in stable discriminant order.
    pub const ALL: [Self; 12] = [
        Self::List,
        Self::Read,
        Self::Create,
        Self::Write,
        Self::Append,
        Self::Delete,
        Self::Rename,
        Self::Fork,
        Self::MergeInto,
        Self::ReadHistory,
        Self::Lock,
        Self::Admin,
    ];
}

/// Kind of authenticated identity. Anonymous identities carry no trusted claims.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// A person.
    User,
    /// An agent execution.
    Agent,
    /// A service account.
    Service,
    /// A group identity.
    Group,
    /// An unauthenticated caller.
    Anonymous,
}

/// Host-authenticated identity and claims; never populated from request claims.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    /// Identity namespace.
    pub kind: PrincipalKind,
    /// Stable identity within the namespace.
    pub id: String,
    /// Task ancestry, root first, ending with this task.
    pub lineage: Vec<String>,
    /// Authenticated group memberships.
    pub groups: BTreeSet<String>,
    /// Authenticated roles.
    pub roles: BTreeSet<String>,
    /// Authenticated tags.
    pub tags: BTreeSet<String>,
    /// Additional authenticated attributes.
    pub attributes: BTreeMap<String, String>,
}

/// Relation of the caller's task to the resource's task.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    /// The same task.
    SelfTask,
    /// The caller is the immediate parent.
    Parent,
    /// The caller is an immediate child.
    Child,
    /// Distinct tasks with the same immediate parent.
    Sibling,
    /// The caller is a strict ancestor.
    Ancestor,
    /// The caller is a strict descendant.
    Descendant,
    /// Tasks share a root, including the same task.
    SameTaskTree,
}

impl Relation {
    pub(super) fn matches(self, caller: &[String], resource: &[String]) -> bool {
        if caller.is_empty() || resource.is_empty() {
            return false;
        }
        match self {
            Self::SelfTask => caller == resource,
            Self::Parent => resource.len() == caller.len() + 1 && resource.starts_with(caller),
            Self::Child => caller.len() == resource.len() + 1 && caller.starts_with(resource),
            Self::Sibling => {
                caller.len() > 1
                    && caller.len() == resource.len()
                    && caller != resource
                    && caller.split_last().map(|(_, p)| p) == resource.split_last().map(|(_, p)| p)
            }
            Self::Ancestor => caller.len() < resource.len() && resource.starts_with(caller),
            Self::Descendant => caller.len() > resource.len() && caller.starts_with(resource),
            Self::SameTaskTree => caller.first() == resource.first(),
        }
    }
}

/// Inclusive generation interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationRange {
    /// First visible generation.
    pub first: u64,
    /// Last visible generation.
    pub last: u64,
}

impl GenerationRange {
    pub(super) const fn contains(self, value: u64) -> bool {
        self.first <= value && value <= self.last
    }
}

/// Selection of head or historical generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Generations {
    /// Only the current head.
    Head,
    /// A pinned inclusive interval.
    Range(GenerationRange),
    /// All generations including head.
    All,
}

/// Resource selector: task tree, volume, generation, then path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePattern {
    /// Task root, or any lineage when absent.
    pub lineage_root: Option<String>,
    /// Exact volume identity, or any volume when absent.
    pub volume: Option<String>,
    /// Permitted generations.
    pub generations: Generations,
    /// Canonical volume-relative glob; `**` includes the volume root.
    pub path: String,
}

/// Borrowed concrete resource, with the authoritative current head.
#[derive(Clone, Copy, Debug)]
pub struct Resource<'a> {
    /// Authoritative volume name semantics, never supplied by the caller.
    pub config: crate::VolumeConfig,
    /// Root-first resource task ancestry.
    pub lineage: &'a [String],
    /// Volume identity.
    pub volume: &'a str,
    /// Requested generation.
    pub generation: u64,
    /// Current head generation.
    pub head: u64,
    /// Lossless Unicode projection of the profile's native path, empty for root.
    /// Other name encodings and non-Unicode native names are unsupported.
    pub path: &'a str,
}

impl ResourcePattern {
    pub(super) fn validate(&self) -> Result<(), InvalidPolicy> {
        if let Generations::Range(range) = self.generations
            && range.first > range.last
        {
            return Err(InvalidPolicy("generation range is reversed"));
        }
        glob::validate(&self.path)
    }

    pub(super) fn matches_view(&self, resource: Resource<'_>) -> bool {
        self.lineage_root
            .as_ref()
            .is_none_or(|root| resource.lineage.first() == Some(root))
            && self
                .volume
                .as_deref()
                .is_none_or(|volume| volume == resource.volume)
            && match self.generations {
                Generations::Head => resource.generation == resource.head,
                Generations::Range(range) => range.contains(resource.generation),
                Generations::All => true,
            }
    }
}

/// Conjunctive principal selector; absent fields match any principal.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalPattern {
    /// Required identity namespace.
    pub kind: Option<PrincipalKind>,
    /// Required identity.
    pub id: Option<String>,
    /// Required group membership.
    pub group: Option<String>,
    /// Required attribute values.
    pub attributes: BTreeMap<String, String>,
}

/// Conjunctive conditions. Express alternatives with separate allow rules.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conditions {
    /// Exclusive Unix expiry in seconds.
    pub expires_at: Option<u64>,
    /// Additional inclusive generation interval.
    pub generations: Option<GenerationRange>,
    /// Caller relation to the resource task.
    pub relation: Option<Relation>,
    /// Required role.
    pub role: Option<String>,
    /// Required tag.
    pub tag: Option<String>,
    /// Trusted distinct approvals for this exact merge; only valid on merge-into rules.
    pub approvals: Option<u32>,
}

/// Trusted dynamic decision inputs, bound to the exact operation by the host.
#[derive(Clone, Copy, Debug)]
pub struct Context {
    /// Current Unix time in seconds.
    pub now: u64,
    /// Count of distinct verified approvals for this merge and target generation.
    pub approvals: u32,
}

/// Rule effect. Any matching denial overrides every matching allowance.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// Grant the selected actions.
    Allow,
    /// Forbid the selected actions.
    Deny,
}

/// One attributable policy rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// Stable id, unique across all layers in this policy.
    pub id: String,
    /// Allow or deny.
    pub effect: Effect,
    /// Caller selector.
    pub principal: PrincipalPattern,
    /// Independently granted actions.
    pub actions: BTreeSet<Action>,
    /// Resource selector.
    pub resource: ResourcePattern,
    /// Additional predicates.
    pub conditions: Conditions,
}

/// Versioned volume metadata. Each layer must allow; denials in any layer win.
/// Fork tightening adds a layer, preserving inherited layers byte for byte.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Encoding version; unknown versions fail closed.
    pub schema_version: u32,
    /// Monotonic volume policy revision.
    pub version: u64,
    /// Intersected rule layers; an empty policy denies everything.
    pub layers: Vec<Vec<Rule>>,
}

/// Invalid policy or pattern. Invalid data never grants access.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid filesystem access policy: {0}")]
pub struct InvalidPolicy(pub &'static str);

impl Policy {
    /// Validate once at admission or compilation, including every glob.
    pub fn validate(&self) -> Result<(), InvalidPolicy> {
        if self.schema_version != POLICY_SCHEMA_VERSION {
            return Err(InvalidPolicy("unsupported schema version"));
        }
        let mut ids = BTreeSet::new();
        for rule in self.layers.iter().flatten() {
            if rule.id.is_empty() || !ids.insert(&rule.id) {
                return Err(InvalidPolicy("rule ids must be nonempty and unique"));
            }
            if rule.conditions.approvals.is_some()
                && rule.actions != BTreeSet::from([Action::MergeInto])
            {
                return Err(InvalidPolicy("approval conditions require only merge-into"));
            }
            if rule
                .conditions
                .generations
                .is_some_and(|range| range.first > range.last)
            {
                return Err(InvalidPolicy("generation condition is reversed"));
            }
            rule.resource.validate()?;
        }
        Ok(())
    }
}
