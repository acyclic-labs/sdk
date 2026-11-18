//! Durable, agent-neutral composition of recursively forked workspaces.
//!
//! A context is the smallest unit adapters need to identify: a set of roots,
//! their exact SDK workspaces, and one direct parent.  Host session and agent
//! identifiers intentionally stay outside this module.  Context records are
//! small control-plane facts; filesystem contents remain immutable SDK
//! generations and are never enumerated here.

use crate::WorkspaceId;
use crate::record_store::stored_revision;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;
use uuid::Uuid;

const WORKSPACE_CONTEXT_VERSION: u32 = 1;

uuid_identity!(
    WorkspaceContextId,
    "Stable opaque identity of one multi-root workspace context."
);
uuid_identity!(
    WorkspaceRootId,
    "Stable identity of one physical root across recursively forked contexts."
);

/// Lifecycle state retained independently from whether a native mount is loaded.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceContextState {
    /// The context accepts operations and may be mounted.
    Active,
    /// The context is retained for inspection or later resume.
    Frozen,
    /// The context and its descendants are no longer publishable.
    Discarded,
}

/// One root binding inside a context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceContextRoot {
    /// Stable identity shared by this root throughout the context tree.
    pub root_id: WorkspaceRootId,
    /// Canonical physical root visible to the root context.
    pub source_path: PathBuf,
    /// Actual SDK workspace holding this context's root generation.
    pub workspace_id: WorkspaceId,
    /// Canonical name needed to reopen the SDK workspace after restart.
    pub workspace_name: String,
    /// Exact direct-parent workspace, absent only in a root context.
    pub parent_workspace_id: Option<WorkspaceId>,
    /// Native mount destination when loaded. This is routing state, not authority.
    pub mount_path: Option<PathBuf>,
}

/// Durable multi-root workspace context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceContext {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Stable context identity.
    pub context_id: WorkspaceContextId,
    /// Direct parent allowed to receive publication.
    pub parent_context_id: Option<WorkspaceContextId>,
    /// Independently forked roots keyed by stable physical-root identity.
    pub roots: BTreeMap<WorkspaceRootId, WorkspaceContextRoot>,
    /// Durable logical lifecycle.
    pub state: WorkspaceContextState,
}

/// Whether cwd routing selected a managed mount or the physical root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceRouteKind {
    /// A child mount encloses the requested path.
    Mount,
    /// A registered physical root encloses the requested path.
    Source,
}

/// Deterministic result of resolving a path inside one context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceRoute {
    /// Context in which routing was requested.
    pub context_id: WorkspaceContextId,
    /// Selected root.
    pub root_id: WorkspaceRootId,
    /// Selected route class.
    pub kind: WorkspaceRouteKind,
    /// Canonical binding prefix.
    pub prefix: PathBuf,
    /// Path relative to the selected binding prefix.
    pub relative_path: PathBuf,
}

/// Durable optimistic-concurrency adapter for workspace contexts.
pub trait WorkspaceContextStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one context, if registered.
    fn load(
        &self,
        context_id: WorkspaceContextId,
    ) -> impl Future<Output = Result<Option<WorkspaceContext>, Self::Error>> + Send;

    /// Lists all durable contexts. Records are small and this is used only for
    /// routing, recursive discard, and recovery—not filesystem indexing.
    fn list(&self) -> impl Future<Output = Result<Vec<WorkspaceContext>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a context.
    fn compare_and_swap(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: WorkspaceContext,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Atomically verifies every listed revision and replaces the supplied
    /// records. Expected revision zero means that context must be absent.
    fn compare_and_swap_many(
        &self,
        expected_revisions: BTreeMap<WorkspaceContextId, u64>,
        replacements: Vec<WorkspaceContext>,
        require_exact_set: bool,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Atomically validates and tombstones one direct-child subtree.
    fn discard_subtree(
        &self,
        parent: WorkspaceContextId,
        child: WorkspaceContextId,
        maximum: u32,
    ) -> impl Future<Output = Result<WorkspaceContextDiscardOutcome, Self::Error>> + Send;
}

/// Atomic durable result of a bounded subtree discard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkspaceContextDiscardOutcome {
    /// The complete subtree was durably tombstoned.
    Discarded(Vec<WorkspaceContextId>),
    /// The child does not name a compatible durable record.
    IncompatibleState,
    /// The supplied parent is not the exact direct parent.
    UnauthorizedParent,
    /// The subtree root was already discarded.
    AlreadyDiscarded,
    /// Traversal exceeded the caller's explicit bound and changed nothing.
    TraversalLimit,
}

/// Core workspace-context registry over one durable adapter.
///
/// Each mutation attempts one conditional publication. A concurrent change
/// returns [`WorkspaceContextError::Conflict`] without repeating the operation.
pub struct WorkspaceContextRegistry<S> {
    store: S,
}

impl<S> WorkspaceContextRegistry<S> {
    /// Creates a registry over a durable state adapter.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Borrows the underlying adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

/// Workspace-context registration, lineage, and routing failure.
#[derive(Debug, Error)]
pub enum WorkspaceContextError<E: std::error::Error + 'static> {
    /// The durable adapter failed.
    #[error("workspace-context store failed: {0}")]
    Store(E),
    /// Persisted state uses an unsupported version or mismatched identity.
    #[error("workspace-context state is incompatible")]
    IncompatibleState,
    /// A context already exists with different immutable lineage.
    #[error("workspace context conflicts with its durable registration")]
    ConflictingRegistration,
    /// A checked revision or context set changed before publication.
    #[error("workspace context changed before publication")]
    Conflict,
    /// Root bindings are malformed or do not match the direct parent.
    #[error("workspace context roots do not match their direct parent")]
    InvalidRoots,
    /// A host path is not absolute and lexically normalized.
    #[error("workspace context path is not canonical")]
    NonCanonicalPath,
    /// Only the direct parent may publish or discard this context.
    #[error("only the direct parent may perform this workspace-context operation")]
    UnauthorizedParent,
    /// Discard traversal encountered a cycle or exceeded its safe bound.
    #[error("workspace-context traversal exceeded its safe bound")]
    TraversalLimit,
    /// The context cannot be mutated in its current lifecycle state.
    #[error("workspace context is discarded")]
    Discarded,
    /// A root cannot be removed while a descendant still depends on it.
    #[error("workspace root is still bound by a live descendant")]
    RootInUse,
}

impl<S: WorkspaceContextStore> WorkspaceContextRegistry<S> {
    /// Registers a physical root context without reading directory contents.
    pub async fn register_root(
        &self,
        context_id: WorkspaceContextId,
        roots: impl IntoIterator<Item = WorkspaceContextRoot>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        self.register(context_id, None, roots).await
    }

    /// Registers a recursively forked context after the SDK workspaces have
    /// been forked. Every supplied root must point at the matching workspace in
    /// the exact direct parent.
    pub async fn register_child(
        &self,
        context_id: WorkspaceContextId,
        parent_context_id: WorkspaceContextId,
        roots: impl IntoIterator<Item = WorkspaceContextRoot>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        self.register(context_id, Some(parent_context_id), roots)
            .await
    }

    /// Adds one lazily attached root to an existing context without reading
    /// directory contents.
    ///
    /// Root contexts may attach a new physical root directly. Descendants may
    /// attach only a root already present in their exact direct parent, and
    /// the child binding must name that parent's workspace. Repeating the
    /// exact attachment is idempotent.
    pub async fn adopt_root(
        &self,
        context_id: WorkspaceContextId,
        root: WorkspaceContextRoot,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        collect_roots([root.clone()])?;
        let mut context = self.resolve(context_id).await?;
        if context.state == WorkspaceContextState::Discarded {
            return Err(WorkspaceContextError::Discarded);
        }
        if let Some(existing) = context.roots.get(&root.root_id) {
            return if existing == &root {
                Ok(context)
            } else {
                Err(WorkspaceContextError::ConflictingRegistration)
            };
        }
        if context
            .roots
            .values()
            .any(|existing| existing.source_path == root.source_path)
        {
            return Err(WorkspaceContextError::ConflictingRegistration);
        }

        let mut expected = BTreeMap::from([(context_id, context.revision)]);
        match context.parent_context_id {
            None if root.parent_workspace_id.is_some() => {
                return Err(WorkspaceContextError::InvalidRoots);
            }
            None => {}
            Some(parent_context_id) => {
                let parent = self.resolve(parent_context_id).await?;
                let valid_parent = parent.state != WorkspaceContextState::Discarded
                    && parent.roots.get(&root.root_id).is_some_and(|parent_root| {
                        parent_root.source_path == root.source_path
                            && root.parent_workspace_id == Some(parent_root.workspace_id)
                    });
                if !valid_parent {
                    return Err(WorkspaceContextError::InvalidRoots);
                }
                expected.insert(parent_context_id, parent.revision);
            }
        }

        context.roots.insert(root.root_id, root);
        context.revision = context
            .revision
            .checked_add(1)
            .ok_or(WorkspaceContextError::IncompatibleState)?;
        if self
            .store
            .compare_and_swap_many(expected, vec![context.clone()], false)
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            return Ok(context);
        }
        Err(WorkspaceContextError::Conflict)
    }

    /// Removes one root binding only when no live descendant depends on it.
    ///
    /// The caller must separately settle or explicitly discard unpublished
    /// filesystem changes before invoking this topology operation. Verifying
    /// the complete context set makes concurrent descendant creation fail the
    /// CAS rather than leaving a child bound to a missing parent root.
    /// Every record in that snapshot must satisfy the stored-record contract;
    /// incompatible snapshots fail before lifecycle or root-removal checks.
    pub async fn remove_root(
        &self,
        context_id: WorkspaceContextId,
        root_id: WorkspaceRootId,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        let contexts = self
            .store
            .list()
            .await
            .map_err(WorkspaceContextError::Store)?;
        let mut by_id = BTreeMap::new();
        for context in contexts {
            validate_record(&context, context.context_id)?;
            if by_id.insert(context.context_id, context).is_some() {
                return Err(WorkspaceContextError::IncompatibleState);
            }
        }
        let mut context = by_id
            .get(&context_id)
            .cloned()
            .ok_or(WorkspaceContextError::IncompatibleState)?;
        if context.state == WorkspaceContextState::Discarded {
            return Err(WorkspaceContextError::Discarded);
        }
        if !context.roots.contains_key(&root_id) || context.roots.len() == 1 {
            return Err(WorkspaceContextError::InvalidRoots);
        }
        // Ancestors already walked to a root without meeting `context_id`
        // end later walks, keeping deep fork chains linear.
        let mut cleared = BTreeSet::new();
        for candidate in by_id.values() {
            if candidate.context_id == context_id
                || candidate.state == WorkspaceContextState::Discarded
                || !candidate.roots.contains_key(&root_id)
            {
                continue;
            }
            let mut parent = candidate.parent_context_id;
            let mut visited = BTreeSet::new();
            while let Some(id) = parent {
                if id == context_id {
                    return Err(WorkspaceContextError::RootInUse);
                }
                if cleared.contains(&id) {
                    break;
                }
                if !visited.insert(id) {
                    return Err(WorkspaceContextError::IncompatibleState);
                }
                parent = by_id
                    .get(&id)
                    .ok_or(WorkspaceContextError::IncompatibleState)?
                    .parent_context_id;
            }
            cleared.append(&mut visited);
        }
        context.roots.remove(&root_id);
        context.revision = context
            .revision
            .checked_add(1)
            .ok_or(WorkspaceContextError::IncompatibleState)?;
        let expected = by_id
            .iter()
            .map(|(id, record)| (*id, record.revision))
            .collect();
        if self
            .store
            .compare_and_swap_many(expected, vec![context.clone()], true)
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            return Ok(context);
        }
        Err(WorkspaceContextError::Conflict)
    }

    /// Resolves one durable context.
    pub async fn resolve(
        &self,
        context_id: WorkspaceContextId,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        let context = self
            .store
            .load(context_id)
            .await
            .map_err(WorkspaceContextError::Store)?
            .ok_or(WorkspaceContextError::IncompatibleState)?;
        validate_record(&context, context_id)?;
        Ok(context)
    }

    /// Verifies that `parent` is the child's exact publication target.
    pub async fn authorize_parent(
        &self,
        child: WorkspaceContextId,
        parent: WorkspaceContextId,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        let context = self.resolve(child).await?;
        if context.state == WorkspaceContextState::Discarded {
            return Err(WorkspaceContextError::Discarded);
        }
        if context.parent_context_id != Some(parent) {
            return Err(WorkspaceContextError::UnauthorizedParent);
        }
        Ok(context)
    }

    /// Changes only mount-routing state. Mount lifecycle remains an injected
    /// product capability and never changes filesystem semantics.
    pub async fn set_mount(
        &self,
        context_id: WorkspaceContextId,
        root_id: WorkspaceRootId,
        mount_path: Option<PathBuf>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        if mount_path
            .as_deref()
            .is_some_and(|path| !is_canonical(path))
        {
            return Err(WorkspaceContextError::NonCanonicalPath);
        }
        self.update(context_id, |context| {
            if context.state == WorkspaceContextState::Discarded {
                return Err(WorkspaceContextError::Discarded);
            }
            let root = context
                .roots
                .get_mut(&root_id)
                .ok_or(WorkspaceContextError::InvalidRoots)?;
            root.mount_path = mount_path;
            Ok(())
        })
        .await
    }

    /// Advances one root binding after a compatibility branch switch.
    ///
    /// This changes only the workspace selected by the context. The stable
    /// root identity, source path, and mount route are preserved.
    pub async fn set_workspace(
        &self,
        context_id: WorkspaceContextId,
        root_id: WorkspaceRootId,
        workspace_id: WorkspaceId,
        workspace_name: String,
        parent_workspace_id: Option<WorkspaceId>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        if workspace_name.is_empty() {
            return Err(WorkspaceContextError::InvalidRoots);
        }
        let mut context = self.resolve(context_id).await?;
        if context.state == WorkspaceContextState::Discarded {
            return Err(WorkspaceContextError::Discarded);
        }
        let mut expected = BTreeMap::from([(context_id, context.revision)]);
        match context.parent_context_id {
            None if parent_workspace_id.is_some() => {
                return Err(WorkspaceContextError::InvalidRoots);
            }
            None => {}
            Some(parent_context_id) => {
                let parent = self.resolve(parent_context_id).await?;
                if parent.state == WorkspaceContextState::Discarded
                    || parent
                        .roots
                        .get(&root_id)
                        .is_none_or(|root| Some(root.workspace_id) != parent_workspace_id)
                {
                    return Err(WorkspaceContextError::InvalidRoots);
                }
                expected.insert(parent_context_id, parent.revision);
            }
        }
        let root = context
            .roots
            .get_mut(&root_id)
            .ok_or(WorkspaceContextError::InvalidRoots)?;
        root.workspace_id = workspace_id;
        root.workspace_name = workspace_name;
        root.parent_workspace_id = parent_workspace_id;
        context.revision = context
            .revision
            .checked_add(1)
            .ok_or(WorkspaceContextError::IncompatibleState)?;
        if self
            .store
            .compare_and_swap_many(expected, vec![context.clone()], false)
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            return Ok(context);
        }
        Err(WorkspaceContextError::Conflict)
    }

    /// Freezes or resumes a retained context without changing its roots.
    pub async fn set_active(
        &self,
        context_id: WorkspaceContextId,
        active: bool,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        self.update(context_id, |context| {
            if context.state == WorkspaceContextState::Discarded {
                return Err(WorkspaceContextError::Discarded);
            }
            context.state = if active {
                WorkspaceContextState::Active
            } else {
                WorkspaceContextState::Frozen
            };
            Ok(())
        })
        .await
    }

    /// Recursively tombstones a child subtree. The caller must be the exact
    /// parent of the subtree root. Objects may be reclaimed later.
    pub async fn discard_subtree(
        &self,
        parent: WorkspaceContextId,
        child: WorkspaceContextId,
        maximum: u32,
    ) -> Result<Vec<WorkspaceContextId>, WorkspaceContextError<S::Error>> {
        match self
            .store
            .discard_subtree(parent, child, maximum)
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            WorkspaceContextDiscardOutcome::Discarded(contexts) => Ok(contexts),
            WorkspaceContextDiscardOutcome::IncompatibleState => {
                Err(WorkspaceContextError::IncompatibleState)
            }
            WorkspaceContextDiscardOutcome::UnauthorizedParent => {
                Err(WorkspaceContextError::UnauthorizedParent)
            }
            WorkspaceContextDiscardOutcome::AlreadyDiscarded => {
                Err(WorkspaceContextError::Discarded)
            }
            WorkspaceContextDiscardOutcome::TraversalLimit => {
                Err(WorkspaceContextError::TraversalLimit)
            }
        }
    }

    /// Routes a canonical cwd by managed mount first, then longest enclosing
    /// physical root. This operation never probes the host filesystem.
    pub async fn route(
        &self,
        context_id: WorkspaceContextId,
        path: &Path,
    ) -> Result<Option<WorkspaceRoute>, WorkspaceContextError<S::Error>> {
        if !is_canonical(path) {
            return Err(WorkspaceContextError::NonCanonicalPath);
        }
        let context = self.resolve(context_id).await?;
        if context.state == WorkspaceContextState::Discarded {
            return Err(WorkspaceContextError::Discarded);
        }
        let mounted = context
            .roots
            .values()
            .filter_map(|root| root.mount_path.as_ref().map(|prefix| (root, prefix)))
            .filter(|(_, prefix)| path.starts_with(prefix))
            .max_by_key(|(_, prefix)| prefix.components().count());
        let selected = mounted
            .map(|(root, prefix)| (root, prefix, WorkspaceRouteKind::Mount))
            .or_else(|| {
                context
                    .roots
                    .values()
                    .filter(|root| path.starts_with(&root.source_path))
                    .max_by_key(|root| root.source_path.components().count())
                    .map(|root| (root, &root.source_path, WorkspaceRouteKind::Source))
            });
        Ok(selected.map(|(root, prefix, kind)| WorkspaceRoute {
            context_id,
            root_id: root.root_id,
            kind,
            prefix: prefix.clone(),
            relative_path: path
                .strip_prefix(prefix)
                .unwrap_or_else(|_| Path::new(""))
                .to_path_buf(),
        }))
    }

    async fn register(
        &self,
        context_id: WorkspaceContextId,
        parent_context_id: Option<WorkspaceContextId>,
        roots: impl IntoIterator<Item = WorkspaceContextRoot>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        let roots = collect_roots(roots)?;
        if parent_context_id.is_none()
            && roots
                .values()
                .any(|root| root.parent_workspace_id.is_some())
        {
            return Err(WorkspaceContextError::InvalidRoots);
        }
        let mut expected = BTreeMap::from([(context_id, 0)]);
        if let Some(parent_id) = parent_context_id {
            let parent = self.resolve(parent_id).await?;
            if parent.state == WorkspaceContextState::Discarded
                || roots.len() != parent.roots.len()
                || roots.iter().any(|(root_id, root)| {
                    parent.roots.get(root_id).is_none_or(|parent_root| {
                        root.source_path != parent_root.source_path
                            || root.parent_workspace_id != Some(parent_root.workspace_id)
                    })
                })
            {
                return Err(WorkspaceContextError::InvalidRoots);
            }
            expected.insert(parent_id, parent.revision);
        }
        let record = WorkspaceContext {
            version: WORKSPACE_CONTEXT_VERSION,
            revision: 1,
            context_id,
            parent_context_id,
            roots,
            state: WorkspaceContextState::Active,
        };
        if self
            .store
            .compare_and_swap_many(expected, vec![record.clone()], false)
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            return Ok(record);
        }
        if let Some(existing) = self
            .store
            .load(context_id)
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            validate_record(&existing, context_id)?;
            return if same_registration(&existing, &record) {
                Ok(existing)
            } else {
                Err(WorkspaceContextError::ConflictingRegistration)
            };
        }
        Err(WorkspaceContextError::Conflict)
    }

    async fn update(
        &self,
        context_id: WorkspaceContextId,
        action: impl FnOnce(&mut WorkspaceContext) -> Result<(), WorkspaceContextError<S::Error>>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        let mut context = self.resolve(context_id).await?;
        let expected = context.revision;
        action(&mut context)?;
        context.revision = expected
            .checked_add(1)
            .ok_or(WorkspaceContextError::IncompatibleState)?;
        if self
            .store
            .compare_and_swap(context_id, expected, context.clone())
            .await
            .map_err(WorkspaceContextError::Store)?
        {
            return Ok(context);
        }
        Err(WorkspaceContextError::Conflict)
    }
}

fn collect_roots<E: std::error::Error + 'static>(
    roots: impl IntoIterator<Item = WorkspaceContextRoot>,
) -> Result<BTreeMap<WorkspaceRootId, WorkspaceContextRoot>, WorkspaceContextError<E>> {
    let mut collected = BTreeMap::new();
    for root in roots {
        if root.workspace_name.is_empty()
            || !is_canonical(&root.source_path)
            || root
                .mount_path
                .as_deref()
                .is_some_and(|path| !is_canonical(path))
            || collected.insert(root.root_id, root).is_some()
        {
            return Err(WorkspaceContextError::InvalidRoots);
        }
    }
    if collected.is_empty() {
        return Err(WorkspaceContextError::InvalidRoots);
    }
    Ok(collected)
}

fn validate_record<E: std::error::Error + 'static>(
    context: &WorkspaceContext,
    expected: WorkspaceContextId,
) -> Result<(), WorkspaceContextError<E>> {
    if !record_is_valid(context, expected) {
        return Err(WorkspaceContextError::IncompatibleState);
    }
    Ok(())
}

pub(crate) fn record_is_valid(context: &WorkspaceContext, expected: WorkspaceContextId) -> bool {
    !(context.version != WORKSPACE_CONTEXT_VERSION
        || context.context_id != expected
        || context.revision == 0
        || context.roots.is_empty()
        || context.roots.iter().any(|(root_id, root)| {
            root_id != &root.root_id
                || root.workspace_name.is_empty()
                || !is_canonical(&root.source_path)
                || root
                    .mount_path
                    .as_deref()
                    .is_some_and(|path| !is_canonical(path))
        }))
}

fn same_registration(left: &WorkspaceContext, right: &WorkspaceContext) -> bool {
    left.version == right.version
        && left.context_id == right.context_id
        && left.parent_context_id == right.parent_context_id
        && left.roots == right.roots
}

pub(crate) fn is_canonical(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

pub(crate) type WorkspaceContextChildren =
    BTreeMap<WorkspaceContextId, BTreeSet<WorkspaceContextId>>;

pub(crate) fn update_context_children(
    children: &mut WorkspaceContextChildren,
    before: Option<&WorkspaceContext>,
    after: Option<&WorkspaceContext>,
) {
    if let Some(before) = before
        && before.parent_context_id != after.and_then(|record| record.parent_context_id)
        && let Some(parent) = before.parent_context_id
        && let Some(siblings) = children.get_mut(&parent)
    {
        siblings.remove(&before.context_id);
        if siblings.is_empty() {
            children.remove(&parent);
        }
    }
    if let Some(after) = after
        && before.and_then(|record| record.parent_context_id) != after.parent_context_id
        && let Some(parent) = after.parent_context_id
    {
        children.entry(parent).or_default().insert(after.context_id);
    }
}

pub(crate) fn plan_context_subtree_discard(
    root: Option<&WorkspaceContext>,
    children: &WorkspaceContextChildren,
    parent: WorkspaceContextId,
    child: WorkspaceContextId,
    maximum: u32,
) -> Result<Vec<WorkspaceContextId>, WorkspaceContextDiscardOutcome> {
    if maximum == 0 {
        return Err(WorkspaceContextDiscardOutcome::TraversalLimit);
    }
    let Some(root) = root else {
        return Err(WorkspaceContextDiscardOutcome::IncompatibleState);
    };
    if root.state == WorkspaceContextState::Discarded {
        return Err(WorkspaceContextDiscardOutcome::AlreadyDiscarded);
    }
    if root.parent_context_id != Some(parent) {
        return Err(WorkspaceContextDiscardOutcome::UnauthorizedParent);
    }
    let maximum = maximum as usize;
    let mut pending = vec![child];
    let mut seen = BTreeSet::new();
    while let Some(context_id) = pending.pop() {
        if !seen.insert(context_id) || seen.len() > maximum {
            return Err(WorkspaceContextDiscardOutcome::TraversalLimit);
        }
        if let Some(descendants) = children.get(&context_id) {
            if seen.len() + pending.len() + descendants.len() > maximum {
                return Err(WorkspaceContextDiscardOutcome::TraversalLimit);
            }
            pending.extend(descendants.iter().copied());
        }
    }
    Ok(seen.into_iter().collect())
}

pub(crate) fn discard_context_subtree(
    records: &mut BTreeMap<WorkspaceContextId, WorkspaceContext>,
    children: &WorkspaceContextChildren,
    parent: WorkspaceContextId,
    child: WorkspaceContextId,
    maximum: u32,
) -> WorkspaceContextDiscardOutcome {
    let discarded =
        match plan_context_subtree_discard(records.get(&child), children, parent, child, maximum) {
            Ok(discarded) => discarded,
            Err(outcome) => return outcome,
        };
    let mut replacements = Vec::with_capacity(discarded.len());
    for context_id in &discarded {
        let Some(mut context) = records.get(context_id).cloned() else {
            return WorkspaceContextDiscardOutcome::IncompatibleState;
        };
        if !record_is_valid(&context, *context_id) {
            return WorkspaceContextDiscardOutcome::IncompatibleState;
        }
        let Some(revision) = context.revision.checked_add(1) else {
            return WorkspaceContextDiscardOutcome::IncompatibleState;
        };
        context.state = WorkspaceContextState::Discarded;
        for root in context.roots.values_mut() {
            root.mount_path = None;
        }
        context.revision = revision;
        replacements.push(context);
    }
    for replacement in replacements {
        records.insert(replacement.context_id, replacement);
    }
    WorkspaceContextDiscardOutcome::Discarded(discarded)
}

/// Process-local context adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryWorkspaceContextStore {
    state: Mutex<MemoryWorkspaceContextState>,
}

#[derive(Default)]
struct MemoryWorkspaceContextState {
    records: BTreeMap<WorkspaceContextId, WorkspaceContext>,
    children: WorkspaceContextChildren,
}

impl MemoryWorkspaceContextStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local context synchronization failure.
#[derive(Debug, Error)]
#[error("workspace-context memory store is unavailable")]
pub struct MemoryWorkspaceContextStoreError;

impl WorkspaceContextStore for MemoryWorkspaceContextStore {
    type Error = MemoryWorkspaceContextStoreError;

    async fn load(
        &self,
        context_id: WorkspaceContextId,
    ) -> Result<Option<WorkspaceContext>, Self::Error> {
        self.state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)
            .map(|state| state.records.get(&context_id).cloned())
    }

    async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
        self.state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)
            .map(|state| state.records.values().cloned().collect())
    }

    async fn compare_and_swap(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: WorkspaceContext,
    ) -> Result<bool, Self::Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)?;
        if stored_revision(&state.records, &context_id) != expected_revision {
            return Ok(false);
        }
        let before = state.records.get(&context_id).cloned();
        update_context_children(&mut state.children, before.as_ref(), Some(&replacement));
        state.records.insert(context_id, replacement);
        Ok(true)
    }

    async fn compare_and_swap_many(
        &self,
        expected_revisions: BTreeMap<WorkspaceContextId, u64>,
        replacements: Vec<WorkspaceContext>,
        require_exact_set: bool,
    ) -> Result<bool, Self::Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)?;
        if require_exact_set && state.records.len() != expected_revisions.len() {
            return Ok(false);
        }
        if expected_revisions
            .iter()
            .any(|(context_id, expected)| stored_revision(&state.records, context_id) != *expected)
            || replacements
                .iter()
                .any(|replacement| !expected_revisions.contains_key(&replacement.context_id))
        {
            return Ok(false);
        }
        for replacement in replacements {
            let before = state.records.get(&replacement.context_id).cloned();
            update_context_children(&mut state.children, before.as_ref(), Some(&replacement));
            state.records.insert(replacement.context_id, replacement);
        }
        Ok(true)
    }

    async fn discard_subtree(
        &self,
        parent: WorkspaceContextId,
        child: WorkspaceContextId,
        maximum: u32,
    ) -> Result<WorkspaceContextDiscardOutcome, Self::Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)?;
        let MemoryWorkspaceContextState { records, children } = &mut *state;
        Ok(discard_context_subtree(
            records, children, parent, child, maximum,
        ))
    }
}
