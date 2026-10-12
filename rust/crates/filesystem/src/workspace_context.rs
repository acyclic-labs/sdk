//! Durable, agent-neutral composition of recursively forked workspaces.
//!
//! A context is the smallest unit adapters need to identify: a set of roots,
//! their exact SDK workspaces, and one direct parent.  Host session and agent
//! identifiers intentionally stay outside this module.  Context records are
//! small control-plane facts; filesystem contents remain immutable SDK
//! generations and are never enumerated here.

use crate::WorkspaceId;
use crate::record_store::{Revisioned, stored_revision};
use crate::kernel::allocation::KernelOwnedAllocation;
#[cfg(not(target_arch = "wasm32"))]
use crate::kernel::allocation::KernelAccounting;
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

    /// Loads metadata on the caller's original native invocation.
    /// Custom adapters opt in explicitly; the default performs no I/O.
    #[cfg(not(target_arch = "wasm32"))]
    fn load_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<Option<crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>>, (WorkspaceContextError<Self::Error>, Option<crate::NativeAllocation>)>> + Send {
        let _ = context_id;
        async move {
            KernelAccounting::native(original, cancellation).check()
                .map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
            Err((WorkspaceContextError::NativeOriginalUnsupported, None))
        }
    }

    /// Performs the same revision comparison and index update with custody.
    /// The replacement's already-paid physical payload moves into the store.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_swap_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<bool, (WorkspaceContextError<Self::Error>, Option<crate::NativeAllocation>)>> + Send {
        let _ = (context_id, expected_revision);
        async move {
            KernelAccounting::native(original, cancellation).check()
                .map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
            drop(replacement);
            Err((WorkspaceContextError::NativeOriginalUnsupported, None))
        }
    }

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
    /// The original native invocation rejected measured metadata work.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeFilesystem(crate::FsError),
    /// A custom adapter has not implemented original-ledger metadata work.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("workspace-context store does not support original native work")]
    NativeOriginalUnsupported,
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

    /// Registers root metadata without content reads on the original invocation.
    ///
    /// The physical roots and their exact exclusive paid backing move together.
    /// The caller must not release or re-adopt the transferred allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn register_root_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        roots: (Vec<WorkspaceContextRoot>, crate::NativeAllocation),
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>, (WorkspaceContextError<S::Error>, Option<crate::NativeAllocation>)> {
        let record = native_root_record(context_id, roots, original, cancellation)
            .map_err(|error| (error, None))?;
        self.register_record(record, RegistrationMode::Native { original, cancellation })
            .await
            .map(|record| {
                crate::lazy_workspace::NativeOwnedCoreState::from_parts(
                    record.context, record.allocation.expect("native registration retains its physical payload"),
                )
            })
            .map_err(native_registration_failure)
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
        loop {
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

            context.roots.insert(root.root_id, root.clone());
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
        }
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
        loop {
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
        }
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
            root.mount_path = mount_path.clone();
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
        loop {
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
            root.workspace_name.clone_from(&workspace_name);
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
        }
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
        let record = ContextOwnedRecord::ordinary(WorkspaceContext {
            version: WORKSPACE_CONTEXT_VERSION,
            revision: 1,
            context_id,
            parent_context_id,
            roots: collect_roots(roots)?,
            state: WorkspaceContextState::Active,
        });
        self.register_record(record, RegistrationMode::Ordinary { _lifetime: std::marker::PhantomData })
            .await
            .map(|record| record.context)
            .map_err(|(error, _)| error)
    }

    async fn register_record(
        &self,
        record: ContextOwnedRecord,
        mode: RegistrationMode<'_>,
    ) -> Result<ContextOwnedRecord, (WorkspaceContextError<S::Error>, Option<KernelOwnedAllocation>)> {
        let context_id = record.context.context_id;
        let parent_context_id = record.context.parent_context_id;
        let roots = &record.context.roots;
        if parent_context_id.is_none()
            && roots.values().any(|root| root.parent_workspace_id.is_some())
        {
            return Err((WorkspaceContextError::InvalidRoots, None));
        }
        loop {
            // Native root registration has no parent and needs no temporary
            // expected-revision map. Ordinary child registration retains its
            // exact existing multi-record optimistic-concurrency operation.
            let committed = match mode {
                RegistrationMode::Ordinary { .. } => {
                    let mut expected = BTreeMap::from([(context_id, 0)]);
                    if let Some(parent_id) = parent_context_id {
                        let parent = self.resolve(parent_id).await.map_err(|error| (error, None))?;
                        if parent.state == WorkspaceContextState::Discarded
                            || roots.len() != parent.roots.len()
                            || roots.iter().any(|(root_id, root)| {
                                parent.roots.get(root_id).is_none_or(|parent_root| {
                                    root.source_path != parent_root.source_path
                                        || root.parent_workspace_id != Some(parent_root.workspace_id)
                                })
                            })
                        {
                            return Err((WorkspaceContextError::InvalidRoots, None));
                        }
                        expected.insert(parent_id, parent.revision);
                    }
                    self.store.compare_and_swap_many(expected, vec![record.context.clone()], false)
                        .await.map_err(|error| (WorkspaceContextError::Store(error), None))?
                }
                #[cfg(not(target_arch = "wasm32"))]
                RegistrationMode::Native { original, cancellation } => {
                    let accounting = KernelAccounting::native(original, cancellation);
                    accounting.check().map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
                    let replacement = clone_context_with_native_work(&record.context, original, cancellation)
                        .map_err(|error| (error, None))?;
                    accounting.check().map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
                    self.store.compare_and_swap_with_native_work(context_id, 0, replacement, original, cancellation)
                        .await.map_err(|(error, allocation)| (error, allocation.map(KernelOwnedAllocation::NativeOriginal)))?
                }
            };
            if committed {
                return Ok(record);
            }
            let existing = match mode {
                RegistrationMode::Ordinary { .. } => self.store.load(context_id).await
                    .map_err(|error| (WorkspaceContextError::Store(error), None))?
                    .map(ContextOwnedRecord::ordinary),
                #[cfg(not(target_arch = "wasm32"))]
                RegistrationMode::Native { original, cancellation } => {
                    KernelAccounting::native(original, cancellation).check()
                        .map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
                    self.store.load_with_native_work(context_id, original, cancellation).await
                        .map_err(|(error, allocation)| (error, allocation.map(KernelOwnedAllocation::NativeOriginal)))?
                        .map(ContextOwnedRecord::native)
                }
            };
            if let Some(existing) = existing {
                validate_record(&existing.context, context_id).map_err(|error| (error, None))?;
                return if same_registration(&existing.context, &record.context) {
                    Ok(existing)
                } else {
                    Err((WorkspaceContextError::ConflictingRegistration, None))
                };
            }
        }
    }

    async fn update(
        &self,
        context_id: WorkspaceContextId,
        mut action: impl FnMut(&mut WorkspaceContext) -> Result<(), WorkspaceContextError<S::Error>>,
    ) -> Result<WorkspaceContext, WorkspaceContextError<S::Error>> {
        loop {
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
        }
    }
}

#[derive(Clone, Copy)]
enum RegistrationMode<'a> {
    Ordinary { _lifetime: std::marker::PhantomData<&'a ()> },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    },
}

// Intrinsic persisted metadata custody; no external guard registry or journal.
struct ContextOwnedRecord {
    context: WorkspaceContext,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::NativeAllocation>,
    #[cfg(not(target_arch = "wasm32"))]
    entry_allocation: Option<crate::NativeAllocation>,
}

impl ContextOwnedRecord {
    fn ordinary(context: WorkspaceContext) -> Self {
        Self {
            context,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
            #[cfg(not(target_arch = "wasm32"))]
            entry_allocation: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native(record: crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>) -> Self {
        let (context, allocation) = record.into_parts();
        Self { context, allocation: Some(allocation), entry_allocation: None }
    }
}

impl Revisioned for ContextOwnedRecord {
    fn revision(&self) -> u64 { self.context.revision }
}

impl std::ops::Deref for ContextOwnedRecord {
    type Target = WorkspaceContext;
    fn deref(&self) -> &WorkspaceContext { &self.context }
}

impl std::ops::DerefMut for ContextOwnedRecord {
    fn deref_mut(&mut self) -> &mut WorkspaceContext { &mut self.context }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_registration_failure<E: std::error::Error + 'static>(
    failure: (WorkspaceContextError<E>, Option<KernelOwnedAllocation>),
) -> (WorkspaceContextError<E>, Option<crate::NativeAllocation>) {
    let (error, allocation) = failure;
    (error, allocation.and_then(|allocation| match allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => Some(allocation),
        KernelOwnedAllocation::Standalone => None,
    }))
}

#[cfg(not(target_arch = "wasm32"))]
fn context_root_backing(root: &WorkspaceContextRoot) -> Result<u64, crate::FsError> {
    (root.source_path.capacity() as u64)
        .checked_add(root.workspace_name.capacity() as u64)
        .and_then(|bytes| bytes.checked_add(root.mount_path.as_ref().map_or(0, |path| path.capacity() as u64)))
        .ok_or_else(|| crate::WorkError::Overflow.into())
}

#[cfg(not(target_arch = "wasm32"))]
fn context_native_backing(context: &WorkspaceContext) -> Result<u64, crate::FsError> {
    context.roots.values().try_fold(0u64, |bytes, root| {
        let backing = context_root_backing(root)?;
        bytes.checked_add(std::mem::size_of::<(WorkspaceRootId, WorkspaceContextRoot)>() as u64)
            .and_then(|bytes| bytes.checked_add(backing))
            .ok_or_else(|| crate::WorkError::Overflow.into())
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn native_root_record<E: std::error::Error + 'static>(
    context_id: WorkspaceContextId,
    roots: (Vec<WorkspaceContextRoot>, crate::NativeAllocation),
    original: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<ContextOwnedRecord, WorkspaceContextError<E>> {
    let mut accounting = KernelAccounting::native(original, cancellation);
    accounting.check().map_err(WorkspaceContextError::NativeFilesystem)?;
    let vec_bytes = (roots.0.capacity() as u64)
        .checked_mul(std::mem::size_of::<WorkspaceContextRoot>() as u64)
        .ok_or_else(|| WorkspaceContextError::NativeFilesystem(crate::WorkError::Overflow.into()))?;
    let bytes = roots.0.iter().try_fold(vec_bytes, |bytes, root| {
        bytes.checked_add(context_root_backing(root)?).ok_or_else(|| crate::FsError::from(crate::WorkError::Overflow))
    }).map_err(WorkspaceContextError::NativeFilesystem)?;
    if !roots.1.work().shares_allocation_context(original) || roots.1.bytes() != bytes {
        return Err(WorkspaceContextError::NativeFilesystem(
            crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout),
        ));
    }
    let (roots, allocation) = roots;
    let mut input = (roots, KernelOwnedAllocation::NativeOriginal(allocation));
    let copied = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<(WorkspaceRootId, WorkspaceContextRoot)>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    let collected = collect_roots_with(input.0, |step| match step {
        RootCollectionStep::Validate => accounting.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(WorkspaceContextError::NativeFilesystem),
        RootCollectionStep::Insert => {
            accounting.admit(copied).map_err(WorkspaceContextError::NativeFilesystem)?;
            let mut entry = accounting.claim_owned(
                std::mem::size_of::<(WorkspaceRootId, WorkspaceContextRoot)>() as u64, 0,
            ).map_err(WorkspaceContextError::NativeFilesystem)?;
            input.1.absorb(&mut entry).map_err(WorkspaceContextError::NativeFilesystem)
        }
        RootCollectionStep::Inserted => accounting.record_completed(copied)
            .map_err(WorkspaceContextError::NativeFilesystem),
    })?;
    let KernelOwnedAllocation::NativeOriginal(allocation) = input.1 else {
        return Err(WorkspaceContextError::IncompatibleState);
    };
    let mut record = ContextOwnedRecord {
        context: WorkspaceContext {
            version: WORKSPACE_CONTEXT_VERSION,
            revision: 1,
            context_id,
            parent_context_id: None,
            roots: collected,
            state: WorkspaceContextState::Active,
        },
        allocation: Some(allocation),
        entry_allocation: None,
    };
    // The IntoIter has destroyed the moved vector's backing, not its roots.
    record.allocation.as_mut().expect("native root custody").release(vec_bytes)
        .map_err(|error| WorkspaceContextError::NativeFilesystem(error.into()))?;
    accounting.check().map_err(WorkspaceContextError::NativeFilesystem)?;
    Ok(record)
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_context_path(
    path: &Path,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(PathBuf, KernelOwnedAllocation), crate::FsError> {
    let bytes = path.as_os_str().len() as u64;
    let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
    accounting.admit(copied)?;
    let allocation = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
    let mut owned = (PathBuf::new(), allocation);
    owned.0.try_reserve_exact(path.as_os_str().len())
        .map_err(|error| crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error)))?;
    accounting.reconcile_owned(&mut owned.1, bytes, owned.0.capacity() as u64)?;
    owned.0.push(path);
    accounting.record_completed(copied)?;
    Ok(owned)
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_context_name(
    name: &str,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(String, KernelOwnedAllocation), crate::FsError> {
    let bytes = name.len() as u64;
    let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
    accounting.admit(copied)?;
    let allocation = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
    let mut owned = (String::new(), allocation);
    owned.0.try_reserve_exact(name.len())
        .map_err(|error| crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error)))?;
    accounting.reconcile_owned(&mut owned.1, bytes, owned.0.capacity() as u64)?;
    owned.0.push_str(name);
    accounting.record_completed(copied)?;
    Ok(owned)
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_context_with_native_work<E: std::error::Error + 'static>(
    context: &WorkspaceContext,
    original: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>, WorkspaceContextError<E>> {
    let mut accounting = KernelAccounting::native(original, cancellation);
    let allocation = accounting.claim_owned(0, 0)
        .map_err(WorkspaceContextError::NativeFilesystem)?;
    let mut owned = (BTreeMap::new(), allocation);
    for (root_id, root) in &context.roots {
        accounting.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(WorkspaceContextError::NativeFilesystem)?;
        let mut entry = accounting.claim_owned(
            std::mem::size_of::<(WorkspaceRootId, WorkspaceContextRoot)>() as u64, 0,
        ).map_err(WorkspaceContextError::NativeFilesystem)?;
        owned.1.absorb(&mut entry).map_err(WorkspaceContextError::NativeFilesystem)?;
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<(WorkspaceRootId, WorkspaceContextRoot)>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied).map_err(WorkspaceContextError::NativeFilesystem)?;
        let mut source = clone_context_path(&root.source_path, &mut accounting)
            .map_err(WorkspaceContextError::NativeFilesystem)?;
        let mut name = clone_context_name(&root.workspace_name, &mut accounting)
            .map_err(WorkspaceContextError::NativeFilesystem)?;
        let mut mount = root.mount_path.as_deref().map(|path| clone_context_path(path, &mut accounting))
            .transpose().map_err(WorkspaceContextError::NativeFilesystem)?;
        owned.1.absorb(&mut source.1).map_err(WorkspaceContextError::NativeFilesystem)?;
        owned.1.absorb(&mut name.1).map_err(WorkspaceContextError::NativeFilesystem)?;
        if let Some((_, allocation)) = &mut mount {
            owned.1.absorb(allocation).map_err(WorkspaceContextError::NativeFilesystem)?;
        }
        let source_path = source.0;
        let workspace_name = name.0;
        let mount_path = mount.map(|(path, _)| path);
        accounting.admit(copied).map_err(WorkspaceContextError::NativeFilesystem)?;
        owned.0.insert(*root_id, WorkspaceContextRoot {
            root_id: root.root_id,
            source_path,
            workspace_id: root.workspace_id,
            workspace_name,
            parent_workspace_id: root.parent_workspace_id,
            mount_path,
        });
        accounting.record_completed(copied).map_err(WorkspaceContextError::NativeFilesystem)?;
    }
    let header_copy = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<WorkspaceContext>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(header_copy).map_err(WorkspaceContextError::NativeFilesystem)?;
    let KernelOwnedAllocation::NativeOriginal(allocation) = owned.1 else {
        return Err(WorkspaceContextError::IncompatibleState);
    };
    let result = crate::lazy_workspace::NativeOwnedCoreState::from_parts(WorkspaceContext {
        version: context.version,
        revision: context.revision,
        context_id: context.context_id,
        parent_context_id: context.parent_context_id,
        roots: owned.0,
        state: context.state,
    }, allocation);
    accounting.record_completed(header_copy).map_err(WorkspaceContextError::NativeFilesystem)?;
    Ok(result)
}

fn collect_roots<E: std::error::Error + 'static>(
    roots: impl IntoIterator<Item = WorkspaceContextRoot>,
) -> Result<BTreeMap<WorkspaceRootId, WorkspaceContextRoot>, WorkspaceContextError<E>> {
    collect_roots_with(roots, |_| Ok(()))
}

enum RootCollectionStep {
    Validate,
    Insert,
    Inserted,
}

fn collect_roots_with<E: std::error::Error + 'static>(
    roots: impl IntoIterator<Item = WorkspaceContextRoot>,
    mut insertion: impl FnMut(RootCollectionStep) -> Result<(), WorkspaceContextError<E>>,
) -> Result<BTreeMap<WorkspaceRootId, WorkspaceContextRoot>, WorkspaceContextError<E>> {
    let mut collected = BTreeMap::new();
    for root in roots {
        insertion(RootCollectionStep::Validate)?;
        if root.workspace_name.is_empty()
            || !is_canonical(&root.source_path)
            || root
                .mount_path
                .as_deref()
                .is_some_and(|path| !is_canonical(path))
        {
            return Err(WorkspaceContextError::InvalidRoots);
        }
        insertion(RootCollectionStep::Insert)?;
        let duplicate = collected.insert(root.root_id, root).is_some();
        insertion(RootCollectionStep::Inserted)?;
        if duplicate {
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
    records: BTreeMap<WorkspaceContextId, ContextOwnedRecord>,
    children: WorkspaceContextChildren,
}

fn replace_context_record(
    state: &mut MemoryWorkspaceContextState,
    context_id: WorkspaceContextId,
    replacement: ContextOwnedRecord,
) {
    let MemoryWorkspaceContextState { records, children } = state;
    #[cfg(not(target_arch = "wasm32"))]
    let mut replacement = replacement;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(before) = records.get_mut(&context_id) {
        // This is the same physical retained entry, even when its payload is
        // replaced on a different original invocation. Never adopt it again.
        replacement.entry_allocation = before.entry_allocation.take();
    }
    update_context_children(
        children, records.get(&context_id).map(|record| &record.context), Some(&replacement.context),
    );
    records.insert(context_id, replacement);
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>>, (WorkspaceContextError<Self::Error>, Option<crate::NativeAllocation>)> {
        KernelAccounting::native(original, cancellation).charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
        let state = self.state.lock()
            .map_err(|_| (WorkspaceContextError::Store(MemoryWorkspaceContextStoreError), None))?;
        state.records.get(&context_id).map(|record| {
            clone_context_with_native_work(&record.context, original, cancellation)
        }).transpose().map_err(|error| (error, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: crate::lazy_workspace::NativeOwnedCoreState<WorkspaceContext>,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<bool, (WorkspaceContextError<Self::Error>, Option<crate::NativeAllocation>)> {
        let mut replacement = ContextOwnedRecord::native(replacement);
        if !replacement.allocation.as_ref().expect("native record custody").work().shares_allocation_context(original) {
            return Err((WorkspaceContextError::IncompatibleState, None));
        }
        let required = context_native_backing(&replacement.context)
            .map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
        if replacement.allocation.as_ref().expect("native record custody").bytes() < required {
            return Err((WorkspaceContextError::IncompatibleState, None));
        }
        let mut accounting = KernelAccounting::native(original, cancellation);
        accounting.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
        let mut state = self.state.lock()
            .map_err(|_| (WorkspaceContextError::Store(MemoryWorkspaceContextStoreError), None))?;
        if stored_revision(&state.records, &context_id) != expected_revision {
            return Ok(false);
        }
        if !state.records.contains_key(&context_id) {
            let backing = accounting.claim_owned(
                std::mem::size_of::<(WorkspaceContextId, ContextOwnedRecord)>() as u64, 0,
            ).map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
            if let KernelOwnedAllocation::NativeOriginal(backing) = backing {
                replacement.entry_allocation = Some(backing);
            }
        }
        let mut bytes = 0u64;
        if let Some(parent) = replacement.context.parent_context_id {
            if !state.children.contains_key(&parent) {
                bytes = bytes.checked_add(std::mem::size_of::<(WorkspaceContextId, BTreeSet<WorkspaceContextId>)>() as u64)
                    .ok_or_else(|| (WorkspaceContextError::NativeFilesystem(crate::WorkError::Overflow.into()), None))?;
            }
            if state.children.get(&parent).is_none_or(|children| !children.contains(&replacement.context.context_id)) {
                bytes = bytes.checked_add(std::mem::size_of::<WorkspaceContextId>() as u64)
                    .ok_or_else(|| (WorkspaceContextError::NativeFilesystem(crate::WorkError::Overflow.into()), None))?;
            }
        }
        let mut backing = accounting.claim_owned(bytes, 0)
            .map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
        if let KernelOwnedAllocation::NativeOriginal(backing) = &mut backing {
            replacement.allocation.as_mut().expect("native record custody").absorb(backing)
                .map_err(|error| (WorkspaceContextError::NativeFilesystem(error.into()), None))?;
        }
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<(WorkspaceContextId, WorkspaceContext)>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied).map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
        replace_context_record(&mut state, context_id, replacement);
        accounting.record_completed(copied).map_err(|error| (WorkspaceContextError::NativeFilesystem(error), None))?;
        Ok(true)
    }

    async fn load(
        &self,
        context_id: WorkspaceContextId,
    ) -> Result<Option<WorkspaceContext>, Self::Error> {
        self.state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)
            .map(|state| state.records.get(&context_id).map(|record| record.context.clone()))
    }

    async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
        self.state
            .lock()
            .map_err(|_| MemoryWorkspaceContextStoreError)
            .map(|state| state.records.values().map(|record| record.context.clone()).collect())
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
        replace_context_record(&mut state, context_id, ContextOwnedRecord::ordinary(replacement));
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
            let context_id = replacement.context_id;
            replace_context_record(&mut state, context_id, ContextOwnedRecord::ordinary(replacement));
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
        let discarded = match plan_context_subtree_discard(
            state.records.get(&child).map(|record| &record.context),
            &state.children, parent, child, maximum,
        ) {
            Ok(discarded) => discarded,
            Err(outcome) => return Ok(outcome),
        };
        // Validate every revision before changing any record. Mutating scalar
        // metadata in place keeps each native record's intrinsic custody.
        if discarded.iter().any(|id| state.records.get(id).is_none_or(|record| {
            !record_is_valid(&record.context, *id) || record.revision.checked_add(1).is_none()
        })) {
            return Ok(WorkspaceContextDiscardOutcome::IncompatibleState);
        }
        for id in &discarded {
            let record = state.records.get_mut(id).expect("validated subtree record");
            record.context.revision += 1;
            record.context.state = WorkspaceContextState::Discarded;
            for root in record.context.roots.values_mut() {
                if let Some(path) = root.mount_path.take() {
                    #[cfg(not(target_arch = "wasm32"))]
                    let bytes = path.capacity() as u64;
                    drop(path);
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(allocation) = &mut record.allocation {
                        allocation.release(bytes).map_err(|_| MemoryWorkspaceContextStoreError)?;
                    }
                }
            }
        }
        Ok(WorkspaceContextDiscardOutcome::Discarded(discarded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct CountingContextStore {
        inner: MemoryWorkspaceContextStore,
        loads: Arc<AtomicUsize>,
        lists: Arc<AtomicUsize>,
        swaps: Arc<AtomicUsize>,
        duplicate_list: bool,
        insert_before_swap: Mutex<Option<WorkspaceContext>>,
    }

    impl CountingContextStore {
        fn reset(&self) {
            self.loads.store(0, Ordering::Relaxed);
            self.lists.store(0, Ordering::Relaxed);
            self.swaps.store(0, Ordering::Relaxed);
        }

        fn counts(&self) -> (usize, usize, usize) {
            (
                self.loads.load(Ordering::Relaxed),
                self.lists.load(Ordering::Relaxed),
                self.swaps.load(Ordering::Relaxed),
            )
        }
    }

    impl WorkspaceContextStore for CountingContextStore {
        type Error = MemoryWorkspaceContextStoreError;

        async fn load(
            &self,
            context_id: WorkspaceContextId,
        ) -> Result<Option<WorkspaceContext>, Self::Error> {
            self.loads.fetch_add(1, Ordering::Relaxed);
            self.inner.load(context_id).await
        }

        async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
            self.lists.fetch_add(1, Ordering::Relaxed);
            let mut records = self.inner.list().await?;
            if self.duplicate_list {
                records.extend(records.last().cloned());
            }
            Ok(records)
        }

        async fn compare_and_swap(
            &self,
            context_id: WorkspaceContextId,
            expected_revision: u64,
            replacement: WorkspaceContext,
        ) -> Result<bool, Self::Error> {
            self.swaps.fetch_add(1, Ordering::Relaxed);
            self.inner
                .compare_and_swap(context_id, expected_revision, replacement)
                .await
        }

        async fn compare_and_swap_many(
            &self,
            expected_revisions: BTreeMap<WorkspaceContextId, u64>,
            replacements: Vec<WorkspaceContext>,
            require_exact_set: bool,
        ) -> Result<bool, Self::Error> {
            self.swaps.fetch_add(1, Ordering::Relaxed);
            let inserted = self
                .insert_before_swap
                .lock()
                .expect("injection lock")
                .take();
            if let Some(record) = inserted {
                assert!(
                    self.inner
                        .compare_and_swap(record.context_id, 0, record)
                        .await?
                );
            }
            self.inner
                .compare_and_swap_many(expected_revisions, replacements, require_exact_set)
                .await
        }

        async fn discard_subtree(
            &self,
            parent: WorkspaceContextId,
            child: WorkspaceContextId,
            maximum: u32,
        ) -> Result<WorkspaceContextDiscardOutcome, Self::Error> {
            self.swaps.fetch_add(1, Ordering::Relaxed);
            self.inner.discard_subtree(parent, child, maximum).await
        }
    }

    fn context(byte: u8) -> WorkspaceContextId {
        WorkspaceContextId::from_bytes([byte; 16])
    }

    fn root_id(byte: u8) -> WorkspaceRootId {
        WorkspaceRootId::from_bytes([byte; 16])
    }

    fn workspace(byte: u8) -> WorkspaceId {
        WorkspaceId::from_bytes([byte; 16])
    }

    fn numbered_context(number: u128) -> WorkspaceContextId {
        WorkspaceContextId::from_bytes(number.to_le_bytes())
    }

    fn numbered_root(number: u128) -> WorkspaceRootId {
        WorkspaceRootId::from_bytes(number.to_le_bytes())
    }

    fn numbered_workspace(number: u128) -> WorkspaceId {
        WorkspaceId::from_bytes(number.to_le_bytes())
    }

    fn scaled_roots(generation: u128, count: u128) -> Vec<WorkspaceContextRoot> {
        (0..count)
            .map(|number| WorkspaceContextRoot {
                root_id: numbered_root(number + 1),
                source_path: PathBuf::from(if cfg!(windows) {
                    format!("C:\\scale-root-{number}")
                } else {
                    format!("/scale-root-{number}")
                }),
                workspace_id: numbered_workspace(generation * count + number + 1),
                workspace_name: format!("scale-{generation}-{number}"),
                parent_workspace_id: (generation > 0)
                    .then(|| numbered_workspace((generation - 1) * count + number + 1)),
                mount_path: None,
            })
            .collect()
    }

    fn root(root_byte: u8, workspace_byte: u8, parent: Option<u8>) -> WorkspaceContextRoot {
        WorkspaceContextRoot {
            root_id: root_id(root_byte),
            source_path: PathBuf::from(if cfg!(windows) { "C:\\work" } else { "/work" }),
            workspace_id: workspace(workspace_byte),
            workspace_name: format!("workspace-{workspace_byte}"),
            parent_workspace_id: parent.map(workspace),
            mount_path: None,
        }
    }

    #[tokio::test]
    async fn recursive_contexts_require_exact_direct_parent_roots() {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        registry
            .register_root(context(1), [root(9, 1, None)])
            .await
            .expect("root");
        registry
            .register_child(context(2), context(1), [root(9, 2, Some(1))])
            .await
            .expect("child");
        registry
            .register_child(context(3), context(2), [root(9, 3, Some(2))])
            .await
            .expect("grandchild");

        registry
            .authorize_parent(context(3), context(2))
            .await
            .expect("direct parent");
        assert!(matches!(
            registry.authorize_parent(context(3), context(1)).await,
            Err(WorkspaceContextError::UnauthorizedParent)
        ));
    }

    #[tokio::test]
    async fn root_removal_requires_descendants_to_release_the_binding_first() {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        registry
            .register_root(context(41), scaled_roots(0, 2))
            .await
            .expect("two root bindings");
        registry
            .register_child(context(42), context(41), scaled_roots(1, 2))
            .await
            .expect("child bindings");
        assert!(matches!(
            registry.remove_root(context(41), numbered_root(2)).await,
            Err(WorkspaceContextError::RootInUse)
        ));
        let child = registry
            .remove_root(context(42), numbered_root(2))
            .await
            .expect("child releases its root");
        assert!(!child.roots.contains_key(&numbered_root(2)));
        let parent = registry
            .remove_root(context(41), numbered_root(2))
            .await
            .expect("parent releases unused root");
        assert!(!parent.roots.contains_key(&numbered_root(2)));
        assert!(matches!(
            registry.remove_root(context(41), numbered_root(1)).await,
            Err(WorkspaceContextError::InvalidRoots)
        ));
    }

    #[tokio::test]
    async fn deep_fork_chain_root_removal_keeps_unrelated_lineage_and_live_descendants() {
        const DEPTH: u128 = 1024;
        let ordered_context = |number: u128| WorkspaceContextId::from_bytes(number.to_be_bytes());
        let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
        // The unrelated chain sorts before the root being removed, exercising
        // cached ancestor walks before inspecting that root's descendants.
        registry
            .register_root(ordered_context(1), scaled_roots(0, 2))
            .await
            .expect("unrelated root");
        for generation in 1..=DEPTH {
            registry
                .register_child(
                    ordered_context(generation + 1),
                    ordered_context(generation),
                    scaled_roots(generation, 2),
                )
                .await
                .expect("deep unrelated child");
        }
        let target = ordered_context(DEPTH + 2);
        registry
            .register_root(target, scaled_roots(0, 2))
            .await
            .expect("independent target root");
        let before = registry
            .resolve(target)
            .await
            .expect("target before removal");
        let mut expected_records = registry
            .store()
            .list()
            .await
            .expect("all records before removal");
        registry.store().reset();
        let removed = registry
            .remove_root(target, numbered_root(2))
            .await
            .expect("unrelated descendants do not block removal");
        assert_eq!(registry.store().counts(), (0, 1, 1));
        assert_eq!(removed.revision, before.revision + 1);
        assert_eq!(removed.roots.len(), 1);
        assert_eq!(
            removed.roots.get(&numbered_root(1)),
            before.roots.get(&numbered_root(1))
        );
        *expected_records
            .iter_mut()
            .find(|record| record.context_id == target)
            .expect("target record") = removed;
        assert_eq!(
            registry
                .store()
                .list()
                .await
                .expect("all records after removal"),
            expected_records
        );
        registry
            .adopt_root(target, before.roots[&numbered_root(2)].clone())
            .await
            .expect("restore target binding");

        for generation in 1..=DEPTH {
            registry
                .register_child(
                    ordered_context(DEPTH * 2 + 3 - generation),
                    if generation == 1 {
                        target
                    } else {
                        ordered_context(DEPTH * 2 + 4 - generation)
                    },
                    scaled_roots(generation, 2),
                )
                .await
                .expect("deep target child");
            registry
                .set_active(ordered_context(DEPTH * 2 + 3 - generation), false)
                .await
                .expect("freeze retained descendant");
        }
        // Frozen descendants remain retained and must still block removal.
        let records_before = registry
            .store()
            .list()
            .await
            .expect("all records before rejection");
        registry.store().reset();
        assert!(matches!(
            registry.remove_root(target, numbered_root(2)).await,
            Err(WorkspaceContextError::RootInUse)
        ));
        assert_eq!(registry.store().counts(), (0, 1, 0));
        assert_eq!(
            registry
                .store()
                .list()
                .await
                .expect("all records after rejection"),
            records_before
        );
    }

    #[tokio::test]
    async fn root_removal_rechecks_a_concurrently_registered_descendant() {
        let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
        let before = registry
            .register_root(context(41), scaled_roots(0, 2))
            .await
            .expect("target root");
        let child = WorkspaceContext {
            version: WORKSPACE_CONTEXT_VERSION,
            revision: 1,
            context_id: context(42),
            parent_context_id: Some(context(41)),
            roots: scaled_roots(1, 2)
                .into_iter()
                .map(|root| (root.root_id, root))
                .collect(),
            state: WorkspaceContextState::Active,
        };
        *registry
            .store()
            .insert_before_swap
            .lock()
            .expect("injection lock") = Some(child.clone());
        registry.store().reset();
        assert!(matches!(
            registry.remove_root(context(41), numbered_root(2)).await,
            Err(WorkspaceContextError::RootInUse)
        ));
        assert_eq!(registry.store().counts(), (0, 2, 1));
        assert_eq!(registry.resolve(context(41)).await.expect("target"), before);
        assert_eq!(
            registry.resolve(context(42)).await.expect("new child"),
            child
        );
    }

    #[tokio::test]
    async fn root_removal_rejects_invalid_stored_records_before_publication() {
        let mut accepted = Vec::new();
        for corruption in 0..9 {
            let registry = WorkspaceContextRegistry::new(CountingContextStore {
                duplicate_list: corruption == 7,
                ..CountingContextStore::default()
            });
            registry
                .register_root(context(41), scaled_roots(0, 2))
                .await
                .expect("target root");
            registry
                .register_root(context(42), scaled_roots(0, 2))
                .await
                .expect("unrelated root");
            {
                let mut state = registry.store().inner.state.lock().expect("store lock");
                let record = state.records.get_mut(&context(42)).expect("stored context");
                match corruption {
                    0 => record.version = 0,
                    1 => record.revision = 0,
                    2 => record.roots.clear(),
                    7 => {}
                    8 => record.version += 1,
                    _ => {
                        let root = record.roots.get_mut(&numbered_root(1)).expect("root");
                        match corruption {
                            3 => root.root_id = numbered_root(99),
                            4 => root.source_path = PathBuf::from("relative"),
                            5 => root.mount_path = Some(PathBuf::from("relative")),
                            6 => root.workspace_name.clear(),
                            _ => unreachable!(),
                        }
                    }
                }
            }
            let before = registry
                .store()
                .inner
                .list()
                .await
                .expect("corrupt snapshot");
            registry.store().reset();
            if !matches!(
                registry.remove_root(context(41), numbered_root(2)).await,
                Err(WorkspaceContextError::IncompatibleState)
            ) {
                accepted.push(corruption);
                continue;
            }
            assert_eq!(registry.store().counts(), (0, 1, 0));
            assert_eq!(
                registry
                    .store()
                    .inner
                    .list()
                    .await
                    .expect("unchanged snapshot"),
                before
            );
        }
        assert!(accepted.is_empty(), "accepted corruptions {accepted:?}");
    }

    #[tokio::test]
    async fn root_removal_rejects_cyclic_and_missing_ancestry_without_mutation() {
        for cyclic in [false, true] {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(41), scaled_roots(0, 2))
                .await
                .expect("target root");
            registry
                .register_root(context(42), scaled_roots(0, 2))
                .await
                .expect("unrelated root");
            // Persisted adapters can return malformed lineage after corruption.
            // Registration intentionally cannot create either of these states.
            registry
                .store()
                .state
                .lock()
                .expect("store lock")
                .records
                .get_mut(&context(42))
                .expect("unrelated context")
                .parent_context_id = Some(if cyclic { context(42) } else { context(99) });
            let before = registry
                .store()
                .list()
                .await
                .expect("records before removal");
            assert!(matches!(
                registry.remove_root(context(41), numbered_root(2)).await,
                Err(WorkspaceContextError::IncompatibleState)
            ));
            assert_eq!(
                registry
                    .store()
                    .list()
                    .await
                    .expect("records after removal"),
                before
            );
        }
    }

    #[tokio::test]
    async fn roots_are_adopted_lazily_through_the_exact_parent() {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        registry
            .register_root(context(1), [root(9, 1, None)])
            .await
            .expect("root");
        registry
            .register_child(context(2), context(1), [root(9, 2, Some(1))])
            .await
            .expect("child");

        let source_path = PathBuf::from(if cfg!(windows) {
            "C:\\second-work"
        } else {
            "/second-work"
        });
        let parent_root = WorkspaceContextRoot {
            root_id: root_id(10),
            source_path: source_path.clone(),
            workspace_id: workspace(3),
            workspace_name: "workspace-3".to_owned(),
            parent_workspace_id: None,
            mount_path: None,
        };
        registry
            .adopt_root(context(1), parent_root.clone())
            .await
            .expect("parent adopts root");
        let child_root = WorkspaceContextRoot {
            root_id: root_id(10),
            source_path,
            workspace_id: workspace(4),
            workspace_name: "workspace-4".to_owned(),
            parent_workspace_id: Some(workspace(3)),
            mount_path: None,
        };
        let adopted = registry
            .adopt_root(context(2), child_root.clone())
            .await
            .expect("child adopts parent root");
        assert_eq!(adopted.roots.get(&root_id(10)), Some(&child_root));
        assert_eq!(
            registry
                .adopt_root(context(2), child_root)
                .await
                .expect("exact retry"),
            adopted
        );
    }

    #[tokio::test]
    async fn child_cannot_adopt_a_root_outside_its_direct_parent() {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        registry
            .register_root(context(1), [root(9, 1, None)])
            .await
            .expect("root");
        registry
            .register_child(context(2), context(1), [root(9, 2, Some(1))])
            .await
            .expect("child");

        let unauthorized = WorkspaceContextRoot {
            root_id: root_id(10),
            source_path: PathBuf::from(if cfg!(windows) {
                "C:\\outside"
            } else {
                "/outside"
            }),
            workspace_id: workspace(4),
            workspace_name: "workspace-4".to_owned(),
            parent_workspace_id: Some(workspace(3)),
            mount_path: None,
        };
        assert!(matches!(
            registry.adopt_root(context(2), unauthorized).await,
            Err(WorkspaceContextError::InvalidRoots)
        ));
    }

    #[tokio::test]
    async fn mount_routing_wins_and_discard_is_recursive() {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        registry
            .register_root(context(1), [root(9, 1, None)])
            .await
            .expect("root");
        registry
            .register_child(context(2), context(1), [root(9, 2, Some(1))])
            .await
            .expect("child");
        registry
            .register_child(context(3), context(2), [root(9, 3, Some(2))])
            .await
            .expect("grandchild");
        let mount = PathBuf::from(if cfg!(windows) {
            "C:\\mounts\\child"
        } else {
            "/mounts/child"
        });
        registry
            .set_mount(context(2), root_id(9), Some(mount.clone()))
            .await
            .expect("set mount");
        let route = registry
            .route(context(2), &mount.join("src/lib.rs"))
            .await
            .expect("route")
            .expect("mapped");
        assert_eq!(route.kind, WorkspaceRouteKind::Mount);
        assert_eq!(route.relative_path, PathBuf::from("src/lib.rs"));

        let discarded = registry
            .discard_subtree(context(1), context(2), 8)
            .await
            .expect("discard");
        assert_eq!(discarded, vec![context(2), context(3)]);
        assert_eq!(
            registry
                .resolve(context(3))
                .await
                .expect("grandchild")
                .state,
            WorkspaceContextState::Discarded
        );
        assert!(matches!(
            registry.route(context(2), &mount).await,
            Err(WorkspaceContextError::Discarded)
        ));
        assert!(matches!(
            registry
                .set_mount(context(2), root_id(9), Some(mount.clone()))
                .await,
            Err(WorkspaceContextError::Discarded)
        ));
        assert!(matches!(
            registry.authorize_parent(context(2), context(1)).await,
            Err(WorkspaceContextError::Discarded)
        ));
        assert!(matches!(
            registry.discard_subtree(context(1), context(2), 8).await,
            Err(WorkspaceContextError::Discarded)
        ));
    }

    #[tokio::test]
    async fn workspace_rebinding_preserves_direct_parent_lineage() {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        registry
            .register_root(context(1), [root(9, 1, None)])
            .await
            .expect("root");
        registry
            .register_child(context(2), context(1), [root(9, 2, Some(1))])
            .await
            .expect("child");

        assert!(matches!(
            registry
                .set_workspace(
                    context(2),
                    root_id(9),
                    workspace(3),
                    "topic".to_owned(),
                    Some(workspace(99)),
                )
                .await,
            Err(WorkspaceContextError::InvalidRoots)
        ));
        assert!(matches!(
            registry
                .set_workspace(
                    context(1),
                    root_id(9),
                    workspace(4),
                    "root-topic".to_owned(),
                    Some(workspace(1)),
                )
                .await,
            Err(WorkspaceContextError::InvalidRoots)
        ));
        let child = registry.resolve(context(2)).await.expect("child");
        assert_eq!(child.roots[&root_id(9)].workspace_id, workspace(2));
    }

    #[tokio::test]
    async fn many_roots_and_deep_lineage_have_constant_store_work_per_step() {
        const ROOTS: u128 = 64;
        const DEPTH: u128 = 64;

        let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
        registry
            .register_root(numbered_context(1), scaled_roots(0, ROOTS))
            .await
            .expect("large root context");

        for generation in 1..DEPTH {
            registry.store().reset();
            registry
                .register_child(
                    numbered_context(generation + 1),
                    numbered_context(generation),
                    scaled_roots(generation, ROOTS),
                )
                .await
                .expect("deep child context");
            assert_eq!(
                registry.store().counts(),
                (1, 0, 1),
                "child registration must inspect only its direct parent"
            );
        }

        registry.store().reset();
        let route = registry
            .route(
                numbered_context(DEPTH),
                Path::new(if cfg!(windows) {
                    "C:\\scale-root-63\\nested\\file"
                } else {
                    "/scale-root-63/nested/file"
                }),
            )
            .await
            .expect("route")
            .expect("matching root");
        assert_eq!(route.root_id, numbered_root(ROOTS));
        assert_eq!(registry.store().counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn ten_thousand_durable_contexts_and_128_active_children_remain_direct() {
        const CHILDREN: u128 = 10_000;
        const ACTIVE_CHILDREN: u128 = 128;

        let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
        let parent_context = numbered_context(1);
        let root = scaled_roots(0, 1).pop().expect("root binding");
        let parent_workspace = root.workspace_id;
        registry
            .register_root(parent_context, [root.clone()])
            .await
            .expect("root context");

        for number in 0..CHILDREN {
            registry.store().reset();
            registry
                .register_child(
                    numbered_context(number + 2),
                    parent_context,
                    [WorkspaceContextRoot {
                        workspace_id: numbered_workspace(number + 2),
                        workspace_name: format!("sibling-{number}"),
                        parent_workspace_id: Some(parent_workspace),
                        ..root.clone()
                    }],
                )
                .await
                .expect("sibling child context");
            assert_eq!(
                registry.store().counts(),
                (1, 0, 1),
                "registration must inspect only the direct parent"
            );
        }

        for number in ACTIVE_CHILDREN..CHILDREN {
            registry.store().reset();
            registry
                .set_active(numbered_context(number + 2), false)
                .await
                .expect("freeze retained sibling");
            assert_eq!(registry.store().counts(), (1, 0, 1));
        }

        registry.store().reset();
        let records = registry.store().list().await.expect("durable contexts");
        assert_eq!(registry.store().counts(), (0, 1, 0));
        assert_eq!(
            records.len(),
            usize::try_from(CHILDREN + 1).expect("context count fits usize")
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| {
                    record.parent_context_id == Some(parent_context)
                        && record.state == WorkspaceContextState::Active
                })
                .count(),
            usize::try_from(ACTIVE_CHILDREN).expect("active child count fits usize")
        );

        registry.store().reset();
        let route = registry
            .route(
                numbered_context(ACTIVE_CHILDREN + 1),
                &root.source_path.join("nested/file"),
            )
            .await
            .expect("route")
            .expect("matching root");
        assert_eq!(route.root_id, root.root_id);
        assert_eq!(registry.store().counts(), (1, 0, 0));

        registry.store().reset();
        assert!(matches!(
            registry
                .discard_subtree(parent_context, numbered_context(2), 0)
                .await,
            Err(WorkspaceContextError::TraversalLimit)
        ));
        assert_eq!(
            registry.store().counts(),
            (0, 0, 1),
            "the registry must delegate one atomic bounded discard without listing all contexts"
        );
        assert_eq!(
            registry
                .resolve(numbered_context(2))
                .await
                .expect("bounded rejection changes nothing")
                .state,
            WorkspaceContextState::Active
        );
    }
}
