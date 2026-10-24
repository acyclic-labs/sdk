//! An opaque tree whose derived objects have invocation-local custody.
use super::*;
use crate::demand::DemandSource;
use crate::{LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore};
use acyclic_stream::OwnedBytes;

/// Immutable tree backed by disposable objects and read-only authority.
pub struct ReadOnlyGeneration<'a, A, O> {
    inner: Generation<ReadAuthority<'a, A>, TransientObjects<'a, O>>,
    original: &'a Fs<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Rebinds an authenticated tree without granting publication authority.
    pub fn read_only_generation<'a>(
        &'a self,
        workspace: &crate::Workspace<A, O>,
        generation: GenerationId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<ReadOnlyGeneration<'a, A, O>, crate::FsError> {
        if !Arc::ptr_eq(&self.inner, &workspace.volume.fs.inner) {
            return Err(crate::FsError::VolumeMismatch);
        }
        let filesystem = private_fs(self, work)?;
        Ok(ReadOnlyGeneration {
            original: self,
            inner: Generation {
                workspace: rebind(workspace, &filesystem),
                id: generation,
                publication_closure: None,
            },
        })
    }

    /// Derives an immutable lazy cut without creating a scratch workspace.
    pub async fn read_only_snapshot<'a, D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &'a self,
        workspace: &crate::Workspace<A, O>,
        view: &LazyWorkspace<A, O, D, S>,
        cut: LazySnapshotRef,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<ReadOnlyGeneration<'a, A, O>, crate::LazyWorkspaceError> {
        let tree = self.read_only_generation(workspace, cut.authored_generation, work)?;
        let inner = view
            .inspect_snapshot_on(tree.inner.workspace, cut, work, cancellation)
            .await?;
        Ok(ReadOnlyGeneration {
            inner,
            original: self,
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ReadOnlyGeneration<'_, A, O> {
    /// Resolves another tree within the same disposable object composition.
    pub fn related_generation(
        &self,
        workspace: &crate::Workspace<A, O>,
        generation: GenerationId,
    ) -> Result<Self, crate::WorkspaceError> {
        if !Arc::ptr_eq(&self.original.inner, &workspace.volume.fs.inner) {
            return Err(crate::WorkspaceError::ForeignGeneration);
        }
        Ok(Self {
            original: self.original,
            inner: Generation {
                workspace: rebind(workspace, &self.inner.workspace.volume.fs),
                id: generation,
                publication_closure: None,
            },
        })
    }

    /// Resolves a lazy cut using the same disposable object composition.
    pub async fn related_snapshot<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &self,
        workspace: &crate::Workspace<A, O>,
        view: &LazyWorkspace<A, O, D, S>,
        cut: LazySnapshotRef,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Self, crate::LazyWorkspaceError> {
        let tree = self.related_generation(workspace, cut.authored_generation)?;
        let inner = view
            .inspect_snapshot_on(tree.inner.workspace, cut, work, cancellation)
            .await?;
        Ok(Self {
            original: self.original,
            inner,
        })
    }

    /// Exact content identity, which need not exist in durable storage.
    #[must_use]
    pub const fn id(&self) -> GenerationId {
        self.inner.id
    }

    /// Inspects physical content into this tree's private object backend.
    pub async fn capture_native(
        mut self,
        view: NativeGitView<'_>,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Self, NativeGitInspectionError> {
        let workspace = &self.inner.workspace;
        let mut candidate = workspace
            .checkout(
                GenerationSelector::Exact(self.inner.id),
                CheckoutMode::tracking_transaction(),
                work,
                cancellation,
            )
            .await?;
        let root = NamespacePath::new(Vec::new(), workspace.limits())
            .map_err(crate::CaptureError::from)?;
        let baseline = view.materialized.then(|| {
            crate::native_capture::NativeViewBaseline::new(
                view.options.expected_root_identity,
                false,
            )
        });
        crate::native_capture::capture_subtrees_with_native_work(
            &mut candidate,
            &[root],
            view.options,
            view.policy,
            cancellation,
            baseline.as_ref(),
            work,
        )
        .await
        .map_err(|failure| failure.error)?;
        self.inner.id =
            GenerationId::new(candidate.checkpoint_root(work, cancellation).await?.digest);
        Ok(self)
    }

    /// Compares both immutable trees with their own Git ignore policies.
    pub async fn diff_to(
        &self,
        to: &Self,
        repository: &GitIgnorePolicy,
        tracked: &BTreeSet<String>,
        cancellation: &CancellationToken,
    ) -> Result<GitDiffCounts, crate::GitCaptureError> {
        crate::git_compatible_diff_counts(
            &self.inner,
            &to.inner,
            repository,
            tracked,
            100_000,
            cancellation,
        )
        .await
    }

    /// Reads file or link bytes without following symbolic links.
    pub async fn file_bytes(
        &self,
        path: &str,
        maximum: u64,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<OwnedBytes, crate::WorkspaceError> {
        if self.inner.stat(path, work, cancellation).await?.kind == FileKind::SymbolicLink {
            Ok(self
                .inner
                .read_symbolic_link(path, work, cancellation)
                .await?
                .bytes)
        } else {
            self.inner.read(path, maximum, work, cancellation).await
        }
    }
}
