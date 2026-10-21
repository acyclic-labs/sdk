//! Git operations over either a source-backed checkout or an SDK volume.
use super::*;

/// The task-selected filesystem view, borrowed for one facade operation.
#[derive(Clone, Copy)]
pub enum View<'a> {
    /// A normal checkout whose unchanged files remain demand-backed.
    Checkout(&'a LocalLazyWorkspace),
    /// An SDK volume, including a child's copy-on-write fork.
    Volume(&'a LocalWorkspace),
}

/// A branch selected by the facade and retained by its task binding.
pub enum OwnedView {
    /// A branch retaining its original native source capability.
    Checkout(LocalLazyWorkspace),
    /// A branch of an SDK volume.
    Volume(LocalWorkspace),
}

impl OwnedView {
    /// Retains a source-backed branch for an ordinary checkout binding.
    pub fn into_checkout(self) -> crate::Result<LocalLazyWorkspace> {
        match self {
            Self::Checkout(view) => Ok(view),
            Self::Volume(_) => Err(Error::Unauthorized(
                "expected a source-backed checkout branch".into(),
            )),
        }
    }

    /// Borrows the selected view without copying its source or contents.
    pub const fn view(&self) -> View<'_> {
        match self {
            Self::Checkout(view) => View::Checkout(view),
            Self::Volume(view) => View::Volume(view),
        }
    }

    /// The authored workspace supplying this view's generations.
    pub const fn workspace(&self) -> &LocalWorkspace {
        self.view().workspace()
    }
}

impl<'a> View<'a> {
    /// The authored workspace supplying this view's generations.
    pub const fn workspace(self) -> &'a LocalWorkspace {
        match self {
            Self::Checkout(view) => view.workspace(),
            Self::Volume(view) => view,
        }
    }

    pub(super) async fn read(
        self,
        path: &str,
        maximum: u64,
    ) -> crate::Result<acyclic_stream::OwnedBytes> {
        let cancellation = CancellationToken::new();
        match self {
            Self::Checkout(view) => view
                .read(
                    path,
                    maximum,
                    &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
                )
                .await
                .map(|chunk| chunk.bytes)
                .map_err(Error::from),
            Self::Volume(view) => {
                let generation = view
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                Ok(generation
                    .read(path, maximum, WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value)
            }
        }
    }

    pub(super) async fn tree(
        self,
        exact: bool,
        permit: PublicationPermit,
    ) -> crate::Result<GitTreeRef> {
        let cancellation = CancellationToken::new();
        match self {
            Self::Checkout(view) if exact => {
                let generation = view
                    .exactify(
                        "/",
                        permit,
                        &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
                    )
                    .await?;
                Ok(GitTreeRef::exact(view.workspace().id(), generation.id()))
            }
            Self::Checkout(view) => view
                .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&cancellation))
                .await
                .map(GitTreeRef::Lazy)
                .map_err(Error::from),
            Self::Volume(view) => {
                let generation = view
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                Ok(GitTreeRef::exact(view.id(), generation.id()))
            }
        }
    }

    pub(super) async fn fork(
        self,
        destination: &str,
        operation: OperationId,
        permit: PublicationPermit,
    ) -> crate::Result<OwnedView> {
        let cancellation = CancellationToken::new();
        let key = IdempotencyKey::from_bytes(operation.into_bytes());
        match self {
            Self::Checkout(view) => view
                .fork(
                    destination,
                    key,
                    permit,
                    &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
                )
                .await
                .map(|(child, _)| OwnedView::Checkout(child))
                .map_err(Error::from),
            Self::Volume(view) => {
                let generation = view
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                let child = view
                    .fork::<_, _, WorkspaceError>(
                        destination,
                        acyclic_fs::ForkOptions::from_generation(generation, key),
                        permit,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                        (),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                Ok(OwnedView::Volume(child))
            }
        }
    }

    pub(super) async fn related(self, workspace: LocalWorkspace) -> crate::Result<OwnedView> {
        match self {
            Self::Checkout(view) => view
                .open_related(
                    workspace,
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                )
                .await
                .map(OwnedView::Checkout)
                .map_err(Error::from),
            Self::Volume(_) => Ok(OwnedView::Volume(workspace)),
        }
    }

    pub(crate) async fn optional(
        self,
        path: &str,
        maximum: u64,
    ) -> crate::Result<Option<acyclic_stream::OwnedBytes>> {
        match self.read(path, maximum).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(Error::NativeSource { source, .. })
                if matches!(*source, acyclic_fs::LazyWorkspaceError::NotFound) =>
            {
                Ok(None)
            }
            Err(Error::NativeWorkspace { source, .. })
                if matches!(*source, WorkspaceError::NotFound) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

impl<'a> From<&'a LocalLazyWorkspace> for View<'a> {
    fn from(value: &'a LocalLazyWorkspace) -> Self {
        Self::Checkout(value)
    }
}

impl<'a> From<&'a LocalWorkspace> for View<'a> {
    fn from(value: &'a LocalWorkspace) -> Self {
        Self::Volume(value)
    }
}

#[cfg(test)]
mod tests;
