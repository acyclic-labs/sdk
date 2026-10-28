//! Git operations over either a source-backed checkout or an SDK volume.
use super::*;

/// The task-selected filesystem view, borrowed for one facade operation.
pub enum View<'a, P: ?Sized + 'static = DefaultProvider, O: 'static = DefaultObjects> {
    /// A normal checkout whose unchanged files remain demand-backed.
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    Checkout(&'a ManagedLazyWorkspace<P, O>),
    /// An SDK volume, including a child's copy-on-write fork.
    Volume(&'a ManagedWorkspace<P, O>),
}

/// A branch selected by the facade and retained by its task binding.
pub enum OwnedView<P: ?Sized + 'static = DefaultProvider, O: 'static = DefaultObjects> {
    /// A branch retaining its original native source capability.
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    Checkout(ManagedLazyWorkspace<P, O>),
    /// A branch of an SDK volume.
    Volume(ManagedWorkspace<P, O>),
}

impl<P: ?Sized + 'static, O: 'static> OwnedView<P, O> {
    /// Retains a source-backed branch for an ordinary checkout binding.
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    pub fn into_checkout(self) -> crate::Result<ManagedLazyWorkspace<P, O>> {
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
            Self::Checkout(view) => Ok(view),
            Self::Volume(_) => Err(Error::Unauthorized(
                "expected a source-backed checkout branch".into(),
            )),
        }
    }

    /// Borrows the selected view without copying its source or contents.
    pub const fn view(&self) -> View<'_, P, O> {
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
            Self::Checkout(view) => View::Checkout(view),
            Self::Volume(view) => View::Volume(view),
        }
    }

    /// The authored workspace supplying this view's generations.
    pub const fn workspace(&self) -> &ManagedWorkspace<P, O>
    where
        P: acyclic_fs::distributed::AuthorityProvider,
        O: acyclic_fs::AsyncObjectStore,
    {
        self.view().workspace()
    }
}

impl<
    'a,
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
> View<'a, P, O>
{
    /// The authored workspace supplying this view's generations.
    pub const fn workspace(self) -> &'a ManagedWorkspace<P, O> {
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
            Self::Checkout(view) => view.workspace(),
            Self::Volume(view) => view,
        }
    }
}

impl<P: ?Sized + 'static, O: 'static> Copy for View<'_, P, O> {}
impl<P: ?Sized + 'static, O: 'static> Clone for View<'_, P, O> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
> View<'_, P, O>
{
    pub(super) async fn read(
        self,
        path: &str,
        maximum: u64,
    ) -> crate::Result<acyclic_stream::OwnedBytes> {
        let cancellation = CancellationToken::new();
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
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
        _exact: bool,
        _permit: PublicationPermit,
    ) -> crate::Result<GitTreeRef> {
        let cancellation = CancellationToken::new();
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
            Self::Checkout(view) if _exact => {
                let generation = view
                    .exactify(
                        "/",
                        _permit,
                        &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
                    )
                    .await?;
                Ok(GitTreeRef::exact(view.workspace().id(), generation.id()))
            }
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
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
    ) -> crate::Result<OwnedView<P, O>> {
        let cancellation = CancellationToken::new();
        let key = IdempotencyKey::from_bytes(operation.into_bytes());
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
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

    pub(super) async fn related(
        self,
        workspace: ManagedWorkspace<P, O>,
    ) -> crate::Result<OwnedView<P, O>> {
        match self {
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
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
            #[cfg(not(target_arch = "wasm32"))]
            Err(Error::NativeSource { source, .. })
                if matches!(*source, acyclic_fs::LazyWorkspaceError::NotFound) =>
            {
                Ok(None)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Err(Error::NativeWorkspace { source, .. })
                if matches!(*source, WorkspaceError::NotFound) =>
            {
                Ok(None)
            }
            Err(Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
impl<'a, P: ?Sized + 'static, O: 'static> From<&'a ManagedLazyWorkspace<P, O>> for View<'a, P, O> {
    fn from(value: &'a ManagedLazyWorkspace<P, O>) -> Self {
        Self::Checkout(value)
    }
}

impl<'a, P: ?Sized + 'static, O: 'static> From<&'a ManagedWorkspace<P, O>> for View<'a, P, O> {
    fn from(value: &'a ManagedWorkspace<P, O>) -> Self {
        Self::Volume(value)
    }
}

#[cfg(test)]
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod tests;
