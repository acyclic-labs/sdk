use super::*;

pub(super) fn checkout_source<A, O>(
    checkout: crate::Checkout<A, O>,
    root: (
        crate::kernel::NamespacePath,
        crate::kernel::allocation::KernelOwnedAllocation,
    ),
    publication: MountPublication,
    work: &NativeMaterializationWork,
    original: Option<&NativeMaterializationWork>,
    guard: Option<&Arc<dyn super::super::NativeMountCallbackGuard>>,
) -> Result<Arc<CheckoutMountSource<A, O>>, MountLifecycleError>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let config = checkout.volume_config();
    let shared = SharedCheckout::with_publication_meter(checkout, publication, work)?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) = root.1 else {
        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
    };
    let source =
        CheckoutMountSource::new_at_meter(shared, config, root.0, custody, work, original)?;
    let source = match guard {
        Some(guard) => source.with_native_callback_guard(guard.clone()),
        None => source,
    };
    source.into_arc_meter(work).map_err(Into::into)
}

/// When authored native mutations become a durable workspace generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MountPublication {
    /// Publish on native close/fsync/rename boundaries and explicit sync.
    #[default]
    CloseAndSync,
    /// Publish every independently admitted native mutation.
    PerMutation,
    /// Publish only through [`Mount::sync`] or orderly [`Mount::unmount`].
    Manual,
}

/// Explicit customer mount configuration.
#[derive(Clone)]
pub struct MountOptions {
    pub(crate) writable: bool,
    pub(crate) subdirectory: std::borrow::Cow<'static, str>,
    pub(crate) publication: MountPublication,
    pub(super) work: Option<NativeMaterializationWork>,
    pub(super) callback_guard: Option<Arc<dyn super::super::NativeMountCallbackGuard>>,
    pub(super) native_original: Option<NativeMaterializationWork>,
}

impl MountOptions {
    /// Creates a writable tracking-safe sparse mount with close/fsync publication.
    #[must_use]
    pub fn read_write() -> Self {
        Self {
            writable: true,
            subdirectory: std::borrow::Cow::Borrowed("/"),
            publication: MountPublication::CloseAndSync,
            work: None,
            callback_guard: None,
            native_original: None,
        }
    }

    /// Creates a pinned immutable mount.
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            writable: false,
            subdirectory: std::borrow::Cow::Borrowed("/"),
            publication: MountPublication::Manual,
            work: None,
            callback_guard: None,
            native_original: None,
        }
    }

    /// Projects one exact workspace directory as the native mount root.
    #[must_use]
    pub fn subdirectory(mut self, path: impl Into<String>) -> Self {
        self.subdirectory = std::borrow::Cow::Owned(path.into());
        self
    }

    /// Selects the exact publication policy for writable authored effects.
    #[must_use]
    pub const fn publication(mut self, publication: MountPublication) -> Self {
        self.publication = publication;
        self
    }

    /// Carries the original native allowance and authorization in mount admission.
    #[must_use]
    pub fn native_context(
        mut self,
        work: &NativeMaterializationWork,
        guard: Option<Arc<dyn super::super::NativeMountCallbackGuard>>,
        original: Option<&NativeMaterializationWork>,
    ) -> Self {
        self.work = Some(work.clone());
        self.callback_guard = guard;
        self.native_original = original.cloned();
        self
    }
}
