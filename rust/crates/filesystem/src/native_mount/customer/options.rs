use super::*;

pub(super) fn require_selection(
    options: &MountOptions,
    generation: GenerationId,
) -> Result<(), MountLifecycleError> {
    if options
        .require_access_view()?
        .current()
        .map_err(|_| MountLifecycleError::NativePolicy("access view is unavailable"))?
        .data_generation()
        .is_some_and(|selected| selected != generation)
    {
        return Err(MountLifecycleError::NativePolicy(
            "checkout differs from its authenticated data selection",
        ));
    }
    Ok(())
}

pub(super) fn require_disclosure(
    view: &crate::access::PathTrie,
) -> Result<(), MountLifecycleError> {
    let now =
        super::super::adapter::access::access_time(&crate::native_mount::clock::Clock::capture())?;
    if !view.allows_every_path(
        &[crate::access::Action::List, crate::access::Action::Read],
        now,
    ) {
        return Err(MountLifecycleError::NativePolicy(
            "materialized working sets require unrestricted disclosure",
        ));
    }
    Ok(())
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
    pub(crate) access_view: Option<Arc<crate::access::AccessView>>,
    pub(crate) access_refresh: Option<Arc<dyn super::super::NativeAccessOwner>>,
    pub(crate) writable: bool,
    pub(crate) subdirectory: std::borrow::Cow<'static, str>,
    pub(crate) publication: MountPublication,
    pub(super) work: Option<NativeMaterializationWork>,
    pub(super) callback_guard: Option<Arc<dyn super::super::NativeMountCallbackGuard>>,
    pub(super) native_original: Option<NativeMaterializationWork>,
}

impl MountOptions {
    pub(super) fn data_selector(
        &self,
    ) -> Result<crate::model::GenerationSelector, MountLifecycleError> {
        let view = self
            .require_access_view()?
            .current()
            .map_err(|_| MountLifecycleError::NativePolicy("access view is unavailable"))?;
        Ok(if self.writable {
            crate::model::GenerationSelector::Head
        } else {
            view.data_generation().map_or(
                crate::model::GenerationSelector::Head,
                crate::model::GenerationSelector::Exact,
            )
        })
    }

    /// Bound policy/capability owner for preparation and inspection. Native
    /// operations additionally fence this view against current persisted policy.
    pub fn require_access_view(
        &self,
    ) -> Result<&Arc<crate::access::AccessView>, MountLifecycleError> {
        self.access_view
            .as_ref()
            .ok_or(MountLifecycleError::NativePolicy(
                "native mounts require an authenticated policy and capability view",
            ))
    }

    pub(super) fn work(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<NativeMaterializationWork, MountLifecycleError> {
        let work = if let Some(work) = &self.work {
            work.clone()
        } else {
            acyclic_native_runtime::initialize_native_io_services()
                .map_err(NativeMountError::from)?;
            super::super::initialize_native_mount_services()?;
            NativeMaterializationWork::new(budget, cancellation)
                .map_err(|(cause, _)| MountLifecycleError::Materialization(cause))?
        };
        if self
            .native_original
            .as_ref()
            .is_some_and(|binding| !work.shares_allocation_context(binding))
        {
            return Err(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )
            .into());
        }
        Ok(work)
    }

    /// Creates a writable tracking-safe sparse mount with close/fsync publication.
    #[must_use]
    pub fn read_write() -> Self {
        Self {
            access_view: None,
            access_refresh: None,
            writable: true,
            subdirectory: std::borrow::Cow::Borrowed("/"),
            publication: MountPublication::CloseAndSync,
            work: None,
            callback_guard: None,
            native_original: None,
        }
    }

    /// Bind policy intersected with the authenticated caller's capability.
    /// Mount admission rejects options without this binding.
    #[must_use]
    pub fn access_view(mut self, view: Arc<crate::access::AccessView>) -> Self {
        self.access_view = Some(view);
        self
    }

    /// Reload the authenticated view when the persisted policy fence changes.
    #[must_use]
    pub fn access_refresh(mut self, refresh: Arc<dyn super::super::NativeAccessOwner>) -> Self {
        self.access_refresh = Some(refresh);
        self
    }

    /// Narrow the projection to read-only while retaining its authenticated access owner.
    #[must_use]
    pub fn restrict_to_read_only(mut self) -> Self {
        self.writable = false;
        self
    }

    /// Creates a pinned immutable mount.
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            writable: false,
            publication: MountPublication::Manual,
            ..Self::read_write()
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

pub(super) async fn in_native_heap<T, F>(
    work: &NativeMaterializationWork,
    create: impl FnOnce() -> F,
) -> Result<T, MountLifecycleError>
where
    F: Future<Output = Result<T, MountLifecycleError>>,
{
    let mut custody = None;
    let (mut future, _) = crate::heap_future::in_heap_admitted(
        &mut custody,
        |custody, bytes| {
            **custody =
                Some(work.claim(bytes, u64::from(bytes != 0), crate::WorkCounters::default())?);
            Ok::<(), crate::NativeTreeMaterializationError>(())
        },
        |_| create(),
    )?;
    let result = future.as_mut().await;
    drop(future);
    drop(custody);
    result
}
