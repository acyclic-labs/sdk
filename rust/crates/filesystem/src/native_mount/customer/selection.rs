use super::*;
use crate::access::{PathTrie, projection};
mod root;
pub(super) use root::AdmittedRoot;

// Only this module can construct a selection. The lease is acquired BEFORE
// calling its verified owner, and retained through import, copying and binding.
// It uses the publication CAS's existing terminal registry, never another lock.
pub(super) struct AdmittedSelection<'a> {
    options: &'a MountOptions,
    binding: MountOptions,
    view: Arc<PathTrie>,
    volume: VolumeId,
    config: crate::VolumeConfig,
    generation: GenerationId,
    _policy: projection::Lease,
}

impl<'a> AdmittedSelection<'a> {
    pub(super) async fn admit<A, O>(
        options: &'a MountOptions,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        work: &NativeMaterializationWork,
    ) -> Result<Self, MountLifecycleError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        Self::bind(options, workspace, generation, work, true).await
    }

    async fn bind<A, O>(
        options: &'a MountOptions,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        work: &NativeMaterializationWork,
        materialize: bool,
    ) -> Result<Self, MountLifecycleError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if options
            .native_original
            .as_ref()
            .is_some_and(|binding| !work.shares_allocation_context(binding))
        {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
        }
        let previous = options
            .require_access_view()?
            .current()
            .map_err(|_| MountLifecycleError::NativePolicy("access view is unavailable"))?;
        require_volume(
            &previous,
            workspace.id().volume_id(),
            workspace.volume_config(),
        )?;
        options::require_selection(options, generation)?;
        if !workspace.volume.fs.has_process_policy_owner() {
            return Err(MountLifecycleError::NativePolicy(
                "materialization requires a process-owned policy authority",
            ));
        }
        let policy =
            projection::enter(workspace.id().volume_id()).map_err(MountSourceError::from)?;
        let (binding, view) = fresh(options, workspace, generation, work, materialize).await?;
        Ok(Self {
            options,
            binding,
            view,
            volume: workspace.id().volume_id(),
            config: workspace.volume_config(),
            generation,
            _policy: policy,
        })
    }

    pub(super) fn options(&self) -> &'a MountOptions {
        self.options
    }

    pub(super) async fn exactify<A, O, D, S>(
        &mut self,
        lazy: &LazyWorkspace<A, O, D, S>,
        permit: PublicationPermit,
        work: &NativeMaterializationWork,
    ) -> Result<crate::Generation<A, O>, MountLifecycleError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        require_volume(
            &self.view,
            lazy.workspace().id().volume_id(),
            lazy.workspace().volume_config(),
        )?;
        let exact = lazy
            .exactify(
                &self.options.subdirectory,
                permit,
                &mut KernelAccounting::native(work, work.cancellation()),
            )
            .await?;
        // Import may advance data while the lease forbids policy changes. Check
        // the new data generation with the SAME verified capability ceiling.
        let (binding, view) = fresh(self.options, lazy.workspace(), exact.id(), work, true).await?;
        self.binding = binding;
        self.view = view;
        self.generation = exact.id();
        Ok(exact)
    }

    pub(super) fn checkout_source<A, O>(
        &self,
        checkout: crate::Checkout<A, O>,
        root: (
            crate::kernel::NamespacePath,
            crate::kernel::KernelOwnedAllocation,
        ),
        work: &NativeMaterializationWork,
    ) -> Result<Arc<CheckoutMountSource<A, O>>, MountLifecycleError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.require_checkout(&checkout)?;
        let config = checkout.volume_config();
        let access = self.binding.require_access_view()?;
        options::require_selection(&self.binding, checkout.generation_id())?;
        let shared =
            SharedCheckout::with_publication_meter(checkout, self.options.publication, work)?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) = root.1
        else {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
        };
        let source = CheckoutMountSource::new_at_meter(
            shared,
            config,
            root.0,
            custody,
            work,
            self.options.native_original.as_ref(),
        )?
        .with_access_view(Arc::clone(access))?;
        let source = source.with_access_refresh(self.binding.access_refresh.clone());
        let source = match self.options.callback_guard.as_ref() {
            Some(guard) => source.with_native_callback_guard(guard.clone()),
            None => source,
        };
        source.into_arc_meter(work).map_err(Into::into)
    }

    fn require_checkout<A, O>(
        &self,
        checkout: &crate::Checkout<A, O>,
    ) -> Result<(), MountLifecycleError> {
        if checkout.volume_id() != self.volume
            || checkout.volume_config() != self.config
            || checkout.generation_id() != self.generation
        {
            return Err(MountLifecycleError::NativePolicy(
                "checkout differs from its admitted selection",
            ));
        }
        Ok(())
    }

    pub(super) async fn prepare_root<A, O>(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        options: &crate::MaterializeOptions,
        materialize: bool,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        (
            crate::kernel::NamespacePath,
            crate::kernel::KernelOwnedAllocation,
        ),
        MountLifecycleError,
    >
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.require_checkout(checkout)?;
        let KernelAccounting::NativeOriginal { work, cancellation } = &*accounting else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (work, cancellation) = (*work, *cancellation);
        let (root, custody) = crate::workspace::customer_path_with_accounting(
            &self.options.subdirectory,
            self.config,
            accounting,
            cancellation,
        )?;
        if checkout
            .lookup_no_follow(&root, work, cancellation)
            .await
            .map_err(WorkspaceError::from)?
            .record
            .is_none_or(|record| record.kind != FileKind::Directory)
        {
            return Err(WorkspaceError::NotDirectory.into());
        }
        if materialize {
            let candidate = checkout
                .private_candidate(accounting, cancellation)
                .map_err(WorkspaceError::from)?;
            let reader = candidate
                .into_pinned_reader_with_native_work(work, cancellation)
                .map_err(WorkspaceError::from)?;
            super::super::materialize_reader_paths(
                &reader,
                std::slice::from_ref(&root),
                options,
                super::super::MaterializeMode::ReconstructibleView,
                work,
            )
            .await?;
        }
        Ok((root, custody))
    }
}

fn require_volume(
    view: &PathTrie,
    volume: VolumeId,
    config: crate::VolumeConfig,
) -> Result<(), MountLifecycleError> {
    if view.config() != config
        || uuid::Uuid::parse_str(view.volume())
            .map(|id| id.into_bytes())
            .ok()
            != Some(volume.into_bytes())
    {
        return Err(MountLifecycleError::NativePolicy(
            "selection differs from its authenticated volume or configuration",
        ));
    }
    Ok(())
}

async fn fresh<A, O>(
    options: &MountOptions,
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    work: &NativeMaterializationWork,
    materialize: bool,
) -> Result<(MountOptions, Arc<PathTrie>), MountLifecycleError>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let owner = options
        .access_refresh
        .as_ref()
        .ok_or(MountLifecycleError::NativePolicy(
            "materialization requires the authenticated selection owner",
        ))?;
    let binding = owner.select(generation, work)?.await?;
    let view = binding
        .require_access_view()?
        .current()
        .map_err(|_| MountLifecycleError::NativePolicy("access view is unavailable"))?;
    let previous = options
        .require_access_view()?
        .current()
        .map_err(|_| MountLifecycleError::NativePolicy("access view is unavailable"))?;
    require_volume(&view, workspace.id().volume_id(), workspace.volume_config())?;
    if !previous.same_authority(&view) {
        return Err(MountLifecycleError::NativePolicy(
            "selection changed its authenticated authority",
        ));
    }
    options::require_selection(&binding, generation)?;
    if materialize {
        options::require_disclosure(&view)?;
    }
    if materialize && view.valid_until().is_some() {
        return Err(MountLifecycleError::NativePolicy(
            "materialization cannot fence expiring disclosure",
        ));
    }
    let revision = workspace
        .current_access_policy_revision(work, work.cancellation())
        .await
        .map_err(MountSourceError::from)?;
    if !revision.is_some_and(|(head, revision)| {
        view.matches_policy_revision(head, revision)
            && (view.data_generation().is_some()
                || view.matches_policy_revision(generation, revision))
    }) {
        return Err(MountLifecycleError::NativePolicy(
            "selection differs from current persisted policy",
        ));
    }
    Ok((binding, view))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_mount::{NativeAccessOwner, NativeMountCallbackFuture};
    type MemoryWorkspace =
        Workspace<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;
    type MemoryCheckout =
        crate::Checkout<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;
    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    struct DuringAuthorization {
        workspace: MemoryWorkspace,
        inner: Arc<dyn NativeAccessOwner>,
    }

    impl NativeAccessOwner for DuringAuthorization {
        fn check<'a>(
            &'a self,
            work: &'a NativeMaterializationWork,
        ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError> {
            self.inner.check(work)
        }

        fn select<'a>(
            &'a self,
            generation: GenerationId,
            work: &'a NativeMaterializationWork,
        ) -> Result<NativeMountCallbackFuture<'a, MountOptions>, NativeMountError> {
            let admission = self.inner.select(generation, work)?;
            NativeMountCallbackFuture::new(work, || async move {
                let binding = admission.await?;
                // Publish after the owner has read policy but BEFORE it returns
                // authorization. This exercises the actual admission ordering.
                let mut edit = pending_policy(&self.workspace, work, crate::access::INITIAL_POLICY)
                    .await
                    .expect("policy edit fixture");
                assert_refused(publish(&mut edit, work).await);
                Ok(binding)
            })
        }
    }

    pub(super) async fn pending_policy(
        workspace: &MemoryWorkspace,
        work: &NativeMaterializationWork,
        policy: &[u8],
    ) -> TestResult<MemoryCheckout> {
        let mut edit = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                work,
                work.cancellation(),
            )
            .await?;
        edit.write_access_policy(
            policy,
            crate::NamedAttributeWriteMode::Upsert,
            work,
            work.cancellation(),
        )
        .await?;
        Ok(edit)
    }

    pub(super) async fn publish(
        edit: &mut MemoryCheckout,
        work: &NativeMaterializationWork,
    ) -> Result<(), crate::FsError> {
        edit.commit(
            crate::OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            work.cancellation(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .map(|_| ())
        .map_err(|failure| failure.error)
    }

    fn assert_refused(result: Result<(), crate::FsError>) {
        assert!(
            matches!(
                result,
                Err(crate::FsError::Publication(
                    crate::kernel::PublicationError::ActiveProjection
                ))
            ),
            "policy publication escaped materialization admission"
        );
    }

    async fn publication(omit_lease: bool, late_admission: bool) -> TestResult {
        acyclic_native_runtime::initialize_native_io_services()?;
        crate::initialize_native_mount_services()?;
        let cancel = crate::CancellationToken::new();
        let work = super::super::tests::test_work(crate::WorkBudget::UNBOUNDED, &cancel)?;
        let fs = crate::Fs::memory();
        let name = if late_admission {
            "selection-late"
        } else if omit_lease {
            "selection-unfenced"
        } else {
            "selection-fenced"
        };
        let workspace = fs
            .create_workspace(name, crate::WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value;
        let mount =
            super::super::tests::fixture_options(&workspace, MountOptions::read_only()).await?;
        let owner = Arc::new(DuringAuthorization {
            workspace: workspace.clone(),
            inner: mount.access_refresh.clone().expect("fixture owner"),
        });
        let mount = mount.access_refresh(owner.clone());
        let generation = workspace.head(&work, &cancel).await?;
        if late_admission {
            owner.select(generation.id(), &work)?.await?;
            panic!("late admission did not exercise its policy publication check");
        }
        let selection =
            AdmittedSelection::admit(&mount, &workspace, generation.id(), &work).await?;
        let selection = if omit_lease {
            drop(selection);
            None
        } else {
            Some(selection)
        };
        let mut edit = pending_policy(&workspace, &work, crate::access::INITIAL_POLICY).await?;
        assert_refused(publish(&mut edit, &work).await);
        drop(selection);
        publish(&mut edit, &work).await?;
        Ok(())
    }

    #[tokio::test]
    async fn admitted_materialization_fences_authorization_and_copy_until_retirement() -> TestResult
    {
        publication(false, false).await
    }

    #[tokio::test]
    #[should_panic(expected = "policy publication escaped materialization admission")]
    async fn early_materialization_retirement_negative_control() {
        publication(true, false).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "policy publication escaped materialization admission")]
    async fn authorizing_before_admission_negative_control() {
        publication(false, true).await.unwrap();
    }
}
