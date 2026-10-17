use super::*;

impl<A, O> crate::Generation<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Materializes this exact generation and binds its native capture owner.
    /// Cached mount options are reauthorized before any import or host copy.
    /// Raw preparation requires a selection that only the admission module mints:
    ///
    /// ```compile_fail
    /// use acyclic_fs::native_mount::customer::selection::AdmittedSelection;
    /// ```
    pub async fn prepare_native_working_set(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let work = mount.work(budget, cancellation)?;
        let selection =
            AdmittedSelection::admit(mount, &self.workspace(), self.id(), &work).await?;
        self.prepare_working_set(selection, options, budget, cancellation, None, &work)
            .await
    }

    /// Reattaches only a journaled original root without materializing over it.
    pub async fn attach_native_working_set(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        expected: NativeRootIdentity,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let work = mount.work(budget, cancellation)?;
        let selection =
            AdmittedSelection::admit(mount, &self.workspace(), self.id(), &work).await?;
        self.prepare_working_set(
            selection,
            options,
            budget,
            cancellation,
            Some(expected),
            &work,
        )
        .await
    }

    pub(super) async fn prepare_working_set(
        &self,
        selection: AdmittedSelection<'_>,
        options: &crate::MaterializeOptions,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        expected_root_identity: Option<NativeRootIdentity>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let mount = selection.options();
        if let Some(guard) = mount.callback_guard.as_ref() {
            guard.check(work)?.await?;
        }
        options::in_native_heap(work, || async move {
            if !options.destination.is_absolute() || mount.publication != MountPublication::Manual {
                return Err(MountLifecycleError::NativePolicy(
                    "native working set requires an absolute root and manual publication",
                ));
            }
            cancellation
                .check()
                .map_err(|_| MountSourceError::Cancelled)?;
            let workspace = self.workspace();
            let expected_generation = self.id();
            let mut checkout = working_checkout(
                &workspace,
                expected_generation,
                mount.writable,
                work,
                cancellation,
            )
            .await?;
            let (root, root_custody) = selection
                .prepare_root(
                    &mut checkout,
                    options,
                    expected_root_identity.is_none(),
                    &mut crate::kernel::KernelAccounting::native(work, cancellation),
                )
                .await?;
            let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
            let source_root = work.path(&options.destination, None)?;
            let mut custody = source_root.1;
            let source_root = source_root.0;
            let text = root
                .unicode_text_with_accounting(&mut accounting, cancellation)
                .map_err(WorkspaceError::from)?;
            let _text_custody = text.1;
            let text = text.0;
            let selected_root = work.path(Path::new(text.trim_start_matches('/')), None)?;
            let mut selected_custody = selected_root.1;
            let selected_root = selected_root.0;
            custody.absorb(&mut selected_custody)?;
            let source_identity = validate_native_working_set_root(
                &source_root,
                &selected_root,
                expected_root_identity,
                work,
            )?;
            let baseline = NativeViewBaseline::new(source_identity, !mount.writable);
            let source = selection.checkout_source(checkout, (root, root_custody), work)?;
            if expected_root_identity.is_some() {
                validate_recovered_capture(
                    &source,
                    &source_root,
                    source_identity,
                    baseline,
                    work.native_volume_work(capture_budget)?,
                    cancellation,
                )
                .await?;
            }
            if mount.writable
                && workspace.head(work, cancellation).await?.id() != expected_generation
            {
                return Err(WorkspaceError::StaleGeneration.into());
            }
            Ok(NativeWorkingSet {
                source,
                workspace,
                source_root,
                source_identity,
                selected_root,
                expected_generation,
                writable: mount.writable,
                _custody: custody,
            })
        })
        .await
    }
}

async fn working_checkout<A, O>(
    workspace: &Workspace<A, O>,
    expected_generation: GenerationId,
    writable: bool,
    work: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<crate::Checkout<A, O>, MountLifecycleError>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    if writable && workspace.head(work, cancellation).await?.id() != expected_generation {
        return Err(WorkspaceError::StaleGeneration.into());
    }
    let mode = if writable {
        CheckoutMode::tracking_transaction()
    } else {
        CheckoutMode::read_only_pinned()
    };
    let checkout = workspace
        .checkout(
            GenerationSelector::Exact(expected_generation),
            mode,
            work,
            cancellation,
        )
        .await?;
    Ok(if writable {
        checkout
    } else {
        checkout.into_unpublishable_capture_candidate()
    })
}

async fn validate_recovered_capture<A, O>(
    source: &CheckoutMountSource<A, O>,
    source_root: &Path,
    source_identity: NativeRootIdentity,
    baseline: NativeViewBaseline,
    capture: NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<(), MountLifecycleError>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    source
        .capture_host(
            source_root,
            &[MountPath::root()],
            source_identity,
            HostCaptureScope::Subtree,
            Some(baseline),
            &capture,
        )
        .await?;
    if super::super::adapter::await_lock(
        source.shared_checkout().lock(),
        &mut super::super::adapter::NativeDemandCarrier::Native(&capture),
        cancellation,
        Some(source.cancellation()),
    )
    .await?
    .has_pending_mutations()
    {
        return Err(MountLifecycleError::NativePolicy(
            "journaled native allocation diverged from its original generation",
        ));
    }
    Ok(())
}
