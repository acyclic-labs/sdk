use super::*;

impl<A, O> crate::Generation<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Materializes this exact generation and binds its native capture owner.
    pub async fn prepare_native_working_set(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        self.prepare_working_set(mount, options, budget, cancellation, None)
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
        self.prepare_working_set(mount, options, budget, cancellation, Some(expected))
            .await
    }

    pub(super) async fn prepare_working_set(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        expected_root_identity: Option<NativeRootIdentity>,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let original = mount.work(capture_budget, cancellation)?;
        let work = &original;
        super::super::adapter::validate_native_source_io_binding(
            work,
            mount.native_original.as_ref(),
        )?;
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
            if mount.writable
                && workspace.head(work, cancellation).await?.id() != expected_generation
            {
                return Err(WorkspaceError::StaleGeneration.into());
            }
            let mode = if mount.writable {
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
            let mut checkout = if mount.writable {
                checkout
            } else {
                checkout.into_unpublishable_capture_candidate()
            };
            let config = checkout.volume_config();
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(work, cancellation);
            let root = crate::workspace::customer_path_with_accounting(
                &mount.subdirectory,
                config,
                &mut accounting,
                cancellation,
            )?;
            let root_custody = root.1;
            let root = root.0;
            if checkout
                .lookup_no_follow(&root, work, cancellation)
                .await
                .map_err(WorkspaceError::from)?
                .record
                .is_none_or(|record| record.kind != FileKind::Directory)
            {
                return Err(WorkspaceError::NotDirectory.into());
            }
            if expected_root_identity.is_none() {
                let candidate = checkout
                    .private_candidate(work, cancellation)
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
            let source = options::checkout_source(checkout, (root, root_custody), mount, work)?;
            if expected_root_identity.is_some() {
                let capture = work.native_volume_work(capture_budget)?;
                source
                    .capture_host(
                        &source_root,
                        &[MountPath::root()],
                        source_identity,
                        HostCaptureScope::Subtree,
                        Some(baseline),
                        &capture,
                    )
                    .await?;
                if source
                    .shared_checkout()
                    .lock_with_carrier(
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
