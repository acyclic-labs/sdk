use super::*;

/// One process-owned customer mount over a canonical workspace checkout.
pub struct Mount<A, O> {
    pub(crate) source: Arc<CheckoutMountSource<A, O>>,
    pub(crate) session: Mutex<Option<NativeMountSession>>,
    pub(crate) destination: PathBuf,
    pub(super) _destination_custody: NativeAllocation,
}

impl<A, O> Mount<A, O> {
    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }
}

impl<A, O> Mount<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Publishes all pending authored effects with one fenced generation.
    ///
    /// # Errors
    ///
    /// Returns a typed publication failure without discarding pending state.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async(PublicationPermit::Unrestricted)
            .await
            .map_err(MountLifecycleError::Source)?;
        Ok(())
    }

    /// Publishes all pending effects under one active operation lease.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async(permit)
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Publishes pending writes and advances the live checkout to workspace head.
    ///
    /// This keeps a direct-parent agent mount coherent after a child join
    /// without exposing remount orchestration to adapters.
    pub async fn refresh(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.advance_to_head().await
    }

    /// Advances an already-clean live checkout to the workspace head.
    ///
    /// Use this after an external, fenced workspace publication when the
    /// caller already synchronized the mount before that publication. Unlike
    /// [`Self::refresh`], this does not try to republish against the newer head.
    ///
    /// # Errors
    ///
    /// Returns a typed conflict or storage failure without changing the
    /// mounted generation.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        revalidate_session(&self.session)?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Publishes all pending effects on the source's dedicated callback runtime.
    ///
    /// # Errors
    ///
    /// Returns a typed publication failure without discarding pending state.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects and detaches the native namespace exactly once.
    ///
    /// # Errors
    ///
    /// Publication failure leaves the live mount owned by this handle. A detach
    /// failure retains driver state and its destination fence for drop retry.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this mount without publishing pending callbacks.
    ///
    /// This is the fail-closed recovery path after a fenced publication. The
    /// caller may remount from the last durable generation.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    ///
    /// # Errors
    ///
    /// Publication or detach failure retains the complete owner for retry.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        self.sync_blocking()?;
        self.detach()
    }

    pub(crate) fn detach(&self) -> Result<(), MountLifecycleError> {
        let mut owner = match self.session.lock() {
            Ok(owner) => owner,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(session) = owner.as_mut() else {
            return Ok(());
        };
        session.stop().map_err(MountLifecycleError::Native)?;
        owner.take();
        self.source.cancel();
        Ok(())
    }
}

impl<A, O> Workspace<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Mounts the complete workspace or one exact subtree as a native directory.
    ///
    /// Admission authenticates only the selected root frontier; it never scans
    /// the workspace or materializes file bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid or non-directory roots, unavailable host capabilities,
    /// busy/nonempty destinations, and storage/authentication failures.
    pub async fn mount(
        &self,
        destination: impl AsRef<Path>,
        options: MountOptions,
    ) -> Result<Mount<A, O>, MountLifecycleError> {
        let work = match options.work.as_ref() {
            Some(work) => work.clone(),
            None => {
                acyclic_native_runtime::initialize_native_io_services()
                    .map_err(NativeMountError::from)?;
                super::super::initialize_native_mount_services()?;
                NativeMaterializationWork::new(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .map_err(|(cause, _)| MountLifecycleError::Materialization(cause))?
            }
        };
        let cancellation = work.cancellation();
        if options
            .native_original
            .as_ref()
            .is_some_and(|binding| !work.shares_allocation_context(binding))
        {
            return Err(super::super::MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )
            .into());
        }
        if let Some(guard) = options.callback_guard.as_ref() {
            guard.check(&work)?.await?;
        }
        let destination = work.path(destination.as_ref(), None)?;
        let destination_custody = destination.1;
        let destination = destination.0;
        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let mut checkout = self
            .checkout(GenerationSelector::Head, mode, &work, cancellation)
            .await?;
        let config = checkout.volume_config();
        let root = crate::workspace::customer_path_with_accounting(
            &options.subdirectory,
            config,
            &mut crate::kernel::allocation::KernelAccounting::native(&work, cancellation),
            cancellation,
        )?;
        let root_custody = root.1;
        let root = root.0;
        let selected = checkout
            .lookup_no_follow(&root, &work, cancellation)
            .await
            .map_err(WorkspaceError::from)?
            .record
            .ok_or(WorkspaceError::NotFound)?;
        if selected.kind != FileKind::Directory {
            return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
        }
        let shared = SharedCheckout::with_publication_meter(checkout, options.publication, &work)?;
        let root_custody = match root_custody {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) => custody,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        };
        let source = CheckoutMountSource::new_at_meter(
            shared,
            config,
            root,
            root_custody,
            &work,
            options.native_original.as_ref(),
        )?;
        let source = match options.callback_guard {
            Some(guard) => source.with_native_callback_guard(guard),
            None => source,
        }
        .into_arc_meter(&work)?;
        let session_destination = work.path(&destination, None)?;
        let prepared =
            super::super::prepare_native_mount_start(&work).map_err(NativeMountError::from)?;
        let session = prepared.start(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id: self.id().volume_id(),
                destination: session_destination.0,
                writable: options.writable,
            },
            Arc::clone(&source) as Arc<dyn super::MountFilesystem>,
            &work,
            Some(session_destination.1),
        )?;
        Ok(Mount {
            source,
            session: Mutex::new(Some(session)),
            destination,
            _destination_custody: destination_custody,
        })
    }
}
