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
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        with_session_work(&self.session, NativeMountSession::flush_callbacks)?;
        self.source
            .sync_async(PublicationPermit::Unrestricted)
            .await
            .map_err(MountLifecycleError::Source)?;
        Ok(())
    }

    /// Advances an already-clean live checkout to the workspace head.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        with_session_work(&self.session, NativeMountSession::revalidate)?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Publishes all pending effects on the source's dedicated callback runtime.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        with_session_work(&self.session, NativeMountSession::flush_callbacks)?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects and detaches the native namespace exactly once.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this mount without publishing pending callbacks.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
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
    pub async fn mount(
        &self,
        destination: impl AsRef<Path>,
        options: MountOptions,
    ) -> Result<Mount<A, O>, MountLifecycleError> {
        let work = options.work(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?;
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
        let source = options::checkout_source(checkout, (root, root_custody), &options, &work)?;
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
