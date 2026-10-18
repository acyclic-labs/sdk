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
        #[cfg(all(test, not(acyclic_fs_native_loom)))]
        super::command_tests::record("sync");
        with_session_work(
            &self.session,
            self.source.as_ref(),
            NativeMountSession::flush_callbacks,
        )?;
        self.source
            .sync_async(PublicationPermit::Unrestricted)
            .await
            .map_err(MountLifecycleError::Source)?;
        Ok(())
    }

    /// Advances an already-clean live checkout to the workspace head.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        #[cfg(all(test, not(acyclic_fs_native_loom)))]
        super::command_tests::record("advance");
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        with_session_work(
            &self.session,
            self.source.as_ref(),
            NativeMountSession::revalidate,
        )?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Publishes all pending effects on the source's dedicated callback runtime.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        with_session_work(
            &self.session,
            self.source.as_ref(),
            NativeMountSession::flush_callbacks,
        )?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes one verified native command and optionally retires its projection.
    /// The filesystem owns both destination cleanup and namespace-fence release.
    pub async fn complete_command(&self, retire: bool) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.advance_to_head().await?;
        if retire {
            let work = self
                .source
                .native_original_work()?
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
            {
                let mut owner = self
                    .session
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(session) = owner.as_mut() {
                    session.prepare_destination_cleanup(&work)?;
                }
            }
            self.detach()?;
        }
        Ok(())
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
        #[cfg(all(test, not(acyclic_fs_native_loom)))]
        super::command_tests::record("detach");
        let mut owner = match self.session.lock() {
            Ok(owner) => owner,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(session) = owner.as_mut() else {
            return Ok(());
        };
        let wait = crate::CancellationToken::new();
        let cleanup = super::super::session::NativeSessionStopWork::AcceptedWait(&wait);
        #[cfg(windows)]
        session.prepare_projection_cleanup()?;
        session.stop_with_context(&cleanup)?;
        #[cfg(windows)]
        if let Some((cause, _)) = session
            .namespace_outcome
            .as_mut()
            .and_then(super::super::projfs::ProjFsNamespaceOutcome::take_capture_failures)
        {
            return Err(cause.into());
        }
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
