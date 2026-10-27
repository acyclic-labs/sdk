use super::super::session::NativeSessionControl;
use super::*;

/// One process-owned customer mount over a canonical workspace checkout.
pub struct Mount<A: 'static, O: 'static> {
    pub(crate) source: Arc<CheckoutMountSource<A, O>>,
    pub(crate) session: Mutex<Option<NativeMountSession>>,
    pub(crate) destination: PathBuf,
    pub(super) _destination_custody: NativeAllocation,
    pub(super) retiring: std::sync::atomic::AtomicBool,
}

impl<A, O> Mount<A, O> {
    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }

    /// Retirement has started; the existing cleanup owner must finish before reuse.
    #[must_use]
    pub fn is_retiring(&self) -> bool {
        self.retiring.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl<A, O> Mount<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Publishes all pending authored effects with one fenced generation.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        #[cfg(all(test, not(loom)))]
        super::command_tests::record("sync");
        with_session_work_async(
            &self.session,
            self.source.as_ref(),
            NativeSessionControl::FlushCallbacks,
        )
        .await?;
        self.source
            .sync_async(PublicationPermit::Unrestricted)
            .await
            .map_err(MountLifecycleError::Source)?;
        Ok(())
    }

    /// Advances an already-clean live checkout to the workspace head.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        #[cfg(all(test, not(loom)))]
        super::command_tests::record("advance");
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        with_session_work_async(
            &self.session,
            self.source.as_ref(),
            NativeSessionControl::Revalidate,
        )
        .await?;
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
        if self.is_retiring() {
            return self.detach();
        }
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
        if self.is_retiring() {
            return self.detach();
        }
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this mount without publishing pending callbacks.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Retires a command projection without publishing unverified authored effects.
    pub fn abandon_command(&self) -> Result<(), MountLifecycleError> {
        if !self.is_retiring() {
            self.prepare_command_cleanup()?;
        }
        self.detach()
    }

    fn prepare_command_cleanup(&self) -> Result<(), MountLifecycleError> {
        let work = self
            .source
            .native_original_work()?
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        let mut owner = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session) = owner.as_mut() {
            session.prepare_destination_cleanup(&work)?;
        }
        Ok(())
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        if self.is_retiring() {
            return self.detach();
        }
        self.sync_blocking()?;
        self.detach()
    }

    pub(crate) fn detach(&self) -> Result<(), MountLifecycleError> {
        #[cfg(all(test, not(loom)))]
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
        self.retiring
            .store(true, std::sync::atomic::Ordering::Release);
        session.stop_with_context(&cleanup)?;
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
    /// Admits the canonical checkout source without mounting a host directory.
    /// The signed selection owner and original callback guard in `options`
    /// remain attached to the source used by any packet transport.
    pub async fn native_source(
        &self,
        options: &MountOptions,
    ) -> Result<Arc<CheckoutMountSource<A, O>>, MountLifecycleError> {
        let work = options.require_native_work()?;
        if let Some(guard) = options.callback_guard.as_ref() {
            guard.check(work)?.await?;
        }
        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let checkout = self
            .checkout(options.data_selector()?, mode, work, work.cancellation())
            .await?;
        AdmittedRoot::admit(options, self, checkout.generation_id(), work)
            .await?
            .checkout_source(checkout, work)
            .await
    }

    /// Mounts the workspace as a task view: the one public way to mount a
    /// per-agent view. On Linux and Windows the view is a namespace of the
    /// process's one shared native instance, at a fresh path that no other
    /// view of the process has had or will have, so it costs no mount,
    /// thread or descriptor of its own; [`Mount::path`] names it. macOS uses
    /// the existing native mount backend until it supports shared instances.
    pub async fn mount_view(
        &self,
        options: MountOptions,
    ) -> Result<Mount<A, O>, MountLifecycleError> {
        let destination = super::super::shared_view_destination(options.view_root.as_deref())?;
        self.mount(destination.path(), options.for_task_view())
            .await
    }

    /// Mounts the complete workspace or one exact subtree as a native
    /// directory at `destination`, as an instance of its own: for a single
    /// mount such as a user's checkout. A task view is mounted with
    /// [`Self::mount_view`] instead. Root lookup is owned by private
    /// admission, never an unchecked path:
    ///
    /// ```compile_fail
    /// use acyclic_fs::native_mount::customer::selection::root::AdmittedRoot;
    /// ```
    pub async fn mount(
        &self,
        destination: impl AsRef<Path>,
        options: MountOptions,
    ) -> Result<Mount<A, O>, MountLifecycleError> {
        let _startup = super::super::admit_mount_startup().await?;
        let work = options.work(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?;
        let destination = work.path(destination.as_ref(), None)?;
        let destination_custody = destination.1;
        let destination = destination.0;
        let guard = options.callback_guard.clone();
        let original = options.native_original.clone();
        let options = options.native_context(&work, guard, original.as_ref());
        let source = self.native_source(&options).await?;
        let session_destination = work.path(&destination, None)?;
        let prepared =
            super::super::prepare_native_mount_start(&work).map_err(NativeMountError::from)?;
        let session = prepared.start(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id: self.id().volume_id(),
                destination: session_destination.0,
                writable: options.writable,
                shared: options.shared,
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
            retiring: std::sync::atomic::AtomicBool::new(false),
        })
    }
}
