use super::*;

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Exactifies only the selected subtree and prepares its authenticated view.
    pub async fn prepare_native_working_set(
        &self,
        mount: MountOptions,
        options: &crate::MaterializeOptions,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        permit: PublicationPermit,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        mount.require_access_view()?;
        if mount.publication != MountPublication::Manual {
            return Err(MountLifecycleError::NativePolicy(
                "prepared native working sets require manual publication",
            ));
        }
        let original = mount.work(capture_budget, cancellation)?;
        let work = &original;
        if !options.destination.is_absolute() {
            return Err(MountLifecycleError::NativePolicy(
                "native working-set root must be absolute",
            ));
        }
        if let Some(guard) = &mount.callback_guard {
            guard.check(work)?.await?;
        }
        let generation = self.workspace().head(work, cancellation).await?.id();
        let mut selection =
            AdmittedSelection::admit(&mount, self.workspace(), generation, work).await?;
        options::in_native_heap(work, || async move {
            let exact = selection.exactify(self, permit, work).await?;
            exact
                .prepare_working_set(selection, options, capture_budget, cancellation, None, work)
                .await
        })
        .await
    }

    /// Mounts the actual sparse source without scanning unresolved descendants.
    pub async fn mount(
        &self,
        destination: impl AsRef<Path>,
        options: MountOptions,
    ) -> Result<LazyMount<A, O, D, S>, MountLifecycleError> {
        let work = options.work(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?;
        let work = &work;
        let destination = work.path(destination.as_ref(), None)?;
        let destination_custody = destination.1;
        let destination = destination.0;
        let options_bytes = match &options.subdirectory {
            std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(text) => text.capacity() as u64,
        };
        let _options_custody = work.claim(options_bytes, 0, crate::WorkCounters::default())?;
        if options.writable && options.publication == MountPublication::PerMutation {
            return Err(MountSourceError::NativeUnsupported.into());
        }
        let (authored, volume_id, root_text) = AdmittedRoot::admit(
            &options,
            self.workspace(),
            self.workspace().head(work, work.cancellation()).await?.id(),
            work,
        )
        .await?
        .lazy_source(self, work)
        .await?;
        let root_custody = match root_text.1 {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) => custody,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        };
        let root_text = root_text.0;
        let session_destination = work.path(&destination, None)?;
        let lazy = self.share(&mut KernelAccounting::native(work, work.cancellation()))?;
        let mut source_allocation = work.claim(
            std::mem::size_of::<LazyMountSource<A, O, D, S>>() as u64,
            1,
            crate::WorkCounters::default(),
        )?;
        let prepared =
            super::super::prepare_native_mount_start(work).map_err(NativeMountError::from)?;
        let request = NativeMountRequest {
            mount_id: MountId::new(),
            volume_id,
            destination: session_destination.0,
            writable: options.writable,
        };
        let source = Arc::new(
            match LazyMountSource::new(
                lazy,
                authored,
                root_text,
                super::super::adapter::NativeDemandCarrier::Native(work),
                Some((root_custody, &mut source_allocation)),
            ) {
                Ok(source) => source,
                Err(error) => {
                    return Err(prepared
                        .retain_source_startup_failure(request, session_destination.1, error)
                        .into());
                }
            },
        );
        let session = prepared.start(
            request,
            source.clone() as Arc<dyn MountFilesystem>,
            work,
            Some(session_destination.1),
        )?;
        Ok(LazyMount {
            source,
            session: Mutex::new(Some(session)),
            destination,
            _destination_custody: destination_custody,
        })
    }
}
