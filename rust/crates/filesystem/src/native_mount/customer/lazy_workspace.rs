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
        subdirectory: &str,
        options: &crate::MaterializeOptions,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        permit: PublicationPermit,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let original = NativeMaterializationWork::new(capture_budget, cancellation)
            .map_err(|(cause, _)| MountLifecycleError::Materialization(cause))?;
        let work = &original;
        if !options.destination.is_absolute() {
            return Err(MountLifecycleError::NativePolicy(
                "native working-set root must be absolute",
            ));
        }
        let mut frame_custody = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (work, &mut frame_custody),
            |(work, custody), bytes| {
                **custody = Some(work.claim(
                    bytes,
                    u64::from(bytes != 0),
                    crate::WorkCounters::default(),
                )?);
                Ok::<(), crate::NativeTreeMaterializationError>(())
            },
            |(work, _)| async move {
                let exact = self
                    .exactify(
                        subdirectory,
                        permit,
                        &LazyTreapWork::native_with_work(work, work.cancellation())?,
                    )
                    .await?;
                let mut mount = (
                    MountOptions::read_write().publication(MountPublication::Manual),
                    work.claim(0, 0, crate::WorkCounters::default())?,
                );
                if subdirectory != "/" {
                    let (text, mut custody) = work.display(&subdirectory)?;
                    mount.0 = mount.0.subdirectory(text);
                    mount.1.absorb(&mut custody)?;
                }
                mount.0 = mount.0.native_context(work, None, None);
                exact
                    .prepare_native_working_set(&mount.0, options, capture_budget, cancellation)
                    .await
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_custody);
        result
    }

    pub(super) async fn prepare_authored_mount_source(
        &self,
        options: &MountOptions,
        callback_guard: Option<&Arc<dyn super::super::NativeMountCallbackGuard>>,
        work: &NativeMaterializationWork,
        native_original: Option<&NativeMaterializationWork>,
    ) -> Result<
        (
            Arc<CheckoutMountSource<A, O>>,
            VolumeId,
            (String, crate::kernel::allocation::KernelOwnedAllocation),
        ),
        MountLifecycleError,
    > {
        super::super::adapter::validate_native_source_io_binding(work, native_original)?;
        let cancellation = work.cancellation();
        let config = self.workspace().volume_config();
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(work, cancellation);
        let root = crate::workspace::customer_path_with_accounting(
            &options.subdirectory,
            config,
            &mut accounting,
            cancellation,
        )?;
        let root_text = root
            .0
            .unicode_text_with_accounting(&mut accounting, cancellation)
            .map_err(WorkspaceError::from)?;
        let selected = self
            .lookup_with_native_work(&root_text.0, work, cancellation)
            .await?;
        let selected_custody = selected.1;
        let selected = selected.0;
        if !matches!(
            &selected,
            LazyLookup::Authored {
                stat: crate::WorkspaceStat {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            } | LazyLookup::Source(SourceNode {
                kind: SourceNodeKind::Directory,
                ..
            }) | LazyLookup::Shadow {
                record: crate::kernel::FileRecord {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            }
        ) {
            return Err(WorkspaceError::NotDirectory.into());
        }
        if let LazyLookup::Source(node) = &selected {
            let expected_source = self.source_file_id(&node.file_identity, &mut accounting)?;
            work.charge(crate::WorkCounters {
                bytes_hashed: (b"acyclic-fs-lazy-mount-root-v1\0".len()
                    + 16
                    + root_text.0.len()
                    + node.version.0.len()) as u64,
                ..crate::WorkCounters::default()
            })?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-lazy-mount-root-v1\0");
            hasher.update(&self.workspace().id().into_bytes());
            hasher.update(root_text.0.as_bytes());
            hasher.update(&node.version.0);
            let mut key = [0; 16];
            key.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            self.promote_exact(
                &root_text.0,
                expected_source,
                0,
                IdempotencyKey::from_bytes(key),
                &LazyTreapWork::native_with_work(work, cancellation)?,
            )
            .await?;
        }
        drop(selected);
        drop(selected_custody);
        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let checkout = self
            .workspace()
            .checkout(GenerationSelector::Head, mode, work, cancellation)
            .await?;
        let source = options::checkout_source(
            checkout,
            root,
            options.publication,
            work,
            native_original,
            callback_guard,
        )?;
        Ok((source, self.workspace().id().volume_id(), root_text))
    }

    /// Mounts the actual sparse source without scanning unresolved descendants.
    pub async fn mount(
        &self,
        destination: impl AsRef<Path>,
        options: MountOptions,
    ) -> Result<LazyMount<A, O, D, S>, MountLifecycleError> {
        let work = if let Some(work) = options.work.as_ref() {
            work.clone()
        } else {
            acyclic_native_runtime::initialize_native_io_services()
                .map_err(NativeMountError::from)?;
            super::super::initialize_native_mount_services()?;
            NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(cause, _)| MountLifecycleError::Materialization(cause))?
        };
        let work = &work;
        let native_original = options.native_original.clone();
        let native_original = native_original.as_ref();
        let callback_guard = options.callback_guard.clone();
        super::super::adapter::validate_native_source_io_binding(work, native_original)?;
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
        let (authored, volume_id, root_text) = self
            .prepare_authored_mount_source(&options, callback_guard.as_ref(), work, native_original)
            .await?;
        let root_custody = match root_text.1 {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) => custody,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        };
        let root_text = root_text.0;
        let session_destination = work.path(&destination, None)?;
        let lazy = self.clone().into_arc_with_native_work(work)?;
        let mut source_allocation = work.claim(
            std::mem::size_of::<LazyMountSource<A, O, D, S>>() as u64,
            1,
            crate::WorkCounters::default(),
        )?;
        let mut slot = Arc::<LazyMountSource<A, O, D, S>>::new_uninit();
        let prepared =
            super::super::prepare_native_mount_start(work).map_err(NativeMountError::from)?;
        let request = NativeMountRequest {
            mount_id: MountId::new(),
            volume_id,
            destination: session_destination.0,
            writable: options.writable,
        };
        let source = match LazyMountSource::new(
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
        };
        Arc::get_mut(&mut slot)
            .expect("unpublished source slot is unique")
            .write(source);
        // SAFETY: the unique slot was initialized before its Arc was published.
        #[allow(
            unsafe_code,
            reason = "initializes the real source payload before publishing its Arc"
        )]
        let source = unsafe { slot.assume_init() };
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
            payload_custody: None,
        })
    }
}
