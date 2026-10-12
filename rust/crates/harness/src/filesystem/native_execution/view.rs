use super::*;

/// Provider-owned materialized namespace, with the canonical SDK capture owner for every independently authorized volume.
pub struct NativeVolumeView<A, O> {
    pub(super) volumes: Vec<PreparedVolume<A, O>>,
    pub(super) preparation: NativePreparationReceipt,
}

impl<P, A, O> NativeProcessView<P> for NativeVolumeView<A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }
    fn original_native_work(&self) -> &NativeMaterializationWork {
        self.work()
    }
    fn validate_for_dispatch<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || self.validate_dispatch_with_work(work))
    }
    fn validate_working_directory(
        &self,
        cwd: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.work()
            .cancellation()
            .check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        if cwd == self.preparation.manifest.options.root {
            return work
                .open_working_directory(
                    &self.preparation.manifest.options.root,
                    self.original_root_identity()?,
                    Path::new(""),
                )
                .map(drop)
                .map_err(Error::from);
        }
        for (mapping, volume) in self.preparation.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if let Ok(relative) = cwd.strip_prefix(&mapping.path) {
                let physical_work = match &volume.view {
                    NativePreparedVolumeBackend::Materialized { .. } => work,
                    NativePreparedVolumeBackend::HeldLazy(lazy) => lazy.original_work(),
                };
                return physical_work
                    .open_working_directory(&mapping.path, volume.original_identity, relative)
                    .map(drop)
                    .map_err(Error::from);
            }
        }
        Err(Error::from(
            acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
        ))
    }
    fn retain_native_namespace_scopes<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                Vec<acyclic_fs::native_mount::NativeMountWorkScope>,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        crate::NativeFutureBundle::new(work, || async {
            self.work()
                .cancellation()
                .check()
                .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            work.charge(acyclic_fs::WorkCounters {
                items_examined: self.volumes.len() as u64,
                ..Default::default()
            })?;
            let count = self
                .volumes
                .iter()
                .filter(|volume| matches!(volume.view, NativePreparedVolumeBackend::HeldLazy(_)))
                .count();
            let mut custody = work.claim(
                std::mem::size_of::<(
                    Vec<acyclic_fs::native_mount::NativeMountWorkScope>,
                    acyclic_fs::NativeAllocation,
                )>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let (mut scopes, mut vector_custody) = allocate(work, |io| {
                crate::filesystem::fork_preparer_io::vector(count, Some(io))
            })?;
            custody.absorb(&mut vector_custody)?;
            for volume in &self.volumes {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                    lazy.snapshot(lazy.original_work())?.await?;
                    scopes.push(lazy.begin_scope(lazy.original_work())?);
                }
            }
            Ok((scopes, custody))
        })
    }
    fn publish<'a>(
        &'a self,
        owner: &'a TaskJournalOwner<P>,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>,
        >,
    > {
        let custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        crate::NativeFutureBundle::new(work, || async move {
            Ok(self.publish_original(owner, work, Some(custody)).await)
        })
    }
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Issues only an original funded, protected Applied preparation.
    pub fn task_working_view_with_native_work<'a, P: StreamProvider>(
        self: &'a Arc<Self>,
        owner: &'a Arc<TaskJournalOwner<P>>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Arc<dyn NativeTaskWorkingView>>> {
        crate::NativeFutureBundle::new(work, || async move {
            let custody = work.claim(
                std::mem::size_of::<HeldNativeTaskWorkingView<P, A, O>>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let held = HeldNativeTaskWorkingView {
                view: Arc::clone(self),
                owner: Arc::clone(owner),
                custody,
            };
            held.authenticate(work).await?;
            work.charge(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..Default::default()
            })?;
            let resource: Arc<dyn NativeTaskWorkingView> = Arc::new(held);
            owner
                .verify(
                    false,
                    &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
                )
                .await?;
            Ok(resource)
        })
    }

    /// Returns only the actual original operation retained by native preparation.
    #[must_use]
    pub fn work(&self) -> &NativeMaterializationWork {
        self.preparation._custody.work()
    }

    /// Retires only this held view's SDK mounts on their accepted birth credit.
    pub fn cleanup_mounts_with_native_work<'a, P: StreamProvider>(
        &'a self,
        owner: &'a TaskJournalOwner<P>,
        process: &'a mut Option<crate::filesystem::native_process::NativeProcessDrainProof>,
        partial: &'a mut Option<NativeTaskViewDrainReceipt>,
        original: &'a NativeMaterializationWork,
        work: &'a NativeMaterializationWork,
        wait: &'a acyclic_fs::CancellationToken,
    ) -> Result<crate::NativeFutureBundle<'a, NativeTaskViewDrainReceipt>> {
        crate::NativeFutureBundle::new(work, || async move {
            wait.check()
                .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            if !work.remaining()?.has_finite_allowances()
                || !self
                    .preparation
                    ._custody
                    .work()
                    .shares_allocation_context(original)
                || !owner.native_shared_work_matches(original)
                || self.preparation.manifest.volumes.len() != self.volumes.len()
                || process.is_some() == partial.is_some()
            {
                return Err(Error::from(
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                ));
            }
            work.charge(acyclic_fs::WorkCounters {
                items_examined: self.volumes.len() as u64,
                ..Default::default()
            })?;
            let proof = if let Some(receipt) = partial.as_ref() {
                if receipt.mounts.len() > self.volumes.len()
                    || receipt.mounts.capacity() < self.volumes.len()
                {
                    return Err(Error::from(
                        acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    ));
                }
                &receipt.process
            } else {
                process.as_ref().ok_or_else(|| {
                    Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)
                })?
            };
            proof.require_view_binding(self, owner, original)?;
            for volume in &self.volumes {
                if !volume._work.shares_ledger(original) {
                    return Err(Error::from(
                        acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    ));
                }
                if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                    if !lazy.matches_owner(owner)
                        || !lazy
                            .original_work()
                            .shares_allocation_context(&volume._work)
                    {
                        return Err(Error::from(
                            acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                        ));
                    }
                }
            }
            self.authenticate_mount_retirement(owner, work).await?;
            if partial.is_none() {
                let mut custody = work.claim(
                    std::mem::size_of::<NativeTaskViewDrainReceipt>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                let (mounts, mut vector_custody) = allocate(work, |io| {
                    crate::filesystem::fork_preparer_io::vector(self.volumes.len(), Some(io))
                })?;
                custody.absorb(&mut vector_custody)?;
                wait.check()
                    .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
                work.remaining()?;
                let process = process.take().ok_or_else(|| {
                    Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)
                })?;
                *partial = Some(NativeTaskViewDrainReceipt {
                    process,
                    mounts,
                    custody,
                });
            }
            let receipt = partial
                .as_mut()
                .expect("accepted cleanup retains its partial receipt");
            while receipt.mounts.len() < self.volumes.len() {
                wait.check()
                    .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
                self.authenticate_mount_retirement(owner, work).await?;
                let volume = &self.volumes[receipt.mounts.len()];
                let proof = match &volume.view {
                    NativePreparedVolumeBackend::Materialized { view, .. } => {
                        view.retire(wait)?
                    }
                    NativePreparedVolumeBackend::HeldLazy(lazy) => lazy.retire(wait)?,
                };
                receipt.mounts.push(proof);
                if let Some(cause) = receipt
                    .mounts
                    .last_mut()
                    .and_then(acyclic_fs::NativeMountDrainProof::take_retained_capture_failure)
                {
                    return Err(Error::from(cause));
                }
                self.authenticate_mount_retirement(owner, work).await?;
            }
            wait.check()
                .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            work.remaining()?;
            self.authenticate_mount_retirement(owner, work).await?;
            Ok(partial
                .take()
                .expect("completed cleanup retains its receipt"))
        })
    }

    pub(super) async fn authenticate_mount_retirement<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let (outcome, custody) = owner.outcome_with_native_work(work).await?;
        if outcome.is_none() {
            owner
                .verify(
                    true,
                    &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
                )
                .await?;
        }
        drop(outcome);
        drop(custody);
        for mapping in &self.preparation.manifest.volumes {
            owner.require_volume_grant(
                &mapping.volume,
                VolumeOperation::Read,
                &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
            )?;
            if mapping.writable {
                owner.require_volume_grant(
                    &mapping.volume,
                    VolumeOperation::Write,
                    &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
                )?;
            }
        }
        Ok(())
    }

    /// Returns this attachment's original preparation.
    pub fn preparation(&self) -> &NativePreparationReceipt {
        &self.preparation
    }

    /// Exact request/approval manifest. Changing it requires new admission.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }

    /// Reads an entire mounted Project/agent-private file under this view's original task, exact mapping and current READ grant.
    pub async fn read_builtin_file<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        relative: &Path,
        maximum_bytes: usize,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let (mapping, volume, file) = self.builtin_volume(relative, VolumeOperation::Read, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work)
            .await?;
        match &volume.view {
            NativePreparedVolumeBackend::HeldLazy(lazy) => {
                lazy.read(file, maximum_bytes, work)?.await
            }
            NativePreparedVolumeBackend::Materialized { view, .. } => {
                let lease = view
                    .native_source_io_lease()
                    .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
                let physical = work.with_source_io_lease(lease)?;
                physical
                    .read_working_file(
                        &mapping.path,
                        volume.original_identity,
                        file,
                        maximum_bytes,
                        || self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work),
                    )
                    .await
            }
        }
    }

    /// Writes bounded UTF-8 only through an original mounted writable mapping.
    #[allow(
        clippy::too_many_arguments,
        reason = "original command, mapping and edit preimage are explicit"
    )]
    pub async fn write_builtin_file<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        relative: &Path,
        content: &str,
        create_parents: bool,
        expected_sha256: Option<[u8; 32]>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let (mapping, volume, file) =
            self.builtin_volume(relative, VolumeOperation::Write, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work)
            .await?;
        if expected_sha256.is_some() {
            owner.require_volume_grant(
                &mapping.volume,
                VolumeOperation::Read,
                &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
            )?;
        }
        match &volume.view {
            NativePreparedVolumeBackend::HeldLazy(lazy) => {
                lazy.write(file, content, create_parents, expected_sha256, work)?
                    .await
            }
            NativePreparedVolumeBackend::Materialized { view, .. } => {
                let lease = view
                    .native_source_io_lease()
                    .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
                let physical = work.with_source_io_lease(lease)?;
                physical
                    .write_working_file(
                        &mapping.path,
                        volume.original_identity,
                        file,
                        content,
                        create_parents,
                        expected_sha256,
                        || async {
                            self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work)
                                .await?;
                            if expected_sha256.is_some() {
                                owner.require_volume_grant(
                                    &mapping.volume,
                                    VolumeOperation::Read,
                                    &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
                                )?;
                            }
                            Ok::<(), Error>(())
                        },
                    )
                    .await
            }
        }
    }

    pub(super) fn builtin_volume<'a>(
        &'a self,
        relative: &'a Path,
        operation: VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<(&'a NativeVolumeMapping, &'a PreparedVolume<A, O>, &'a Path)> {
        let invalid = || Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout);
        let text = relative.to_str().ok_or_else(invalid)?;
        if text.is_empty()
            || text.contains(['\\', '\0'])
            || text
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(invalid());
        }
        for component in relative.components() {
            work.charge(acyclic_fs::WorkCounters {
                source_path_components: 1,
                ..Default::default()
            })?;
            if !matches!(component, Component::Normal(_)) {
                return Err(invalid());
            }
        }
        if self.volumes.len() != self.preparation.manifest.volumes.len() {
            return Err(invalid());
        }
        for (mapping, volume) in self.preparation.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            let mount = mapping
                .path
                .strip_prefix(&self.preparation.manifest.options.root)
                .map_err(|_| invalid())?;
            let Ok(file) = relative.strip_prefix(mount) else {
                continue;
            };
            let path = match &volume.view {
                NativePreparedVolumeBackend::Materialized { view, .. } => view.path(),
                NativePreparedVolumeBackend::HeldLazy(view) => view.path(),
            };
            if file.as_os_str().is_empty()
                || path != mapping.path
                || !matches!(
                    mapping.volume.class(),
                    crate::conversation::VolumeClass::Project
                        | crate::conversation::VolumeClass::AgentPrivate
                )
                || (operation == VolumeOperation::Write && !mapping.writable)
            {
                return Err(invalid());
            }
            return Ok((mapping, volume, file));
        }
        Err(invalid())
    }

    pub(super) async fn authenticate_builtin_view<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        owner
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
            )
            .await?;
        let store = NativePreparationStore {
            authority: NativePreparationAuthority::Owner {
                owner,
                settlement: false,
            },
            work: work.clone(),
        };
        let record = store
            .load_record(self.preparation.journal.plan.operation_id, false)
            .await?
            .ok_or_else(|| {
                Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)
            })?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: record.plan.edits.len() as u64,
            ..Default::default()
        })?;
        let valid = record.phase == MaterializationPhase::Applied
            && record.root_identity == self.preparation.journal.root_identity
            && record.preparation_identity == self.preparation.journal.preparation_identity
            && record.plan == self.preparation.journal.plan;
        drop(record);
        if !valid {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        owner
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
            )
            .await
    }

    pub(super) async fn verify_builtin_owner<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        mapping: &NativeVolumeMapping,
        operation: VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.task_binding() != (self.preparation.task, &self.preparation.original_fence) {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        self.work()
            .cancellation()
            .check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        owner
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
            )
            .await?;
        owner.require_volume_grant(
            &mapping.volume,
            operation,
            &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
        )?;
        let expected = self
            .preparation
            .journal
            .root_identity
            .map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or_else(|| {
                Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)
            })?;
        let actual = work.root_identity(&self.preparation.manifest.options.root)?;
        if expected != actual {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual },
            ));
        }
        Ok(())
    }

    pub(super) fn original_root_identity(&self) -> Result<acyclic_fs::NativeRootIdentity> {
        self.preparation
            .journal
            .root_identity
            .map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or(boundary(
                crate::wire::ErrorCode::Conflict,
                "native preparation has no original root witness",
            ))
    }

    pub(super) async fn validate_dispatch_with_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let expected = self.original_root_identity()?;
        self.work()
            .cancellation()
            .check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        if work.root_identity(&self.preparation.manifest.options.root)? != expected {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "native preparation root was replaced",
            ));
        }
        for (volume, mapping) in self.volumes.iter().zip(&self.preparation.manifest.volumes) {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..acyclic_fs::WorkCounters::default()
            })?;
            match &volume.view {
                NativePreparedVolumeBackend::Materialized { view, .. } => {
                    view.validate_for_presentation(&volume._work).await?;
                    view.verify_unchanged(&volume._work).await?;
                }
                NativePreparedVolumeBackend::HeldLazy(lazy) => {
                    let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                        return Err(Error::from(
                            acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                        ));
                    };
                    let actual = lazy.snapshot(lazy.original_work())?.await?;
                    if actual.workspace_id != snapshot.workspace_id
                        || actual.source != snapshot.source
                        || lazy.path() != mapping.path
                        || lazy.original_work().root_identity(&mapping.path)?
                            != volume.original_identity
                    {
                        return Err(Error::from(
                            acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                        ));
                    }
                }
            }
        }
        if work.root_identity(&self.preparation.manifest.options.root)? != expected {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "native preparation root changed during validation",
            ));
        }
        self.work()
            .cancellation()
            .check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        Ok(())
    }
}
