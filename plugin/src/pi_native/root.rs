use super::*;
use acyclic_harness::filesystem::PreparationIo;

pub(crate) struct PiNativeRoot {
    allocation: Option<NativePreparationRoot>,
    filesystem: acyclic_fs::LocalFs,
    view_root: PathBuf,
    operation_root: PathBuf,
    task: acyclic_harness::TaskId,
    scratch: VolumeRef,
    _metadata_custody: acyclic_fs::NativeAllocation,
}

impl PiNativeRoot {
    /// Requires the original creator's completed allocation.
    pub(crate) fn witnessed_allocation(&self) -> Result<&NativePreparationRoot> {
        self.allocation.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Indeterminate,
            reason: "Pi native root has no completed original creator witness",
        })
    }

    pub(crate) async fn create_directories(
        &mut self,
        owner_shared: (
            &Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
            &Arc<TaskContext>,
        ),
        binding: &PiTaskBinding,
        options_intent_session: (&NativeViewOptions, &PiNativeLaunchIntent, &FileRef),
        bindings: &[BorrowedNativeVolumeBinding<
            '_,
            crate::LocalAuthorityBackend,
            crate::LocalObjectBackend,
        >],
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        let (owner, shared) = owner_shared;
        let (options, intent, session) = options_intent_session;
        let io = PreparationIo::from_native_work(meter)?;
        let mut history = &io;
        owner.verify(false, &mut Some(&mut history)).await?;
        let reject = || Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi native directory creation differs from its admitted seed",
        };
        let [scratch] = bindings else {
            return Err(reject());
        };
        if owner.task_id() != self.task
            || binding.task_id != self.task
            || shared.durable_task_id() != Some(self.task)
            || binding.scratch.volume != self.scratch
            || options.root != self.view_root
            || !self
                ._metadata_custody
                .work()
                .shares_allocation_context(meter)
            || scratch.volume != &self.scratch
            || scratch.path != "scratch"
            || !scratch.writable
            || scratch.namespace != &NativeVolumeNamespace::MaterializedDirectory
            || session.volume() != &self.scratch
            || session.path() != intent.session_path
        {
            return Err(reject());
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::verify(
                scratch.verifier,
                scratch.scope,
                &self.scratch,
                operation,
                shared.scope().limits().file_bytes,
                &mut Some(&mut history),
            )?;
            owner.require_volume_grant(&self.scratch, operation, &mut Some(&mut history))?;
        }
        let bytes = shared.read_file(session, Some(&io)).await?;
        if bytes.as_ref() != intent.session_bytes.as_bytes() {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi session publication differs from its immutable cut",
            });
        }
        let PiScratchAllocation::Created(creation) = &binding.scratch.allocation else {
            return Err(invalid("Pi scratch has no filesystem creator"));
        };
        let name = self.scratch.storage_name(&mut Some(&mut history))?;
        let actual = self
            .filesystem
            .open_workspace(&name, meter, meter.cancellation())
            .await?;
        if actual.id().into_bytes() != creation.workspace_id {
            return Err(invalid("Pi scratch differs from its filesystem creator"));
        }
        if self.allocation.is_some() {
            return Ok(());
        }
        let target = meter.path(&self.operation_root, Some(Path::new("target")))?;
        let backup = meter.path(&self.operation_root, Some(Path::new("backup")))?;
        for path in [&self.view_root, &self.operation_root, &target.0, &backup.0] {
            owner.verify(false, &mut Some(&mut history)).await?;
            std::fs::create_dir_all(path)
                .map_err(|error| Error::from(meter.runtime_error(error)))?;
        }
        let mut witnessed =
            NativePreparationRoot::witness(&self.view_root, &self.operation_root, meter)?.await?;
        owner.verify(false, &mut Some(&mut history)).await?;
        self._metadata_custody.absorb(&mut witnessed.1)?;
        self.allocation = Some(witnessed.0);
        Ok(())
    }
    /// Mounts the existing lazy child workspace without a copied project tree or another physical-root watcher.
    #[allow(
        clippy::too_many_arguments,
        reason = "original created project, native allocation, and scoped publication selection"
    )]
    pub(crate) async fn mount_project_with_work(
        &self,
        owner: &Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
        shared: &Arc<TaskContext>,
        binding: &PiTaskBinding,
        root_key: &str,
        workspace: &crate::LocalLazyWorkspace,
        original_generation: &acyclic_harness::resources::GenerationRef,
        authority: &(AuthorityVerifier, Scope, acyclic_fs::NativeAllocation),
        writable: bool,
        retained: &mut Option<Arc<crate::LocalSingleMount>>,
        retained_allocation: &mut Option<acyclic_fs::NativeAllocation>,
        original_volume_work: &NativeMaterializationWork,
        meter: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(NativeVolumeMapping, acyclic_fs::NativeAllocation)> {
        let context = shared.as_ref();
        if !authority.2.work().shares_ledger(meter) || !original_volume_work.shares_ledger(meter) {
            return Err(acyclic_harness::Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        owner
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(meter)?),
            )
            .await?;
        if owner.task_id() != self.task
            || binding.task_id != self.task
            || context.durable_task_id() != Some(self.task)
            || binding.scratch.volume != self.scratch
        {
            return Err(Error::Unauthorized(
                "Pi lazy namespace belongs to another original task".into(),
            ));
        }
        let volume = binding
            .project_volumes
            .get(root_key)
            .ok_or_else(|| invalid("Pi lazy project is not selected by the original child"))?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if operation == VolumeOperation::Write && !writable {
                continue;
            }
            ContentGrant::verify(
                &authority.0,
                &authority.1,
                volume,
                operation,
                context.scope().limits().file_bytes,
                &mut Some(&mut &PreparationIo::from_native_work(meter)?),
            )?;
            owner.require_volume_grant(
                volume,
                operation,
                &mut Some(&mut &PreparationIo::from_native_work(meter)?),
            )?;
        }
        let allocation = self.witnessed_allocation()?;
        let route = native_root_route(root_key, meter)?;
        let mut path = meter.path(&allocation.root, Some(Path::new(&route.0)))?;
        if meter.root_identity(&allocation.root)? != allocation.expected_identity {
            return Err(Error::Conflict(
                "Pi original native allocation was replaced".into(),
            ));
        }
        let callback_guard =
            shared.volume_callback(owner, volume, writable, original_volume_work)?;
        let mut volume_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(meter)?);
        if volume.storage_name(&mut Some(&mut volume_io))? != workspace.workspace().name().as_str()
        {
            return Err(invalid(
                "Pi mounted workspace differs from its selected volume",
            ));
        }
        if retained.is_none() {
            if retained_allocation.is_some() {
                return Err(Error::Indeterminate(OperationId::from_bytes(
                    self.task.into_bytes(),
                )));
            }
            std::fs::create_dir_all(&path.0)
                .map_err(|error| Error::from(meter.runtime_error(error)))?;
            let io = PreparationIo::from_native_work(original_volume_work)?;
            let host = acyclic_harness::filesystem::FilesystemHost::new(
                self.filesystem.clone(),
                volume.provider(),
                Some(&io),
            )?;
            let binding = BorrowedNativeVolumeBinding {
                host: &host,
                volume,
                generation: original_generation,
                namespace: &NativeVolumeNamespace::MaterializedDirectory,
                mounted: None,
                path: &route.0,
                writable,
                verifier: &authority.0,
                scope: &authority.1,
                publication: acyclic_fs::PublicationPermit::Unrestricted,
            };
            let selected = workspace
                .workspace()
                .head(original_volume_work, cancellation)
                .await?
                .id();
            let options = binding
                .mount_options(
                    workspace.workspace(),
                    selected,
                    shared,
                    original_volume_work,
                )
                .await?
                .publication(acyclic_fs::MountPublication::Manual)
                .native_context(
                    original_volume_work,
                    Some(Arc::clone(&callback_guard)),
                    Some(original_volume_work),
                );
            let mut allocation = original_volume_work.claim(
                std::mem::size_of::<crate::LocalSingleMount>() as u64,
                1,
                acyclic_fs::WorkCounters::default(),
            )?;
            let mut mount = match workspace.mount(path.0.as_path(), options).await {
                Ok(mount) => mount,
                Err(cause) => return Err(Error::from(cause)),
            };
            let custody_failure = mount.retain_payload_allocation(&mut allocation).err();
            let mount = retained.insert(Arc::new(mount));
            if let Some(cause) = custody_failure {
                *retained_allocation = Some(allocation);
                if mount.retire(&CancellationToken::new()).is_ok() {
                    *retained = None;
                    *retained_allocation = None;
                }
                return Err(Error::from(cause));
            }
        }
        let mount = retained
            .as_ref()
            .ok_or_else(|| invalid("Pi lazy mount acquisition lost its actual owner"))?;
        if mount.path() != path.0.as_path() {
            return Err(Error::Conflict(
                "Pi retained project mount belongs to another native namespace".into(),
            ));
        }
        let snapshot = mount
            .snapshot(original_volume_work, cancellation)
            .await
            .map_err(Error::from)?;
        if snapshot.workspace_id != workspace.workspace().id()
            || snapshot.authored_generation.digest().as_bytes().as_slice()
                != original_generation.as_resource().key()
        {
            return Err(Error::Conflict("Pi lazy mount differs from the original actual filesystem workspace or seed generation".into()));
        }
        owner
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(meter)?),
            )
            .await?;
        let authority_revision = *digest_pi_value(
            &(authority.0.audience(), &authority.1),
            context
                .scope()
                .limits()
                .file_bytes
                .min(crate::MAXIMUM_ADAPTER_STATE_BYTES),
            meter,
        )?
        .as_bytes();
        let mut volume_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(meter)?);
        let volume_copy = volume.copy(&mut Some(&mut volume_io))?;
        let mut volume_copy = (volume_copy, volume_io.into_custody()?);
        let reference = original_generation.as_resource();
        let mut generation_copy = acyclic_harness::resources::GenerationRef::new(
            reference.provider(),
            reference.key(),
            reference.version(),
            Some(meter),
        )
        .and_then(|(reference, custody)| {
            Ok((
                reference,
                custody.ok_or(acyclic_fs::NativeTreeMaterializationError::MissingCustody)?,
            ))
        })?;
        let namespace = NativeVolumeNamespace::LazyMount { snapshot };
        let mut metadata = meter.claim(0, 0, acyclic_fs::WorkCounters::default())?;
        metadata.absorb(&mut volume_copy.1)?;
        metadata.absorb(&mut generation_copy.1)?;
        metadata.absorb(&mut path.1)?;
        Ok((
            NativeVolumeMapping {
                volume: volume_copy.0,
                generation: generation_copy.0,
                namespace,
                path: path.0,
                writable,
                authority_revision,
            },
            metadata,
        ))
    }

    /// Retains the creator through every fallible preparation step.
    pub(crate) async fn prepare<'a>(
        &self,
        owner: &'a Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<
            'a,
            crate::LocalAuthorityBackend,
            crate::LocalObjectBackend,
        >],
        guards: &'a [Arc<TaskContext>],
        meter: &'a NativeMaterializationWork,
    ) -> Result<Arc<crate::pi_native_host::PiNativeLiveView>> {
        owner
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(meter)?),
            )
            .await?;
        let allocation = self.witnessed_allocation()?;
        let reject = || Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi mixed declaration differs from its retained original creator",
        };
        let [scratch, project] = bindings else {
            return Err(reject());
        };
        if self.task != owner.task_id()
            || allocation.root != options.root
            || scratch.volume != &self.scratch
            || scratch.path != "scratch"
            || !scratch.writable
            || !matches!(
                scratch.namespace,
                NativeVolumeNamespace::MaterializedDirectory
            )
            || !matches!(project.namespace, NativeVolumeNamespace::LazyMount { .. })
        {
            return Err(reject());
        }
        let view =
            NativeVolumeView::prepare(owner, guards, allocation, options, bindings, meter)?.await?;
        owner
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(meter)?),
            )
            .await?;
        meter.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(Arc::new(view))
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "original protected creator and exact admitted scratch selection"
)]
pub(crate) async fn native_root<'a>(
    resources: &crate::ServiceResources,
    root_session: &str,
    protected: &crate::pi_owner::PiOwnerConfig,
    owner: &'a Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
    shared: &Arc<TaskContext>,
    binding: &PiTaskBinding,
    options: &NativeViewOptions,
    intent: &PiNativeLaunchIntent,
    session: &FileRef,
    bindings: &[BorrowedNativeVolumeBinding<
        'a,
        crate::LocalAuthorityBackend,
        crate::LocalObjectBackend,
    >],
    meter: &NativeMaterializationWork,
) -> Result<PiNativeRoot> {
    meter.admit(WorkCounters::default())?;
    let reject = || Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason: "Pi native seed metadata differs from its original admitted child",
    };
    let [scratch] = bindings else {
        return Err(reject());
    };
    if shared.durable_task_id() != Some(owner.task_id())
        || owner.task_id() != binding.task_id
        || scratch.volume != &binding.scratch.volume
        || scratch.path != "scratch"
        || !scratch.writable
        || !matches!(
            scratch.namespace,
            NativeVolumeNamespace::MaterializedDirectory
        )
        || !options.work_per_volume.has_finite_allowances()
        || !matches!(binding.scratch.allocation, PiScratchAllocation::Created(_))
    {
        return Err(reject());
    }
    protected.authority(meter)?;
    intent.validate(meter)?;
    let mut session_generation = [0; 32];
    hex::decode_to_slice(session.version(), &mut session_generation).map_err(|_| {
        Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi session has no original filesystem generation",
        }
    })?;
    if session.volume() != &binding.scratch.volume
        || session.path() != intent.session_path
        || scratch.generation.as_resource().key() != session_generation
        || session.descriptor().byte_length() > intent.maximum_bytes
    {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Pi seed session selection differs from its immutable native cut",
        });
    }
    native_service_root(
        &resources.data,
        root_session,
        owner.task_id(),
        &options.root,
        meter,
    )?;
    let task_directory = options.root.parent().ok_or(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: "Pi native view has no original task namespace",
    })?;
    let mut operation_root = meter.path(task_directory, Some(Path::new("preparation")))?;
    let mut scratch_io =
        acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(meter)?);
    let scratch = binding.scratch.volume.copy(&mut Some(&mut scratch_io))?;
    let mut scratch = (scratch, scratch_io.into_custody()?);
    let mut view_root = meter.path(&options.root, None)?;
    let mut custody = meter.claim(
        std::mem::size_of::<PiNativeRoot>() as u64,
        0,
        acyclic_fs::WorkCounters::default(),
    )?;
    custody.absorb(&mut scratch.1)?;
    custody.absorb(&mut view_root.1)?;
    custody.absorb(&mut operation_root.1)?;
    Ok(PiNativeRoot {
        allocation: None,
        filesystem: resources.fs.clone(),
        view_root: view_root.0,
        operation_root: operation_root.0,
        task: owner.task_id(),
        scratch: scratch.0,
        _metadata_custody: custody,
    })
}
