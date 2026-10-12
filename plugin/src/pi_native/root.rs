use super::*;

pub(crate) struct PiNativeRoot {
    pub(crate) view: Option<Arc<crate::pi_native_host::PiNativeLiveView>>,
    allocation: Option<NativePreparationRoot>,
    packet: acyclic_fs::materializer::NativeOwnedRootAllocation,
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

    pub(crate) async fn create_directories_into_with_work<'a>(
        &mut self,
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
    ) -> Result<()> {
        owner
            .verify(false, &mut Some(&mut native_work_io(meter)?))
            .await?;
        if owner.task_id() != self.task
            || binding.task_id != self.task
            || shared.durable_task_id() != Some(self.task)
            || binding.scratch.volume != self.scratch
            || options.root != self.view_root
            || self.packet.operation_id()
                != NativePreparationRoot::operation(owner.task_id(), meter)?
            || !self
                ._metadata_custody
                .work()
                .shares_allocation_context(meter)
            || bindings.len() != 1
            || bindings[0].volume != &self.scratch
            || bindings[0].path != "scratch"
            || !bindings[0].writable
            || !matches!(
                bindings[0].namespace,
                NativeVolumeNamespace::MaterializedDirectory
            )
            || session.volume() != &self.scratch
            || session.path() != intent.session_path
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native directory creation differs from its retained admitted seed",
            });
        }
        let creator = owner.child_scratch_creation_proof()?;
        if acyclic_harness::runtime::task_id_for_admission(creator.origin().intended_child)
            != self.task
            || creator.volume() != &self.scratch
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native directory creation differs from its sealed Created scratch",
            });
        }
        creator
            .authorize_settlement_with_native_work(owner, meter)
            .await?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::verify(
                bindings[0].verifier,
                bindings[0].scope,
                &self.scratch,
                operation,
                shared.scope().limits().file_bytes,
                &mut Some(&mut native_work_io(meter)?),
            )?;
            owner.require_volume_grant(
                &self.scratch,
                operation,
                &mut Some(&mut native_work_io(meter)?),
            )?;
        }
        let bytes = shared.read_file_with_native_work(session, meter).await?;
        if bytes.as_ref() != intent.session_bytes.as_bytes() {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi original session publication differs from its immutable native cut",
            });
        }
        drop(bytes);
        let PiScratchAllocation::Created(creation) = &binding.scratch.allocation else {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native seed scratch has no actual filesystem creator",
            });
        };
        let actual = bindings[0]
            .host
            .open_volume_with_native_work(&self.scratch, meter)
            .await?;
        if actual.id().into_bytes() != creation.workspace_id {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi scratch workspace differs from its original filesystem creator",
            });
        }
        let service_root = self
            .view_root
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .expect("validated native view has a service parent");
        meter.root_identity(service_root)?;
        if self.allocation.is_some() {
            return Ok(());
        }
        let mut recovered =
            crate::pi_native_host::PiNativeLiveView::recover_allocation(owner, options, meter)?
                .await?;
        if let Some(allocation) = recovered.0.as_ref() {
            if allocation.root != self.view_root
                || allocation.operation_directory != self.operation_root
            {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi recorded original preparation selected another native namespace",
                });
            }
            self._metadata_custody.absorb(&mut recovered.1)?;
            self.allocation = recovered.0;
            return Ok(());
        }
        for index in 0..4 {
            if matches!(
                self.packet.status(index),
                Some(acyclic_fs::materializer::NativeOwnedRootStatus::Known(_))
            ) {
                continue;
            }
            self.packet
                .create_into_guarded(index, meter, || async {
                    owner
                        .verify(false, &mut Some(&mut native_work_io(meter)?))
                        .await?;
                    creator
                        .authorize_settlement_with_native_work(owner, meter)
                        .await
                })
                .await?;
        }
        let identities = self
            .packet
            .known_identities()
            .ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Indeterminate,
                reason: "Pi native creator has not confirmed every original directory identity",
            })?;
        creator
            .authorize_settlement_with_native_work(owner, meter)
            .await?;
        let mut witnessed =
            NativePreparationRoot::witness(&self.view_root, &self.operation_root, meter)?.await?;
        let observed = witnessed.0.expected_preparation_identity;
        if witnessed.0.expected_identity != identities[0]
            || acyclic_fs::NativeRootIdentity::from_bytes(observed.operation) != identities[1]
            || acyclic_fs::NativeRootIdentity::from_bytes(observed.target) != identities[2]
            || acyclic_fs::NativeRootIdentity::from_bytes(observed.backup) != identities[3]
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi native witness differs from its actual original creation receipts",
            });
        }
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
        filesystem: Arc<crate::pi_native_host::PiNativeFilesystem>,
        original_generation: &acyclic_harness::resources::GenerationRef,
        authority: &(AuthorityVerifier, Scope, acyclic_fs::NativeAllocation),
        publication: PublicationPermit,
        writable: bool,
        retained: &mut Option<Arc<crate::LocalSingleMount>>,
        retained_allocation: &mut Option<acyclic_fs::NativeAllocation>,
        startup_owner: &mut Option<acyclic_fs::native_mount::NativeMountSession>,
        original_volume_work: &NativeMaterializationWork,
        meter: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(
        NativeVolumeMapping,
        NativeExistingLazyVolume,
        NativeVolumeCallbackGuard<acyclic_stream::LocalStream>,
        acyclic_fs::NativeAllocation,
    )> {
        let context = shared.as_ref();
        if !authority.2.work().shares_ledger(meter) || !original_volume_work.shares_ledger(meter) {
            return Err(acyclic_harness::Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        owner
            .verify(false, &mut Some(&mut native_work_io(meter)?))
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
                &mut Some(&mut native_work_io(meter)?),
            )?;
            owner.require_volume_grant(
                volume,
                operation,
                &mut Some(&mut native_work_io(meter)?),
            )?;
        }
        let allocation = self.witnessed_allocation()?;
        let route = native_root_route(root_key, meter)?;
        let mut path = meter.join_path(&allocation.root, Path::new(&route.0))?;
        if meter.root_identity(&allocation.root)? != allocation.expected_identity {
            return Err(Error::Conflict(
                "Pi original native allocation was replaced".into(),
            ));
        }
        let callback_guard = shared
            .volume_callback_guard_with_native_work(owner, meter)
            .await?;
        let actual = filesystem
            .open_volume_with_native_work(volume, original_volume_work)
            .await?;
        if retained.is_none() {
            if startup_owner.is_some() || retained_allocation.is_some() {
                return Err(Error::Indeterminate(OperationId::from_bytes(
                    self.task.into_bytes(),
                )));
            }
            meter.create_owned_directory(
                &allocation.root,
                allocation.expected_identity,
                Path::new(&route.0),
                true,
            )?;
            let options = if writable {
                acyclic_fs::MountOptions::read_write()
            } else {
                acyclic_fs::MountOptions::read_only()
            }
            .publication(acyclic_fs::MountPublication::Manual);
            let source_guard = callback_guard
                .for_volume(owner, volume, writable, original_volume_work)?
                .0;
            let mut allocation = original_volume_work.claim(
                std::mem::size_of::<crate::LocalSingleMount>() as u64,
                1,
                acyclic_fs::WorkCounters::default(),
            )?;
            let mut slot = Arc::<crate::LocalSingleMount>::new_uninit();
            let mut mount = match workspace
                .mount_with_work(
                    path.0.as_path(),
                    options,
                    Some(source_guard),
                    &original_volume_work,
                    cancellation,
                )
                .await
            {
                Ok(mount) => mount,
                Err(acyclic_fs::native_mount::MountLifecycleError::Native(
                    acyclic_fs::native_mount::NativeMountError::StartupFailed(failure),
                )) => {
                    let (cause, owner) = failure.into_parts();
                    *startup_owner = Some(owner);
                    return Err(Error::from(
                        acyclic_fs::native_mount::MountLifecycleError::Native(cause),
                    ));
                }
                Err(cause) => return Err(Error::from(cause)),
            };
            let custody_failure = mount.retain_payload_allocation(&mut allocation).err();
            Arc::get_mut(&mut slot)
                .expect("new original mount slot is exclusive")
                .write(mount);
            // SAFETY: the exclusive slot contains the successful SDK mount.
            *retained = Some(unsafe { slot.assume_init() });
            if let Some(cause) = custody_failure {
                *retained_allocation = Some(allocation);
                if retained
                    .as_ref()
                    .expect("actual mount retained above")
                    .abandon_with_work(&original_volume_work)
                    .is_ok()
                {
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
            .snapshot_with_work(&original_volume_work, cancellation)
            .await
            .map_err(Error::from)?;
        if snapshot.workspace_id != actual.id()
            || snapshot.authored_generation.digest().as_bytes().as_slice()
                != original_generation.as_resource().key()
        {
            return Err(Error::Conflict("Pi lazy mount differs from the original actual filesystem workspace or seed generation".into()));
        }
        owner
            .verify(false, &mut Some(&mut native_work_io(meter)?))
            .await?;
        let authority_revision = native_digest(
            &(authority.0.audience(), &authority.1),
            context
                .scope()
                .limits()
                .file_bytes
                .min(crate::MAXIMUM_ADAPTER_STATE_BYTES),
            meter,
        )?;
        let mut volume_io = native_work_io(meter)?;
        let volume_copy = volume.copy(&mut Some(&mut volume_io))?;
        let mut volume_copy = (volume_copy, volume_io.into_custody());
        let reference = original_generation.as_resource();
        let mut generation_copy = acyclic_harness::resources::GenerationRef::new_with_native_work(
            reference.provider(),
            reference.key(),
            reference.version(),
            meter,
        )?;
        let namespace = NativeVolumeNamespace::LazyMount { snapshot };
        let declaration = BorrowedNativeVolumeBinding {
            host: filesystem.as_ref(),
            volume,
            generation: original_generation,
            namespace: &namespace,
            path: &route.0,
            writable,
            verifier: &authority.0,
            scope: &authority.1,
            publication,
        };
        let existing = NativeExistingLazyVolume::capture_with_native_work(
            mount,
            owner,
            &declaration,
            &callback_guard,
            meter,
            original_volume_work,
        )?
        .await?;
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
            existing,
            callback_guard,
            metadata,
        ))
    }

    /// Retains the creator through every fallible preparation step.
    pub(crate) async fn prepare<'a>(
        &mut self,
        owner: &'a Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<
            'a,
            crate::LocalAuthorityBackend,
            crate::LocalObjectBackend,
        >],
        guards: &'a [NativeVolumeCallbackGuard<acyclic_stream::LocalStream>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>],
        meter: &'a NativeMaterializationWork,
    ) -> Result<()> {
        owner
            .verify(false, &mut Some(&mut native_work_io(meter)?))
            .await?;
        let allocation = self.witnessed_allocation()?;
        if self.task != owner.task_id()
            || allocation.root != options.root
            || bindings.len() != 2
            || bindings[0].volume != &self.scratch
            || bindings[0].path != "scratch"
            || !bindings[0].writable
            || !matches!(
                bindings[0].namespace,
                NativeVolumeNamespace::MaterializedDirectory
            )
            || !matches!(
                bindings[1].namespace,
                NativeVolumeNamespace::LazyMount { .. }
            )
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi mixed declaration differs from its retained original creator",
            });
        }
        let view = NativeVolumeView::prepare(
            owner,
            guards,
            allocation,
            options,
            bindings,
            existing_lazy,
            meter,
        )?
        .await?;
        let prepared = view.preparation();
        if prepared.task != self.task
            || prepared.journal.plan.operation_id != self.packet.operation_id()
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi mixed view belongs to another original preparation operation",
            });
        }
        owner
            .verify(false, &mut Some(&mut native_work_io(meter)?))
            .await?;
        self.view = Some(view.into_arc_with_native_work(meter)?);
        Ok(())
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
    if shared.durable_task_id() != Some(owner.task_id())
        || owner.task_id() != binding.task_id
        || bindings.len() != 1
        || bindings[0].volume != &binding.scratch.volume
        || bindings[0].path != "scratch"
        || !bindings[0].writable
        || !matches!(
            bindings[0].namespace,
            NativeVolumeNamespace::MaterializedDirectory
        )
        || !options.work_per_volume.has_finite_allowances()
        || !matches!(binding.scratch.allocation, PiScratchAllocation::Created(_))
    {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi native seed metadata differs from its original admitted child",
        });
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
        || bindings[0].generation.as_resource().key() != session_generation
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
    let operation = NativePreparationRoot::operation(owner.task_id(), meter)?;
    let task_directory = options.root.parent().ok_or(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: "Pi native view has no original task namespace",
    })?;
    let mut operation_root = meter.join_path(task_directory, Path::new("preparation"))?;
    let target = meter.join_path(&operation_root.0, Path::new("target"))?;
    let backup = meter.join_path(&operation_root.0, Path::new("backup"))?;
    fn relative<'p>(path: &'p Path, data: &Path) -> Result<&'p Path> {
        path.strip_prefix(data).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi native seed escaped its held private service root",
        })
    }
    let packet = acyclic_fs::materializer::NativeOwnedRootAllocation::new_with_native_work(
        operation,
        &resources.private_state_root,
        [
            relative(&options.root, &resources.data)?,
            relative(&operation_root.0, &resources.data)?,
            relative(&target.0, &resources.data)?,
            relative(&backup.0, &resources.data)?,
        ],
        meter,
    )?;
    let mut scratch_io = native_work_io(meter)?;
    let scratch = binding.scratch.volume.copy(&mut Some(&mut scratch_io))?;
    let mut scratch = (scratch, scratch_io.into_custody());
    let mut view_root = meter.copy_path(&options.root)?;
    let mut custody = meter.claim(
        std::mem::size_of::<PiNativeRoot>() as u64,
        0,
        acyclic_fs::WorkCounters::default(),
    )?;
    custody.absorb(&mut scratch.1)?;
    custody.absorb(&mut view_root.1)?;
    custody.absorb(&mut operation_root.1)?;
    Ok(PiNativeRoot {
        view: None,
        allocation: None,
        packet,
        view_root: view_root.0,
        operation_root: operation_root.0,
        task: owner.task_id(),
        scratch: scratch.0,
        _metadata_custody: custody,
    })
}
