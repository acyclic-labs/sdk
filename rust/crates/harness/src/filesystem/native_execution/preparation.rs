use super::preparation_record::{Intent, invalid};
use super::*;
use acyclic_fs::{
    JournaledMaterializer, MaterializationEdit, MaterializationIntent, MaterializationJournalStore,
    MaterializationPhase, MaterializationPlan, WorkCounters,
};

async fn verify_binding<P: StreamProvider + 'static, A, O>(
    owner: &TaskJournalOwner<P>,
    binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    guard: &crate::runtime::NativeVolumeCallbackGuard<P>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    guard.require_volume(owner, binding.volume, binding.writable, work)?;
    let io = PreparationIo::from_native_work(work)?;
    for operation in [VolumeOperation::Read]
        .into_iter()
        .chain(binding.writable.then_some(VolumeOperation::Write))
    {
        owner.require_volume_grant(binding.volume, operation, &mut Some(&mut io.history_io()))?;
        ContentGrant::authorize_volume(
            binding.verifier,
            binding.scope,
            binding.volume,
            operation,
            owner.input_limits().file_bytes,
            &mut Some(&mut io.history_io()),
        )?;
    }
    owner.verify(false, &mut Some(&mut io.history_io())).await
}

impl<A: AsyncAuthorityStore + Send + Sync + 'static, O: AsyncObjectStore + Send + Sync + 'static>
    NativeVolumeView<A, O>
{
    /// Prepares exact materialized generations through the protected SDK journal.
    pub fn prepare<'a, P: StreamProvider + 'static>(
        owner: &'a Arc<TaskJournalOwner<P>>,
        guards: &'a [crate::runtime::NativeVolumeCallbackGuard<P>],
        allocation: &'a NativePreparationRoot,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        crate::NativeFutureBundle::new(work, || async move {
            if !owner.native_shared_work_matches(work)
                || options.root != allocation.root
                || !options.work_per_volume.has_finite_allowances()
                || bindings.is_empty()
                || bindings.len() > options.maximum_volumes as usize
                || bindings.len() != guards.len()
            {
                return Err(invalid());
            }
            let io = PreparationIo::from_native_work(work)?;
            let plan_io = PreparationIo::from_native_work(work)?;
            let mut mappings = crate::model::vec_with_capacity(bindings.len(), Some(&io))?;
            let mut generations = crate::model::vec_with_capacity(bindings.len(), Some(&io))?;
            let mut selected = crate::model::vec_with_capacity(bindings.len(), Some(&plan_io))?;
            let mut edits = crate::model::vec_with_capacity(bindings.len(), Some(&plan_io))?;
            for (index, (binding, guard)) in bindings.iter().zip(guards).enumerate() {
                if binding.namespace != &NativeVolumeNamespace::MaterializedDirectory {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "native commands require materialized directory selections",
                    });
                }
                if binding.path.is_empty()
                    || binding.path.contains(['\\', '\0'])
                    || binding
                        .path
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == "..")
                {
                    return Err(invalid());
                }
                let path = Path::new(binding.path);
                work.charge(WorkCounters {
                    source_path_components: path.components().count() as u64,
                    ..Default::default()
                })?;
                for prior in &bindings[..index] {
                    work.charge(WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                    if overlap(path, Path::new(prior.path)) {
                        return Err(invalid());
                    }
                }
                let volume_work = work.native_volume_work(options.work_per_volume)?;
                verify_binding(owner, binding, guard, &volume_work).await?;
                let volume_io = PreparationIo::from_native_work(&volume_work)?;
                let reference = crate::filesystem::fork_preparer_io::content_workspace(
                    &binding.host.provider,
                    binding.volume,
                    Some(&volume_io),
                )?;
                let workspace =
                    crate::filesystem::fork_preparer_io::open(binding.host, &reference, &volume_io)
                        .await?;
                let generation = crate::filesystem::fork_preparer_io::generation(
                    binding.host,
                    &workspace,
                    binding.generation,
                    &volume_io,
                )
                .await?;
                if binding.writable
                    && workspace
                        .head(&volume_work, work.cancellation())
                        .await?
                        .id()
                        != generation.id()
                {
                    return Err(invalid());
                }
                let (path, custody) = work.path(&options.root, Some(path))?;
                io.absorb_native_custody(custody)?;
                mappings.push(NativeVolumeMapping {
                    volume: binding.volume.copy(&mut Some(&mut io.history_io()))?,
                    generation: binding.generation.clone_with_io(&io)?,
                    namespace: NativeVolumeNamespace::MaterializedDirectory,
                    path,
                    writable: binding.writable,
                    authority_revision: crate::executor::json::canonical_json_digest(
                        &(binding.verifier.audience(), binding.scope),
                        owner.input_limits().file_bytes,
                        &mut Some(&mut io.history_io()),
                    )?,
                });
                let top = binding.path.split('/').next().ok_or_else(invalid)?;
                if !edits.iter().any(
                    |edit| matches!(edit, MaterializationEdit::Install { path, .. } if path == top),
                ) {
                    let (path, custody) = work.display(top)?;
                    plan_io.absorb_native_custody(custody)?;
                    let (image, custody) =
                        work.copy_bytes(acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)?;
                    plan_io.absorb_native_custody(custody)?;
                    edits.push(MaterializationEdit::Install { path, image });
                }
                selected.push(acyclic_fs::MaterializationPreparationSelection {
                    provider_identity: crate::store::history::canonical_bytes_with_work(
                        binding.volume.provider(),
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        &mut Some(&mut plan_io.history_io()),
                    )?,
                    volume: workspace.id().volume_id(),
                    generation: generation.id(),
                });
                generations.push((generation, volume_work));
            }
            edits.sort_unstable_by(|left, right| match (left, right) {
                (
                    MaterializationEdit::Install { path: left, .. },
                    MaterializationEdit::Install { path: right, .. },
                ) => left.cmp(right),
                _ => unreachable!("only native preparation installs"),
            });
            let (root, custody) = work.path(&options.root, None)?;
            io.absorb_native_custody(custody)?;
            let manifest = NativeViewManifest {
                options: NativeViewOptions {
                    root,
                    maximum_volumes: options.maximum_volumes,
                    work_per_volume: options.work_per_volume,
                },
                volumes: mappings,
            };
            manifest.validate(Some(&io))?;
            let store = super::journal::Store {
                owner: Arc::clone(owner),
                work: work.clone(),
            };
            let operation = super::journal::operation(owner.task_binding().0, work)?;
            let retained = store.load(operation).await?;
            let backend = allocation.backend(work).await?;
            backend.require_selected_root(&edits, &[])?;
            let intent = if let Some(retained) = &retained {
                let original = super::preparation_record::intent(retained, &io)?;
                if (original.task, &original.original_fence) != owner.task_binding()
                    || original.manifest != manifest
                    || original.operation_directory != allocation.operation_directory
                {
                    return Err(invalid());
                }
                original
            } else {
                backend.require_initial_empty()?;
                let (operation_directory, custody) =
                    work.path(&allocation.operation_directory, None)?;
                io.absorb_native_custody(custody)?;
                Intent {
                    task: owner.task_binding().0,
                    original_fence: owner
                        .task_binding()
                        .1
                        .clone_with_work(&mut Some(&mut io.history_io()))?,
                    operation_directory,
                    manifest,
                }
            };
            let plan = MaterializationPlan {
                operation_id: operation,
                intent: MaterializationIntent::NativePreparation {
                    selection: selected,
                    original: crate::store::history::canonical_bytes_with_work(
                        &intent,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        &mut Some(&mut plan_io.history_io()),
                    )?,
                },
                edits,
            };
            if retained
                .as_ref()
                .is_some_and(|retained| retained.plan != plan)
            {
                return Err(invalid());
            }
            drop(retained);
            let materializer = JournaledMaterializer::new(store, backend);
            let retained_plan = plan_io.owned_bytes()?;
            let mut journal = materializer
                .prepare(plan, Some(plan_io.into_native_custody(retained_plan)?))
                .await?;
            if journal.root_identity != Some(allocation.expected_identity.to_bytes())
                || journal.preparation_identity.as_ref()
                    != Some(&allocation.expected_preparation_identity)
            {
                return Err(invalid());
            }
            let verify = || async {
                for (binding, guard) in bindings.iter().zip(guards) {
                    verify_binding(owner, binding, guard, work).await?;
                }
                Ok::<_, Error>(())
            };
            if journal.phase == MaterializationPhase::Preparing && journal.preimages.is_empty() {
                for (binding, (generation, volume_work)) in bindings.iter().zip(&generations) {
                    verify().await?;
                    let completion = materializer.store().prepare_action(journal).await?;
                    let identity = materializer
                        .backend()
                        .reset_target_directory(binding.path)
                        .await?;
                    journal = completion.await?;
                    let (path, _path_custody) =
                        materializer.backend().staging_path(binding.path)?;
                    let options = acyclic_fs::MaterializeOptions {
                        expected_root_identity: Some(identity),
                        ..acyclic_fs::MaterializeOptions::native(path)
                    };
                    let io_paths = PreparationIo::from_native_work(volume_work)?;
                    let (empty, custody) = volume_work.display("")?;
                    io_paths.absorb_native_custody(custody)?;
                    let mut paths = crate::model::vec_with_capacity(1, Some(&io_paths))?;
                    paths.push(empty);
                    journal = materializer
                        .stage_generation(
                            generation,
                            &paths,
                            &options,
                            journal,
                            volume_work,
                            &mut { verify },
                        )
                        .await?;
                }
            }
            journal = materializer
                .recover(
                    operation,
                    acyclic_fs::MaterializationRecovery::Complete,
                    &mut { verify },
                )
                .await?
                .ok_or_else(invalid)?;
            let mut volumes = crate::model::vec_with_capacity(bindings.len(), Some(&io))?;
            for (index, (binding, (generation, volume_work))) in
                bindings.iter().zip(generations).enumerate()
            {
                verify().await?;
                let identity = materializer.backend().mapping_root_identity(binding.path)?;
                let callback = guards[index].for_volume(
                    owner,
                    binding.volume,
                    binding.writable,
                    &volume_work,
                )?;
                let mount = if binding.writable {
                    acyclic_fs::MountOptions::read_write()
                        .publication(acyclic_fs::MountPublication::Manual)
                } else {
                    acyclic_fs::MountOptions::read_only()
                };
                let mount = mount.native_context(
                    &volume_work,
                    Some(Arc::clone(&callback)),
                    Some(&volume_work),
                );
                let view = generation
                    .attach_native_working_set(
                        &mount,
                        &acyclic_fs::MaterializeOptions::native(
                            &intent.manifest.volumes[index].path,
                        ),
                        identity,
                        options.work_per_volume,
                        work.cancellation(),
                    )
                    .await?;
                volumes.push(PreparedVolume {
                    view,
                    publication: binding.publication,
                    work: volume_work,
                    callback,
                });
            }
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.absorb_native_custody(journal.custody.take().ok_or_else(invalid)?)?;
            verify().await?;
            let retained = io.owned_bytes()?;
            Ok(Self {
                volumes,
                preparation: intent,
                custody: io.into_native_custody(retained)?,
            })
        })
    }
}

fn overlap(left: &Path, right: &Path) -> bool {
    left.components()
        .zip(right.components())
        .all(|(left, right)| {
            #[cfg(windows)]
            {
                left.as_os_str()
                    .to_str()
                    .unwrap_or_default()
                    .chars()
                    .flat_map(char::to_uppercase)
                    .eq(right
                        .as_os_str()
                        .to_str()
                        .unwrap_or_default()
                        .chars()
                        .flat_map(char::to_uppercase))
            }
            #[cfg(not(windows))]
            {
                left == right
            }
        })
}

impl From<acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>>
    for Error
{
    fn from(
        error: acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>,
    ) -> Self {
        match error {
            acyclic_fs::MaterializationError::Store(cause) => cause,
            acyclic_fs::MaterializationError::Backend(cause) => cause.into(),
            acyclic_fs::MaterializationError::PlanConflict
            | acyclic_fs::MaterializationError::ExternalMutation
            | acyclic_fs::MaterializationError::Contended => Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "native materialization selection or physical state changed",
            },
            acyclic_fs::MaterializationError::IncompatibleJournal => invalid(),
        }
    }
}
impl From<acyclic_fs::MaterializeError> for Error {
    fn from(cause: acyclic_fs::MaterializeError) -> Self {
        acyclic_fs::native_mount::MountLifecycleError::Materialize(cause).into()
    }
}
impl From<acyclic_fs::NativeStageUncertainty<Error>> for Error {
    fn from(failure: acyclic_fs::NativeStageUncertainty<Error>) -> Self {
        Self::NativeStageUncertain { failure }
    }
}
