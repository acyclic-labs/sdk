use super::preparation_record::{Intent, invalid};
use super::*;
use acyclic_fs::{
    JournaledMaterializer, MaterializationEdit, MaterializationIntent, MaterializationJournalStore,
    MaterializationPhase, MaterializationPlan, WorkCounters,
};

async fn verify_binding<P: StreamProvider + 'static, A, O>(
    owner: &Arc<TaskJournalOwner<P>>,
    binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    guard: &Arc<crate::runtime::TaskContext>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    guard.require_volume(owner, binding.volume, binding.writable, work)?;
    let io = PreparationIo::from_native_work(work)?;
    for operation in [VolumeOperation::Read]
        .into_iter()
        .chain(binding.writable.then_some(VolumeOperation::Write))
    {
        ContentGrant::authorize_volume(
            binding.verifier,
            binding.scope,
            binding.volume,
            operation,
            owner.input_limits().file_bytes,
            &mut Some(&mut io.history_io()),
        )?;
    }
    let metadata_io = PreparationIo::from_native_work(guard.native_work()?)?;
    owner
        .verify(false, &mut Some(&mut metadata_io.history_io()))
        .await
}

#[cfg(test)]
#[path = "preparation_properties.rs"]
mod properties;

impl<A: AsyncAuthorityStore + Send + Sync + 'static, O: AsyncObjectStore + Send + Sync + 'static>
    NativeVolumeView<A, O>
{
    /// Prepares exact native bindings through the protected SDK journal.
    pub fn prepare<'a, P: StreamProvider + 'static>(
        owner: &'a Arc<TaskJournalOwner<P>>,
        guards: &'a [Arc<crate::runtime::TaskContext>],
        allocation: &'a NativePreparationRoot,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        crate::NativeFutureBundle::new(work, || async move {
            Preparation {
                owner,
                guards,
                allocation,
                options,
                bindings,
                work,
            }
            .execute()
            .await
        })
    }
}

struct Preparation<'a, P: StreamProvider, A, O> {
    owner: &'a Arc<TaskJournalOwner<P>>,
    guards: &'a [Arc<crate::runtime::TaskContext>],
    allocation: &'a NativePreparationRoot,
    options: &'a NativeViewOptions,
    bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
    work: &'a NativeMaterializationWork,
}

type SelectedGeneration<A, O> = (
    Option<acyclic_fs::Generation<A, O>>,
    NativeMaterializationWork,
);

struct Selection<'a, A, O> {
    mappings: Vec<NativeVolumeMapping>,
    generations: Vec<SelectedGeneration<A, O>>,
    selected: Vec<acyclic_fs::MaterializationPreparationSelection>,
    edits: Vec<MaterializationEdit>,
    preserved: Vec<&'a str>,
}
struct PreparedPlan<P: StreamProvider> {
    materializer: JournaledMaterializer<
        super::journal::Store<P>,
        Arc<acyclic_fs::NativeTreeMaterializationBackend>,
    >,
    journal: acyclic_fs::MaterializationJournal,
    intent: Arc<Intent>,
    intent_custody: NativeAllocation,
}

struct SelectionPlan {
    selected: Vec<acyclic_fs::MaterializationPreparationSelection>,
    edits: Vec<MaterializationEdit>,
    operation: acyclic_fs::OperationId,
}

impl<'a, P, A, O> Preparation<'a, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn execute(&self) -> Result<NativeVolumeView<A, O>> {
        let Self {
            owner,
            guards,
            allocation,
            options,
            bindings,
            work,
        } = *self;
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
        let Selection {
            mappings,
            generations,
            selected,
            mut edits,
            preserved,
        } = self.select(&io, &plan_io).await?;
        edits.sort_unstable_by(|left, right| match (left, right) {
            (
                MaterializationEdit::Install { path: left, .. },
                MaterializationEdit::Install { path: right, .. },
            ) => left.cmp(right),
            _ => unreachable!("only native preparation installs"),
        });
        let manifest = self.manifest(mappings, &io)?;
        let store = super::journal::Store {
            owner: Arc::clone(owner),
            work: work.clone(),
            original: None,
        };
        let operation = super::journal::operation(owner.task_binding().0, work)?;
        let retained = store.load(operation).await?;
        let backend = allocation.backend(work).await?;
        self.require_selected_root(&backend, &edits, &preserved)?;
        let intent = self.intent(manifest, retained.as_ref(), &backend, &io)?;
        let plan = self.plan(
            &intent,
            retained.as_ref(),
            SelectionPlan {
                selected,
                edits,
                operation,
            },
            &plan_io,
        )?;
        drop(retained);
        self.authorize_manifest(&intent.manifest, &io)?;
        let PreparedPlan {
            materializer,
            journal,
            intent,
            intent_custody,
        } = self
            .prepare_journal(store, backend, intent, plan, plan_io)
            .await?;
        let mut journal = self
            .stage(&materializer, journal, &generations, operation)
            .await?;
        let volumes = self
            .volumes(&materializer, &intent, generations, &io)
            .await?;
        drop(materializer);
        let intent = Arc::try_unwrap(intent).map_err(|_| invalid())?;
        drop(intent_custody);
        io.retain_owned(std::mem::size_of::<NativeVolumeView<A, O>>() as u64)?;
        io.absorb_native_custody(journal.custody.take().ok_or_else(invalid)?)?;
        owner.verify(false, &mut Some(&mut io.history_io())).await?;
        let retained = io.owned_bytes()?;
        Ok(NativeVolumeView {
            volumes,
            preparation: intent,
            custody: io.into_native_custody(retained)?,
        })
    }
    fn manifest(
        &self,
        mappings: Vec<NativeVolumeMapping>,
        io: &PreparationIo,
    ) -> Result<NativeViewManifest> {
        let Self { work, options, .. } = *self;
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
        manifest.validate(Some(io))?;
        Ok(manifest)
    }
    fn require_selected_root(
        &self,
        backend: &acyclic_fs::NativeTreeMaterializationBackend,
        edits: &[MaterializationEdit],
        preserved: &[&str],
    ) -> Result<()> {
        let Self {
            bindings,
            options,
            work,
            ..
        } = *self;
        backend.require_selected_root(edits, preserved, |entry| {
            for binding in bindings {
                if let Some(mount) = binding.mounted
                    && mount.path().parent() == Some(options.root.as_path())
                    && mount.owns_parent_entry(entry, work)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        Ok(())
    }
    async fn prepare_journal(
        &self,
        mut store: super::journal::Store<P>,
        backend: Arc<acyclic_fs::NativeTreeMaterializationBackend>,
        intent: Intent,
        plan: MaterializationPlan,
        plan_io: PreparationIo,
    ) -> Result<PreparedPlan<P>> {
        let work = self.work;
        let MaterializationIntent::NativePreparation { original, .. } = &plan.intent else {
            return Err(invalid());
        };
        let (encoded, custody) = work.copy_bytes(original)?;
        let intent_custody = work.claim(
            (std::mem::size_of::<Intent>() + 2 * std::mem::size_of::<usize>()) as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        let intent = Arc::new(intent);
        store.original = Some((Arc::clone(&intent), encoded, custody));
        let materializer = JournaledMaterializer::new(store, backend);
        let retained_plan = plan_io.owned_bytes()?;
        let journal = materializer
            .prepare(plan, Some(plan_io.into_native_custody(retained_plan)?))
            .await?;

        Ok(PreparedPlan {
            materializer,
            journal,
            intent,
            intent_custody,
        })
    }
    fn binding_work(
        &self,
        index: usize,
        binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    ) -> Result<NativeMaterializationWork> {
        let Self {
            bindings,
            options,
            work,
            ..
        } = *self;
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
        for prior in bindings.iter().take(index) {
            work.charge(WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if overlap(path, Path::new(prior.path))
                || (overlap(
                    Path::new(binding.path.split('/').next().ok_or_else(invalid)?),
                    Path::new(prior.path.split('/').next().ok_or_else(invalid)?),
                ) && binding.mounted.is_some() != prior.mounted.is_some())
            {
                return Err(invalid());
            }
        }
        let volume_work = match (binding.namespace, binding.mounted) {
            (NativeVolumeNamespace::MaterializedDirectory, None) => {
                work.native_volume_work(options.work_per_volume)?
            }
            (NativeVolumeNamespace::LazyMount { .. }, Some(mount)) => {
                let original = mount.work()?;
                if !original.shares_ledger(work)
                    || original.budget() != options.work_per_volume
                    || !original.remaining()?.has_finite_allowances()
                {
                    return Err(invalid());
                }
                original
            }
            _ => return Err(invalid()),
        };

        Ok(volume_work)
    }

    async fn select(
        &self,
        io: &PreparationIo,
        plan_io: &PreparationIo,
    ) -> Result<Selection<'a, A, O>> {
        let Self {
            owner,
            guards,
            bindings,
            work,
            ..
        } = *self;
        let mut mappings = crate::model::vec_with_capacity(bindings.len(), Some(io))?;
        let mut generations = crate::model::vec_with_capacity(bindings.len(), Some(io))?;
        let mut selected = crate::model::vec_with_capacity(bindings.len(), Some(plan_io))?;
        let mut edits = crate::model::vec_with_capacity(bindings.len(), Some(plan_io))?;
        let mut preserved = crate::model::vec_with_capacity(bindings.len(), Some(plan_io))?;
        for (index, (binding, guard)) in bindings.iter().zip(guards).enumerate() {
            let volume_work = self.binding_work(index, binding)?;
            verify_binding(owner, binding, guard, &volume_work).await?;
            let volume_io = PreparationIo::from_native_work(&volume_work)?;
            let reference = super::super::fork_preparer::bounded_io::content_workspace(
                &binding.host.provider,
                binding.volume,
                Some(&volume_io),
            )?;
            let workspace = binding.host.open(&reference, Some(&volume_io)).await?;
            let generation = binding
                .host
                .generation(&workspace, binding.generation, Some(&volume_io))
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
            let (mapping, selection) =
                self.mapping(binding, &workspace, &generation, io, plan_io)?;
            mappings.push(mapping);
            selected.push(selection);
            if let Some(mount) = binding.mounted {
                if mount.path() != mappings.last().ok_or_else(invalid)?.path
                    || !matches!(binding.namespace, NativeVolumeNamespace::LazyMount { snapshot } if snapshot.workspace_id == workspace.id() && snapshot.authored_generation == generation.id())
                {
                    return Err(invalid());
                }
                mounted::verify_snapshot(mount.as_ref(), binding.namespace, &volume_work).await?;
                preserved.push(binding.path);
                generations.push((None, volume_work));
                continue;
            }
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
            generations.push((Some(generation), volume_work));
        }

        Ok(Selection {
            mappings,
            generations,
            selected,
            edits,
            preserved,
        })
    }

    fn mapping(
        &self,
        binding: &BorrowedNativeVolumeBinding<'_, A, O>,
        workspace: &acyclic_fs::Workspace<A, O>,
        generation: &acyclic_fs::Generation<A, O>,
        io: &PreparationIo,
        plan_io: &PreparationIo,
    ) -> Result<(
        NativeVolumeMapping,
        acyclic_fs::MaterializationPreparationSelection,
    )> {
        let Self {
            owner,
            options,
            work,
            ..
        } = *self;
        let (path, custody) = work.path(&options.root, Some(Path::new(binding.path)))?;
        io.absorb_native_custody(custody)?;
        let mapping = NativeVolumeMapping {
            volume: binding.volume.copy(&mut Some(&mut io.history_io()))?,
            generation: binding.generation.clone_with_io(io)?,
            namespace: binding.namespace.clone(),
            path,
            writable: binding.writable,
            authority_revision: crate::executor::json::canonical_json_digest(
                &(binding.verifier.audience(), binding.scope),
                owner.input_limits().file_bytes,
                &mut Some(&mut io.history_io()),
            )?,
        };
        let selection = acyclic_fs::MaterializationPreparationSelection {
            provider_identity: crate::store::history::canonical_bytes_with_work(
                binding.volume.provider(),
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut Some(&mut plan_io.history_io()),
            )?,
            volume: workspace.id().volume_id(),
            generation: generation.id(),
        };
        Ok((mapping, selection))
    }
    fn intent(
        &self,
        manifest: NativeViewManifest,
        retained: Option<&acyclic_fs::MaterializationJournal>,
        backend: &acyclic_fs::NativeTreeMaterializationBackend,
        io: &PreparationIo,
    ) -> Result<Intent> {
        let Self {
            owner,
            allocation,
            work,
            ..
        } = *self;
        let intent = if let Some(retained) = retained {
            let original = super::preparation_record::intent(retained, io)?;
            if (original.task, &original.original_fence) != owner.task_binding()
                || original.manifest != manifest
                || original.operation_directory != allocation.operation_directory
            {
                return Err(invalid());
            }
            original
        } else {
            backend.require_empty_staging()?;
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

        Ok(intent)
    }

    fn plan(
        &self,
        intent: &Intent,
        retained: Option<&acyclic_fs::MaterializationJournal>,
        inputs: SelectionPlan,
        plan_io: &PreparationIo,
    ) -> Result<MaterializationPlan> {
        let work = self.work;
        let SelectionPlan {
            selected,
            edits,
            operation,
        } = inputs;
        let original = if let Some(retained) = retained {
            let MaterializationIntent::NativePreparation { original, .. } = &retained.plan.intent
            else {
                return Err(invalid());
            };
            let (bytes, custody) = work.copy_bytes(original)?;
            plan_io.absorb_native_custody(custody)?;
            bytes
        } else {
            crate::store::history::canonical_bytes_with_work(
                intent,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut Some(&mut plan_io.history_io()),
            )?
        };
        let plan = MaterializationPlan {
            operation_id: operation,
            intent: MaterializationIntent::NativePreparation {
                selection: selected,
                original,
            },
            edits,
        };
        if retained.is_some_and(|retained| retained.plan != plan) {
            return Err(invalid());
        }

        Ok(plan)
    }

    fn authorize_manifest(&self, manifest: &NativeViewManifest, io: &PreparationIo) -> Result<()> {
        let owner = self.owner;
        // The admitted manifest and owner grants are immutable for this
        // adapter's lifetime; each action separately accepts the live fence.
        for mapping in &manifest.volumes {
            owner.require_volume_grant(
                &mapping.volume,
                VolumeOperation::Read,
                &mut Some(&mut io.history_io()),
            )?;
            if mapping.writable {
                owner.require_volume_grant(
                    &mapping.volume,
                    VolumeOperation::Write,
                    &mut Some(&mut io.history_io()),
                )?;
            }
        }

        Ok(())
    }

    async fn stage(
        &self,
        materializer: &JournaledMaterializer<
            super::journal::Store<P>,
            Arc<acyclic_fs::NativeTreeMaterializationBackend>,
        >,
        mut journal: acyclic_fs::MaterializationJournal,
        generations: &[SelectedGeneration<A, O>],
        operation: acyclic_fs::OperationId,
    ) -> Result<acyclic_fs::MaterializationJournal> {
        let Self {
            owner,
            allocation,
            bindings,
            work,
            ..
        } = *self;
        if journal.root_identity != Some(allocation.expected_identity.to_bytes())
            || journal.preparation_identity.as_ref()
                != Some(&allocation.expected_preparation_identity)
        {
            return Err(invalid());
        }
        let verify = move || async move {
            let io = PreparationIo::from_native_work(work)?;
            owner.verify(false, &mut Some(&mut io.history_io())).await
        };
        if journal.phase == MaterializationPhase::Preparing && journal.preimages.is_empty() {
            for (binding, (generation, volume_work)) in bindings.iter().zip(generations) {
                let Some(generation) = generation else {
                    continue;
                };
                let completion = materializer.store().prepare_action(journal).await?;
                let identity = materializer
                    .backend()
                    .reset_target_directory(binding.path)
                    .await?;
                journal = completion.await?;
                let (path, _path_custody) = materializer.backend().staging_path(binding.path)?;
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
                    .stage_generation(generation, &paths, &options, journal, volume_work, &mut {
                        verify
                    })
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

        Ok(journal)
    }

    async fn volumes(
        &self,
        materializer: &JournaledMaterializer<
            super::journal::Store<P>,
            Arc<acyclic_fs::NativeTreeMaterializationBackend>,
        >,
        intent: &Intent,
        generations: Vec<SelectedGeneration<A, O>>,
        io: &PreparationIo,
    ) -> Result<Vec<PreparedVolume<A, O>>> {
        let Self {
            owner,
            guards,
            options,
            bindings,
            work,
            ..
        } = *self;
        let verify = move || async move {
            let io = PreparationIo::from_native_work(work)?;
            owner.verify(false, &mut Some(&mut io.history_io())).await
        };
        let mut volumes = crate::model::vec_with_capacity(bindings.len(), Some(io))?;
        for (index, (binding, (generation, volume_work))) in
            bindings.iter().zip(generations).enumerate()
        {
            verify().await?;
            let callback = guards.get(index).ok_or_else(invalid)?.volume_callback(
                owner,
                binding.volume,
                binding.writable,
                &volume_work,
            )?;
            let view = if let Some(mount) = binding.mounted {
                mounted::verify_snapshot(mount.as_ref(), binding.namespace, &volume_work).await?;
                if materializer.backend().mapping_root_identity(binding.path)?
                    != mount.identity()?
                {
                    return Err(invalid());
                }
                VolumeView::Mounted {
                    owner: Arc::clone(mount),
                    _work_scope: mount.enter(&volume_work)?,
                }
            } else {
                let generation = generation.ok_or_else(invalid)?;
                let mount = binding
                    .mount_options(
                        &generation.workspace(),
                        generation.id(),
                        guards.get(index).ok_or_else(invalid)?,
                        &volume_work,
                    )
                    .await?
                    .publication(acyclic_fs::MountPublication::Manual)
                    .native_context(
                        &volume_work,
                        Some(Arc::clone(&callback)),
                        Some(&volume_work),
                    );
                let identity = materializer.backend().mapping_root_identity(binding.path)?;
                VolumeView::Directory(super::super::default_tools::recursive_box(
                    generation
                        .attach_native_working_set(
                            &mount,
                            &acyclic_fs::MaterializeOptions::native(
                                &intent.manifest.volumes.get(index).ok_or_else(invalid)?.path,
                            ),
                            identity,
                            options.work_per_volume,
                            work.cancellation(),
                        )
                        .await?,
                    Some(io),
                )?)
            };
            volumes.push(PreparedVolume {
                view,
                publication: binding.publication,
                work: volume_work,
                callback,
            });
        }

        Ok(volumes)
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
            acyclic_fs::MaterializationError::Backend(cause)
            | acyclic_fs::MaterializationError::Native(cause) => cause.into(),
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
