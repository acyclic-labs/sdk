use super::*;


impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{

    /// Prepares one immutable mixed declaration before any Scratch staging.
    pub fn prepare<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, guards: &'a [crate::runtime::NativeVolumeCallbackGuard<P>],
        allocation: &'a NativePreparationRoot, options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>], work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        if guards.len() != bindings.len() || existing_lazy.len() != bindings.len() {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        for ((guard, binding), lazy) in guards.iter().zip(bindings).zip(existing_lazy) {
            guard.require_volume_with_native_work(owner, binding.volume, binding.writable, work)?;
            match (binding.namespace, lazy) {
                (NativeVolumeNamespace::MaterializedDirectory, None) => {},
                (NativeVolumeNamespace::LazyMount { .. }, Some(lazy))
                    if lazy.held.original_work().shares_ledger(work)
                        && lazy.held.original_work().budget() == options.work_per_volume
                        && lazy.held.matches_owner(owner) => {},
                _ => return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)),
            }
        }
        crate::NativeFutureBundle::new(work, || async move {
            let cancellation = work.cancellation();
            owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
            if options.maximum_volumes == 0 || bindings.is_empty()
                || bindings.len() as u64 > u64::from(options.maximum_volumes)
                || !options.root.is_absolute() || !allocation.root.is_absolute()
                || !allocation.operation_directory.is_absolute()
                || !options.work_per_volume.has_finite_allowances()
            {
                return Err(boundary(crate::wire::ErrorCode::Invalid, "invalid native preparation allocation or allowances"));
            }
            if options.root != allocation.root {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "native options root differs from witnessed host allocation"));
            }
            let (root, mut output_custody) = work.path(&options.root, None)?;
            let owned_options = NativeViewOptions {
                root, maximum_volumes: options.maximum_volumes,
                work_per_volume: options.work_per_volume,
            };
            let (mut mappings, mut mappings_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            output_custody.absorb(&mut mappings_custody)?;
            let (mut binding_work, binding_work_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for (index, binding) in bindings.iter().enumerate() {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: 1, ..acyclic_fs::WorkCounters::default()
                })?;
                let lazy = existing_lazy.get(index).copied().flatten();
                let path = Path::new(binding.path);
                if path.as_os_str().is_empty() {
                    return Err(boundary(crate::wire::ErrorCode::Invalid, "native mapping path is empty"));
                }
                for part in path.components() {
                    work.charge(acyclic_fs::WorkCounters {
                        source_path_components: 1, ..acyclic_fs::WorkCounters::default()
                    })?;
                    if !matches!(part, Component::Normal(_)) {
                        return Err(boundary(crate::wire::ErrorCode::Invalid, "native volume paths overlap or escape their root"));
                    }
                }
                for prior in &bindings[..index] {
                    work.charge(acyclic_fs::WorkCounters {
                        items_examined: 1, ..acyclic_fs::WorkCounters::default()
                    })?;
                    if native_paths_overlap(path, Path::new(prior.path)) {
                        return Err(boundary(crate::wire::ErrorCode::Invalid, "native volume paths overlap or escape their root"));
                    }
                }
                let volume_work = match lazy {
                    Some(lazy) => lazy.held.original_work().clone(),
                    None => work.native_volume_work(options.work_per_volume)?,
                };
                verify_binding(owner, binding, &volume_work)?;
                let (volume, mut volume_custody) = allocate(&work, |io| binding.volume.copy(&mut Some(&mut io.history_io())))?;
                output_custody.absorb(&mut volume_custody)?;
                let (generation, mut generation_custody) = allocate(&work, |io| binding.generation.clone_with_io(io))?;
                output_custody.absorb(&mut generation_custody)?;
                let (path, mut path_custody) = work.path(&options.root, Some(path))?;
                output_custody.absorb(&mut path_custody)?;
                if let Some(lazy) = lazy {
                    let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else { unreachable!() };
                    if lazy.held.path() != path
                        || lazy.held.snapshot(&volume_work)?.await? != *snapshot
                    {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                }
                binding_work.push(volume_work);
                let authority_revision = {
                    let (bytes, _custody) = encode_record(&(binding.verifier.audience(), binding.scope), work)?;
                    work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64,
                        ..Default::default() })?;
                    *blake3::hash(&bytes).as_bytes()
                };
                if lazy.is_some_and(|lazy| !lazy.held.matches_binding(&volume, &generation,
                    binding.writable, binding.publication, &authority_revision))
                {
                    return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                }
                mappings.push(NativeVolumeMapping {
                    volume, generation, path, writable: binding.writable,
                    namespace: binding.namespace.clone(),
                    authority_revision,
                });
            }
            let manifest = NativeViewManifest {
                options: owned_options, volumes: mappings,
            };
            manifest.validate()?;
            let lazy_count = existing_lazy.iter().filter(|lazy| lazy.is_some()).count();
            let mut preserved_custody = work.claim(
                std::mem::size_of::<Vec<&str>>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            let (mut preserved, mut preserved_slots) = allocate(work, |io| crate::filesystem::fork_preparer_io::vector(lazy_count, Some(io)))?;
            preserved_custody.absorb(&mut preserved_slots)?;
            for (index, binding) in bindings.iter().enumerate() {
                if existing_lazy.get(index).copied().flatten().is_some() {
                    preserved.push(binding.path);
                }
            }
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            let backend = native_backend(&allocation, &work, true).await?;
            let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: false }, work: work.clone() };
            let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
            let operation = NativePreparationRoot::operation(owner.task_binding().0, &work)?;
            let retained = materializer.load(operation).await.map_err(Error::from)?;
            let (mut generations, generations_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            let (mut selection, mut plan_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for (binding, volume_work) in bindings.iter().zip(&binding_work) {
                owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                verify_binding(owner, binding, volume_work)?;
                let workspace = binding.host.open_volume_with_native_work(binding.volume, volume_work).await?;
                let generation = binding.host.generation_with_native_work(
                    &workspace, binding.generation, volume_work,
                ).await?;
                if binding.writable {
                    let head = workspace.head(
                        volume_work, cancellation,
                    ).await.map_err(|source| Error::NativeWorkspace { source, custody: None })?;
                    if head.id() != generation.id() {
                        return Err(boundary(crate::wire::ErrorCode::Conflict, "native destination generation is stale"));
                    }
                }
                let (provider_identity, mut provider_custody) = encode_record(binding.volume.provider(), &work)?;
                plan_custody.absorb(&mut provider_custody)?;
                selection.push(acyclic_fs::MaterializationPreparationSelection {
                    provider_identity, volume: workspace.id().volume_id(), generation: generation.id(),
                });
                generations.push((generation, volume_work.clone()));
            }
            drop(binding_work);
            drop(binding_work_custody);
            let mut intent_operation_custody = None;
            let intent = if let Some(journal) = &retained {
                let (intent, mut intent_custody) = preparation_intent(journal, &work)?;
                if (intent.task, &intent.original_fence) != owner.task_binding() || intent.manifest != manifest
                    || intent.operation_directory != allocation.operation_directory
                {
                    return Err(boundary(crate::wire::ErrorCode::Conflict, "native preparation original selection changed"));
                }
                require_allocation(journal, &allocation)?;
                drop(manifest);
                output_custody.resize(0)?;
                output_custody.absorb(&mut intent_custody)?;
                intent
            } else {
                materializer.backend().require_initial_empty()?;
                let (task, original_fence) = owner.task_binding();
                let (reservation_id, mut reservation_custody) = work.display(&original_fence.reservation_id)?;
                let (placement, mut placement_custody) = work.display(&original_fence.placement)?;
                output_custody.absorb(&mut reservation_custody)?;
                output_custody.absorb(&mut placement_custody)?;
                let original_fence = crate::scheduler::LeaseFence { reservation_id, placement };
                let (operation_directory, operation_custody) = work.path(&allocation.operation_directory, None)?;
                intent_operation_custody = Some(operation_custody);
                NativePreparationIntent { task, original_fence, operation_directory, manifest }
            };
            let manifest = &intent.manifest;
            let (mut top_level, top_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for binding in bindings {
                if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) { continue; }
                let first = Path::new(binding.path).components().next()
                    .and_then(|part| part.as_os_str().to_str())
                    .ok_or(boundary(crate::wire::ErrorCode::Invalid, "native mapping name must be Unicode"))?;
                top_level.push(first);
            }
            top_level.sort_unstable();
            top_level.dedup();
            let (mut edits, mut edits_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector(top_level.len(), Some(io)))?;
            plan_custody.absorb(&mut edits_custody)?;
            for path in &top_level {
                let (path, mut path_custody) = work.display(path)?;
                plan_custody.absorb(&mut path_custody)?;
                let (image, mut image_custody) = work.copy_bytes(acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)?;
                plan_custody.absorb(&mut image_custody)?;
                edits.push(acyclic_fs::MaterializationEdit::Install { path, image });
            }
            drop(top_level);
            drop(top_custody);
            let (original, mut original_custody) = encode_record(&intent, &work)?;
            plan_custody.absorb(&mut original_custody)?;
            let plan = acyclic_fs::MaterializationPlan {
                operation_id: operation,
                intent: acyclic_fs::MaterializationIntent::NativePreparation {
                    selection, original,
                },
                edits,
            };
            if retained.as_ref().is_some_and(|journal| journal.plan != plan) {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "native preparation operation was reused"));
            }
            if retained.is_none() {
                work.set_original_work(Default::default());
            }
            drop(retained);
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            materializer.backend().require_selected_root(&plan.edits, &preserved)?;
            let journal = materializer.prepare(plan, Some(plan_custody)).await.map_err(Error::from)?;
            require_allocation(&journal, &allocation)?;
            let restage = journal.phase == MaterializationPhase::Preparing
                && journal.preimages.is_empty();
            if restage {
                owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                materializer.backend().reset_target_directory("").await?;
                owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                for (binding, (generation, volume_work)) in bindings.iter().zip(&generations) {
                    if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) { continue; }
                    owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                    verify_binding(owner, binding, volume_work)?;
                    let identity = materializer.backend().reset_target_directory(binding.path)
                        .await?;
                    owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                    verify_binding(owner, binding, volume_work)?;
                    let (path, path_custody) = materializer.backend().staging_path(binding.path)?;
                    let staged = (
                        acyclic_fs::MaterializeOptions { expected_root_identity: Some(identity), ..acyclic_fs::MaterializeOptions::native(path) },
                        path_custody,
                    );
                    generation.materialize_with_native_work(
                        &staged.0, &volume_work, cancellation,
                    ).await?;
                    owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                    verify_binding(owner, binding, volume_work)?;
                }
            }
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            materializer.backend().require_selected_root(&journal.plan.edits, &preserved)?;
            drop(journal);
            let journal = {
                let selected = &generations;
                let namespace_root = &manifest.options.root;
                let mut before_effect = || async move {
                    owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                    for (binding, (_, volume_work)) in bindings.iter().zip(selected) {
                        verify_binding(owner, binding, volume_work)?;
                    }
                    Self::authenticate_lazy_roots(owner, bindings, existing_lazy, namespace_root, work)
                        .await?;
                    Ok::<(), Error>(())
                };
                materializer.recover(
                    operation, acyclic_fs::MaterializationRecovery::Complete, &mut before_effect,
                ).await
            }?
                .ok_or(boundary(crate::wire::ErrorCode::Conflict, "native preparation journal disappeared"))?;
            require_allocation(&journal, &allocation)?;
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            materializer.backend().require_selected_root(&journal.plan.edits, &preserved)?;
            let (mut volumes, mut volumes_custody) = allocate(&work, |io| crate::filesystem::fork_preparer_io::vector::<PreparedVolume<A, O>>(bindings.len(), Some(io)))?;
            output_custody.absorb(&mut volumes_custody)?;
            for (index, ((binding, mapping), (generation, volume_work))) in bindings.iter()
                .zip(&manifest.volumes).zip(generations).enumerate()
            {
                owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                verify_binding(owner, binding, &volume_work)?;
                if let Some(lazy) = existing_lazy.get(index).copied().flatten() {
                    let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else { unreachable!() };
                    if lazy.held.path() != mapping.path || lazy.held.snapshot(&volume_work)?.await? != *snapshot {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                    let identity = lazy.held.identity();
                    if volumes.iter().any(|volume| volume.original_identity == identity) {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                    volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::HeldLazy(Arc::clone(&lazy.held)),
                        original_identity: identity,
                        publication: binding.publication, _work: volume_work });
                    continue;
                }
                let callback_guard = guards[index]
                    .for_volume(owner, binding.volume, binding.writable, &volume_work)?;
                let (sdk, publication) = callback_guard;
                let identity = materializer.backend().mapping_root_identity(binding.path)?;
                if volumes.iter().any(|volume| volume.original_identity == identity) {
                    return Err(boundary(crate::wire::ErrorCode::Conflict, "native volume directory aliases changed"));
                }
                let mount = if binding.writable {
                    acyclic_fs::MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual)
                } else { acyclic_fs::MountOptions::read_only() };
                let (attachment_path, path_custody) = work.path(&mapping.path, None)?;
                let attachment = (acyclic_fs::MaterializeOptions::native(attachment_path), path_custody);
                let native_original = Some(&volume_work);
                let view = generation.attach_native_working_set(
                    &mount.native_context(&volume_work, Some(sdk), native_original),
                    &attachment.0, identity, manifest.options.work_per_volume, cancellation,
                ).await?;
                owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                verify_binding(owner, binding, &volume_work)?;
                volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::Materialized { view, callback: publication },
                    original_identity: identity,
                    publication: binding.publication, _work: volume_work });
            }
            drop(generations_custody);
            owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(&work)?)).await?;
            cancellation.check().map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            checkpoint_preparation(materializer.store(), journal).await?;
            let inspection = materializer.inspect(operation).await.map_err(Error::from)?
                .ok_or(boundary(crate::wire::ErrorCode::Conflict, "native preparation journal disappeared"))?;
            owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
            for (binding, volume) in bindings.iter().zip(&volumes) {
                verify_binding(owner, binding, &volume._work)?;
            }
            let mut receipt = preparation_receipt(intent, inspection, None)?;
            output_custody.absorb(&mut receipt._custody)?;
            receipt._custody = output_custody;
            drop(intent_operation_custody);
            Ok(Self { volumes, preparation: receipt })
        })
    }


    pub(super) async fn authenticate_lazy_roots<P: StreamProvider>(
        owner: &TaskJournalOwner<P>, bindings: &[BorrowedNativeVolumeBinding<'_, A, O>],
        existing_lazy: &[Option<&NativeExistingLazyVolume>], root: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
        if existing_lazy.is_empty() { return Ok(()); }
        for (index, binding) in bindings.iter().enumerate() {
            let Some(lazy) = existing_lazy.get(index).copied().flatten() else { continue; };
            verify_binding(owner, binding, work)?;
            let relative = lazy.held.path().strip_prefix(root)
                .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            if relative.as_os_str().is_empty() || relative != Path::new(binding.path) {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let expected = lazy.held.identity();
            let actual = lazy.held.original_work().root_identity(lazy.held.path())?;
            if actual != expected {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual }));
            }
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            };
            if lazy.held.snapshot(lazy.held.original_work())?.await? != *snapshot {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(work)?)).await
    }
}
