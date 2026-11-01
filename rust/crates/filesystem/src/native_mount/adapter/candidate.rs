pub(super) struct CheckoutCandidate<'a, A: 'static, O: 'static> {
    pub(super) base: Checkout<A, O>,
    pub(super) checkout: Checkout<A, O>,
    pub(super) operation: Option<OperationId>,
    pub(super) deadline: Option<u64>,
    pub(super) epoch: u64,
    pub(super) stamp: ViewStamp,
    _publication: tokio::sync::RwLockReadGuard<'a, ()>,
    // Unlock before retiring the registry lease.
    _file_mutation: Option<(
        tokio::sync::OwnedMutexGuard<crate::NativeAllocation>,
        FileMutationLease<'a>,
    )>,
}

#[derive(Clone, Copy)]
pub(super) enum InstallScope<'a> {
    Paths(&'a [NamespacePath]),
    Subtree(&'a NamespacePath),
    CombinedPaths(&'a [NamespacePath], &'a [NamespacePath]),
}

impl InstallScope<'_> {
    fn paths(&self) -> impl Iterator<Item = &NamespacePath> {
        let (first, second) = match self {
            Self::Paths(paths) => (*paths, &[][..]),
            Self::Subtree(root) => (std::slice::from_ref(*root), &[][..]),
            Self::CombinedPaths(first, second) => (*first, *second),
        };
        first.iter().chain(second)
    }
}

impl InputBuffers<'_, '_> {
    pub(super) fn clone_namespace(
        &mut self,
        names: &[LogicalName],
        limits: VolumeLimits,
    ) -> Result<NamespacePath, MountSourceError> {
        let mut owned = self.vec(names.len())?;
        for name in names {
            self.items(1)?;
            owned.push(copy_name(name.encoding(), name.as_bytes(), self)?);
        }
        NamespacePath::new(owned, limits).map_err(MountSourceError::NamespacePath)
    }
}

pub(super) async fn installed_changes<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    carrier: &mut NativeDemandCarrier<'_>,
    cancellation: &CancellationToken,
) -> Result<(ViewEffect, Option<crate::NativeAllocation>), MountSourceError> {
    let native = carrier
        .original_native_work()
        .ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout,
        ))?;
    let (diff, allocation) = base
        .candidate_diff(candidate, None, &native, cancellation)
        .await?;
    let mut inputs = InputBuffers::new(carrier)?;
    let crate::kernel::KernelOwnedAllocation::NativeOriginal(mut allocation) = allocation else {
        unreachable!("native candidate diff");
    };
    inputs
        .allocation
        .as_mut()
        .ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout,
        ))?
        .absorb(&mut allocation)
        .map_err(MountSourceError::NativeWork)?;
    let file_buffer_bytes =
        (diff.files.capacity() * std::mem::size_of::<crate::FileRecordChange>()) as u64;
    let binding_buffer_bytes =
        (diff.bindings.capacity() * std::mem::size_of::<crate::DirectoryBindingChange>()) as u64;
    let mut installed = ViewEffect {
        nodes: inputs.vec(diff.files.len())?,
        names: inputs.vec(diff.bindings.len())?,
        ..ViewEffect::default()
    };
    for change in diff.files {
        inputs.items(1)?;
        installed.nodes.push(change.file_id);
    }
    inputs.release(file_buffer_bytes)?;
    let directories = installed_directories(
        base,
        candidate,
        scope,
        &diff.bindings,
        &mut inputs,
        &native,
        cancellation,
    )
    .await?;
    for binding in diff.bindings {
        install_binding_change(
            binding,
            &directories,
            scope,
            base.volume_config().limits,
            &mut installed,
            &mut inputs,
        )?;
    }
    inputs.release(binding_buffer_bytes)?;
    let bytes = (directories.paths.capacity() * std::mem::size_of::<(FileId, NamespacePath)>())
        as u64
        + directories
            .paths
            .iter()
            .map(|(_, path)| path.retained_bytes())
            .sum::<u64>();
    drop(directories);
    inputs.release(bytes)?;
    Ok((installed, inputs.retain()))
}

struct InstalledDirectories {
    paths: Vec<(FileId, NamespacePath)>,
    index: crate::kernel::allocation::fixed_index::FixedIndex<Option<usize>>,
}

async fn installed_directories<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    bindings: &[crate::DirectoryBindingChange],
    inputs: &mut InputBuffers<'_, '_>,
    native: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<InstalledDirectories, MountSourceError> {
    let mut directories = inputs.vec::<(FileId, NamespacePath)>(bindings.len())?;
    let mut index = crate::kernel::allocation::fixed_index::FixedIndex::default();
    let mut accounting = crate::kernel::KernelAccounting::native(native, cancellation);
    for binding in bindings {
        index.get_or_try_insert_with(binding.directory_id.into_bytes(), &mut accounting, |_| {
            Ok(None)
        })?;
    }
    for path in scope.paths() {
        for length in 0..=path.depth() {
            let prefix = inputs.clone_namespace(
                path.components()
                    .get(..length)
                    .ok_or(MountSourceError::Work(WorkError::Overflow))?,
                base.volume_config().limits,
            )?;
            for checkout in [base, candidate] {
                if let Some(record) = checkout
                    .inspector(
                        &mut crate::kernel::allocation::KernelAccounting::native(
                            native,
                            cancellation,
                        ),
                        cancellation,
                    )?
                    .lookup_no_follow(&prefix, native, cancellation)
                    .await?
                    .record
                {
                    let key = record.file_id.into_bytes();
                    if matches!(index.get(key, &mut accounting)?, Some(None)) {
                        let path = inputs
                            .clone_namespace(prefix.components(), base.volume_config().limits)?;
                        let slot = directories.len();
                        let copied = crate::WorkCounters {
                            bytes_copied: (size_of::<(FileId, NamespacePath)>()
                                + size_of::<Option<usize>>())
                                as u64,
                            ..crate::WorkCounters::UNCHARGED
                        };
                        accounting.admit(copied)?;
                        let entry =
                            index.get_or_try_insert_with(key, &mut accounting, |_| Ok(None))?;
                        directories.push((record.file_id, path));
                        *entry = Some(slot);
                        accounting.record_completed(copied)?;
                    }
                }
            }
            let bytes = prefix.retained_bytes();
            drop(prefix);
            inputs.release(bytes)?;
        }
    }
    Ok(InstalledDirectories {
        paths: directories,
        index,
    })
}

fn install_binding_change(
    binding: crate::DirectoryBindingChange,
    directories: &InstalledDirectories,
    scope: InstallScope<'_>,
    limits: VolumeLimits,
    installed: &mut ViewEffect,
    inputs: &mut InputBuffers<'_, '_>,
) -> Result<(), MountSourceError> {
    let name_bytes = binding.name.retained_bytes() as u64;
    let history_bytes = binding
        .before
        .as_ref()
        .map_or(0, |entry| entry.name.retained_bytes() as u64)
        + binding
            .after
            .as_ref()
            .map_or(0, |entry| entry.name.retained_bytes() as u64);
    drop(binding.before);
    drop(binding.after);
    inputs.release(history_bytes)?;
    let native = inputs
        .carrier
        .original_native_work()
        .ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout,
        ))?;
    let mut accounting = crate::kernel::KernelAccounting::native(&native, native.cancellation());
    let directory = directories
        .index
        .get(binding.directory_id.into_bytes(), &mut accounting)?
        .copied()
        .flatten()
        .and_then(|slot| directories.paths.get(slot))
        .map(|(_, path)| path);
    match (directory, scope) {
        (Some(directory), _) => installed.names.push((
            inputs.clone_namespace(directory.components(), limits)?,
            binding.name,
        )),
        (None, InstallScope::Subtree(root)) => {
            drop(binding.name);
            inputs.release(name_bytes)?;
            if installed.subtrees.is_empty() {
                installed.subtrees = inputs.vec(1)?;
                installed
                    .subtrees
                    .push(inputs.clone_namespace(root.components(), limits)?);
            }
        }
        (None, _) => {
            drop(binding.name);
            inputs.release(name_bytes)?;
            installed.everything = true;
        }
    }
    Ok(())
}
