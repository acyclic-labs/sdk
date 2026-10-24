pub(super) struct CheckoutCandidate<'a, A, O> {
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
    fn find_charged<T>(
        &mut self,
        entries: impl IntoIterator<Item = T>,
        mut matches: impl FnMut(&T) -> bool,
    ) -> Result<Option<T>, MountSourceError> {
        for entry in entries {
            self.items(1)?;
            if matches(&entry) {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    pub(super) fn clone_namespace(
        &mut self,
        names: &[LogicalName],
        limits: VolumeLimits,
    ) -> Result<NamespacePath, MountSourceError> {
        let mut owned = self.vec(names.len())?;
        for name in names {
            self.items(1)?;
            owned.push(copy_name(name.encoding(), name.as_bytes(), limits, self)?);
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
        .candidate_diff(
            candidate,
            MAXIMUM_EXACT_INSTALL_CHANGES,
            &native,
            cancellation,
        )
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
    if diff.truncated {
        drop(diff);
        return Ok((
            ViewEffect {
                everything: true,
                ..ViewEffect::default()
            },
            None,
        ));
    }
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
    let bytes = (directories.capacity() * std::mem::size_of::<(FileId, NamespacePath)>()) as u64
        + directories
            .iter()
            .map(|(_, path)| path.retained_bytes())
            .sum::<u64>();
    drop(directories);
    inputs.release(bytes)?;
    Ok((installed, inputs.retain()))
}

async fn installed_directories<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    bindings: &[crate::DirectoryBindingChange],
    inputs: &mut InputBuffers<'_, '_>,
    native: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<Vec<(FileId, NamespacePath)>, MountSourceError> {
    let mut directories = inputs.vec::<(FileId, NamespacePath)>(bindings.len())?;
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
                    let relevant = inputs
                        .find_charged(bindings, |binding| binding.directory_id == record.file_id)?
                        .is_some();
                    let known = inputs
                        .find_charged(directories.iter(), |(file_id, _)| {
                            *file_id == record.file_id
                        })?
                        .is_some();
                    if relevant && !known {
                        directories.push((
                            record.file_id,
                            inputs.clone_namespace(
                                prefix.components(),
                                base.volume_config().limits,
                            )?,
                        ));
                    }
                }
            }
            let bytes = prefix.retained_bytes();
            drop(prefix);
            inputs.release(bytes)?;
        }
    }
    Ok(directories)
}

fn install_binding_change(
    binding: crate::DirectoryBindingChange,
    directories: &[(FileId, NamespacePath)],
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
    let directory = inputs
        .find_charged(directories.iter(), |(file_id, _)| {
            *file_id == binding.directory_id
        })?
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
