pub(super) struct CheckoutCandidate<A, O> {
    pub(super) base: Checkout<A, O>,
    pub(super) checkout: Checkout<A, O>,
}

/// Paths whose bindings a candidate was prepared to change.
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
            owned.push(copy_name(name.encoding(), name.as_bytes(), limits, self)?);
        }
        NamespacePath::new(owned, limits).map_err(MountSourceError::NamespacePath)
    }

    async fn lookup<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self,
        checkout: &Checkout<A, O>,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, MountSourceError> {
        let record = if let Some(work) = self.carrier.original_native_work() {
            checkout
                .inspector(&work, cancellation)
                .map_err(MountSourceError::Filesystem)?
                .lookup_no_follow(path, &work, cancellation)
                .await
                .map_err(MountSourceError::Filesystem)?
                .record
        } else {
            let (budget, live) = self.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            let result = checkout.inspector(budget, cancellation);
            let mut checkout = self
                .carrier
                .accept_with_prior_live(result, live, fs_error)?;
            let (budget, live) = self.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            let result = checkout.lookup_no_follow(path, budget, cancellation).await;
            self.carrier
                .accept_with_prior_live(result, live, fs_error)?
                .record
        };
        Ok(record)
    }
}

pub(super) async fn installed_changes<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    carrier: &mut NativeDemandCarrier<'_>,
    cancellation: &CancellationToken,
) -> Result<(ViewEffect, Option<crate::NativeAllocation>), MountSourceError> {
    let native = carrier.original_native_work();
    let (diff, allocation) = if let Some(work) = &native {
        base.candidate_diff(candidate, MAXIMUM_EXACT_INSTALL_CHANGES, work, cancellation)
            .await
            .map_err(MountSourceError::Filesystem)?
    } else {
        let (budget, live) = carrier.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(super::lazy::mount_demand_error)?,
                work.live_allocation_bytes(),
            ))
        })?;
        let result = base
            .candidate_diff(
                candidate,
                MAXIMUM_EXACT_INSTALL_CHANGES,
                budget,
                cancellation,
            )
            .await;
        carrier.accept_with_prior_live(result, live, fs_error)?
    };
    let mut inputs = InputBuffers::new(carrier)?;
    match allocation {
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut allocation) => inputs
            .allocation
            .as_mut()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?
            .absorb(&mut allocation)
            .map_err(MountSourceError::NativeWork)?,
        crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
            let bytes = (diff.files.capacity() * std::mem::size_of::<crate::FileRecordChange>()
                + diff.bindings.capacity() * std::mem::size_of::<crate::DirectoryBindingChange>())
                as u64
                + diff
                    .bindings
                    .iter()
                    .map(|binding| binding.name.as_bytes().len() as u64)
                    .sum::<u64>();
            inputs
                .carrier
                .with_work(|work| work.adopt(bytes))
                .map_err(super::lazy::mount_demand_error)?;
            inputs.bytes = bytes;
        }
    }
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
    let mut directories = inputs.vec::<(FileId, NamespacePath)>(diff.bindings.len())?;
    for path in scope.paths() {
        for length in 0..=path.depth() {
            let prefix = inputs
                .clone_namespace(&path.components()[..length], base.volume_config().limits)?;
            for checkout in [base, candidate] {
                if let Some(record) = inputs.lookup(checkout, &prefix, cancellation).await? {
                    let mut relevant = false;
                    for binding in &diff.bindings {
                        inputs.items(1)?;
                        if binding.directory_id == record.file_id {
                            relevant = true;
                            break;
                        }
                    }
                    let mut known = false;
                    for (file_id, _) in &directories {
                        inputs.items(1)?;
                        if *file_id == record.file_id {
                            known = true;
                            break;
                        }
                    }
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
            let bytes = if native.is_some() {
                prefix.retained_bytes()
            } else {
                super::lazy::mount_namespace_bytes(&prefix)
            };
            drop(prefix);
            inputs.release(bytes)?;
        }
    }
    for binding in diff.bindings {
        let name_bytes = if native.is_some() {
            binding.name.retained_bytes() as u64
        } else {
            binding.name.as_bytes().len() as u64
        };
        let history_bytes = if native.is_some() {
            binding
                .before
                .as_ref()
                .map_or(0, |entry| entry.name.retained_bytes() as u64)
                + binding
                    .after
                    .as_ref()
                    .map_or(0, |entry| entry.name.retained_bytes() as u64)
        } else {
            0
        };
        drop(binding.before);
        drop(binding.after);
        inputs.release(history_bytes)?;
        let mut directory = None;
        for (file_id, path) in &directories {
            inputs.items(1)?;
            if *file_id == binding.directory_id {
                directory = Some(path);
                break;
            }
        }
        match (directory, scope) {
            (Some(directory), _) => installed.names.push((
                inputs.clone_namespace(directory.components(), base.volume_config().limits)?,
                binding.name,
            )),
            (None, InstallScope::Subtree(root)) => {
                drop(binding.name);
                inputs.release(name_bytes)?;
                if installed.subtrees.is_empty() {
                    installed.subtrees = inputs.vec(1)?;
                    installed.subtrees.push(
                        inputs.clone_namespace(root.components(), base.volume_config().limits)?,
                    );
                }
            }
            (None, _) => {
                drop(binding.name);
                inputs.release(name_bytes)?;
                installed.everything = true;
            }
        }
    }
    inputs.release(binding_buffer_bytes)?;
    let bytes = (directories.capacity() * std::mem::size_of::<(FileId, NamespacePath)>()) as u64
        + directories
            .iter()
            .map(|(_, path)| {
                if native.is_some() {
                    path.retained_bytes()
                } else {
                    super::lazy::mount_namespace_bytes(path)
                }
            })
            .sum::<u64>();
    drop(directories);
    inputs.release(bytes)?;
    Ok((installed, inputs.retain()))
}
