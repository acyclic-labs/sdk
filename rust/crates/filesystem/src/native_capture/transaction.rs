use super::*;

/// Queues a remove for every descendant of `root` that the checkout holds, deepest first, so that a following remove of `root` itself finds it empty.
pub(super) async fn push_subtree_removals<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: &NamespacePath,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    const PAGE: u32 = 256;
    let limits = checkout.volume_config().limits;
    // Descendants that carried their own hint are already queued (absent paths are captured deepest first); a second remove of the same path would find its source missing.
    let mut queued = CaptureMap::new();
    for mutation in mutations.iter() {
        {
            capture_charge(
                meter,
                WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
            )
        }?;
        if let AuthoredMutation::Remove { path, .. } = mutation
            && !queued.contains_key_measured(path, meter)?
        {
            let path = clone_capture_path(path, meter)?;
            queued.insert_measured(path, (), meter)?;
        }
    }
    let mut found: Vec<(NamespacePath, FileRecord)> = Vec::new();
    let root = clone_capture_path(root, meter)?;
    walk_checkout_subtree(checkout, root, PAGE, meter, |directory, entry, pending| {
        let name = clone_capture_name(&entry.name, meter)?;
        let child = append_path_measured(directory, name, limits, meter)?;
        if entry.record.kind == FileKind::Directory {
            capture_reserve(pending, 1, meter)?;
            pending.push(clone_capture_path(&child, meter)?);
        }
        if !queued.contains_key_measured(&child, meter)? {
            capture_reserve(&mut found, 1, meter)?;
            found.push((child, entry.record));
        } else {
            capture_drop_path(child, meter)?;
        }
        Ok(())
    })
    .await?;
    release_capture_set(queued, meter)?;
    {
        capture_sort_by(
            &mut found,
            |left, right| right.0.depth().cmp(&left.0.depth()),
            meter,
        )
    }?;
    capture_reserve(mutations, found.len(), meter)?;
    let found_bytes =
        found.capacity() as u64 * std::mem::size_of::<(NamespacePath, FileRecord)>() as u64;
    for (path, record) in found {
        mutations.push(AuthoredMutation::Remove {
            path,
            expected_file_id: Some(record.file_id),
        });
        receipt.changed_paths = checked_increment(receipt.changed_paths)?;
    }
    meter
        .native
        .clone()
        .release(found_bytes)
        .map_err(capture_demand_error)?;
    Ok(())
}

pub(super) fn capture_mutation_owned_bytes(
    mutations: &[AuthoredMutation],
    storage_bytes: u64,
    meter: &CaptureWorkScope,
) -> Result<u64, CaptureError> {
    let mut bytes = storage_bytes;
    for mutation in mutations {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        let (path, second) = match mutation {
            AuthoredMutation::Rename {
                source,
                destination,
                ..
            }
            | AuthoredMutation::HardLink {
                source,
                destination,
            } => (source, Some(destination)),
            AuthoredMutation::CloneRange(request) => (&request.source, Some(&request.destination)),
            AuthoredMutation::Reidentify { path, .. }
            | AuthoredMutation::CreateFile { path, .. }
            | AuthoredMutation::Write { path, .. }
            | AuthoredMutation::CreateSymbolicLink { path, .. }
            | AuthoredMutation::CreateReparsePoint { path, .. }
            | AuthoredMutation::CreateFileFromContent { path, .. }
            | AuthoredMutation::CreateDirectory { path, .. }
            | AuthoredMutation::CreateEmptySpecial { path, .. }
            | AuthoredMutation::CreateDevice { path, .. }
            | AuthoredMutation::Remove { path, .. }
            | AuthoredMutation::WriteFromContent { path, .. }
            | AuthoredMutation::SetMetadata { path, .. }
            | AuthoredMutation::Resize { path, .. }
            | AuthoredMutation::ZeroRange { path, .. }
            | AuthoredMutation::Preallocate { path, .. } => (path, None),
        };
        capture_charge(
            meter,
            WorkCounters {
                source_path_components: path.depth() as u64
                    + second.map_or(0, |path| path.depth() as u64),
                ..WorkCounters::UNCHARGED
            },
        )?;
        bytes = bytes
            .checked_add(mutation.logical_owned_bytes())
            .ok_or(CaptureError::from(WorkError::Overflow))?;
    }
    Ok(bytes)
}

pub(super) async fn apply_capture_transaction<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    mutations: Vec<AuthoredMutation>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let mutation_bytes = {
        capture_mutation_owned_bytes(
            &mutations,
            mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64,
            meter,
        )
    }?;
    if mutations.is_empty() {
        drop(mutations);
        meter
            .native
            .clone()
            .release(mutation_bytes)
            .map_err(capture_demand_error)?;
        return Ok(());
    }
    let input = (
        mutations,
        meter
            .native
            .transfer_capture_allocation(
                mutation_bytes,
                meter.custody.as_ref().expect("capture scope is live"),
            )
            .map_err(CaptureError::from)?,
    );
    let apply_cancellation = meter
        .native
        .with_original_leaf(|work| work.cancellation().clone());
    async {
        // Preflight abandonment drops mutation data before its guard.
        let input = input;
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(&meter.native, &apply_cancellation);
        checkout
            .apply_authored_bulk_transaction_with_accounting(
                crate::kernel::Owned {
                    value: input.0,
                    bytes: input.1.bytes(),
                    allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(
                        input.1,
                    ),
                },
                &mut accounting,
                &apply_cancellation,
            )
            .await
    }
    .await?;
    Ok(())
}

pub(super) fn checked_increment(value: u64) -> Result<u64, CaptureError> {
    value
        .checked_add(1)
        .ok_or(CaptureError::from(WorkError::Overflow))
}

pub(super) fn append_hard_link(
    source: NamespacePath,
    destination: NamespacePath,
    current: Option<FileRecord>,
    mutations: &mut Vec<AuthoredMutation>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    {
        capture_reserve(mutations, 1 + usize::from(current.is_some()), meter)?;
        if let Some(record) = current {
            mutations.push(AuthoredMutation::Remove {
                path: clone_capture_path(&destination, meter)?,
                expected_file_id: Some(record.file_id),
            });
        }
        mutations.push(AuthoredMutation::HardLink {
            source,
            destination,
        });
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn append_final_state(
    path: NamespacePath,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    metadata: &HostStat,
    snapshot: HostSnapshot,
    kind: FileKind,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    mutations: &mut Vec<AuthoredMutation>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    capture_reserve(mutations, if exists_with_kind { 2 } else { 1 }, meter)?;
    match kind {
        FileKind::Regular => unreachable!("regular files are staged after preparation"),
        FileKind::Directory => mutations.push(if exists_with_kind {
            AuthoredMutation::SetMetadata {
                path,
                metadata: canonical_metadata,
            }
        } else {
            AuthoredMutation::CreateDirectory {
                path,
                metadata: canonical_metadata,
            }
        }),
        FileKind::SymbolicLink => {
            let target = read_link_bytes(source_root, host_path, meter)?;
            if exists_with_kind {
                mutations.push(AuthoredMutation::Remove {
                    path: clone_capture_path(&path, meter)?,
                    expected_file_id: None,
                });
            }
            let target = capture_link_target(target, meter)?;
            mutations.push(AuthoredMutation::CreateSymbolicLink {
                path,
                target: target.into(),
                metadata: canonical_metadata,
            });
        }
        FileKind::Fifo | FileKind::Socket => mutations.push(if exists_with_kind {
            AuthoredMutation::SetMetadata {
                path,
                metadata: canonical_metadata,
            }
        } else {
            AuthoredMutation::CreateEmptySpecial {
                path,
                kind,
                metadata: canonical_metadata,
            }
        }),
        FileKind::CharacterDevice | FileKind::BlockDevice => {
            let (major, minor) = host_device_identity(metadata)?;
            if exists_with_kind {
                mutations.push(AuthoredMutation::Remove {
                    path: clone_capture_path(&path, meter)?,
                    expected_file_id: None,
                });
            }
            mutations.push(AuthoredMutation::CreateDevice {
                path,
                kind,
                major,
                minor,
                metadata: canonical_metadata,
            });
        }
        FileKind::ReparsePoint | FileKind::MountBoundary => {
            return Err(CaptureError::UnsupportedKind);
        }
    }
    let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
    ensure_same_host_node(&snapshot, &observed)?;
    meter.native.clone().check().map_err(capture_demand_error)
}
