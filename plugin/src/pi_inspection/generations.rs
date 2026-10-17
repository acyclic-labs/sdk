use super::*;

/// Constructed only by the actual selected frozen exactification/restore engines below.
pub(crate) struct PiWritebackGenerations<'a> {
    pub(super) from: crate::LocalGeneration,
    pub(super) candidate: crate::LocalGeneration,
    pub(crate) original_snapshot: acyclic_fs::LazySnapshotRef,
    pub(crate) imported: acyclic_fs::GenerationId,
    pub(super) operation: acyclic_harness::OperationId,
    pub(super) paths: &'a [String],
}

pub(crate) async fn build_writeback_generations<'a>(
    root: &std::sync::Arc<PiWritebackRoot>,
    actual_root: &crate::pi_root_provider::PiRootHandle,
    original: acyclic_fs::LazySnapshotRef,
    imported: &crate::LocalGeneration,
    paths: &'a [String],
    operation: acyclic_harness::OperationId,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<PiWritebackGenerations<'a>> {
    root.verify(native_work).await?;
    let mut grant_io = HistoryWork::new(PreparationIo::from_native_work(native_work)?);
    let storage_name = {
        let mut io = HistoryWork::new(PreparationIo::from_native_work(native_work)?);
        let value = root.volume.storage_name(&mut Some(&mut io))?;
        (value, io.into_custody()?)
    };
    if original.workspace_id != root.workspace
        || imported.workspace_id() != root.workspace
        || actual_root.workspace.workspace().id() != root.workspace
        || actual_root.workspace.workspace().name().as_str() != storage_name.0
        || actual_root.physical.source.root_identity() != root.identity
        || paths.is_empty()
        || paths.len() > MAX_PRIVATE_DIRECTORY_PAGE
    {
        return Err(selected_unauthorized(
            "writeback baseline is not the ORIGINAL creator's frozen native root",
        ));
    }
    let (absolute, custody) = crate::pi_native::pi_native_vec(paths.len(), native_work)?;
    let mut absolute_paths = (absolute, custody);
    for path in paths {
        validate_content_path(path, &mut Some(&mut grant_io))?;
        let (path, mut custody) = native_work.display(&format_args!("/{path}"))?;
        absolute_paths.1.absorb(&mut custody)?;
        absolute_paths.0.push(path);
    }
    absolute_paths.0.sort_unstable();
    if absolute_paths
        .0
        .windows(2)
        .any(|paths| matches!(paths, [left, right] if left == right))
    {
        return Err(selected_invalid("writeback repeats a selected path"));
    }
    let destination = native_work.display(&format_args!("pi-writeback-{operation}-baseline"))?;
    let baseline_key = acyclic_fs::IdempotencyKey::from_bytes(
        phase_operation(operation, b"pi-writeback-baseline").into_bytes(),
    );
    let candidate_key = acyclic_fs::IdempotencyKey::from_bytes(
        phase_operation(operation, b"pi-writeback-candidate").into_bytes(),
    );
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work: native_work,
        cancellation: native_work.cancellation(),
    };
    let (baseline, _) = actual_root
        .workspace
        .fork_snapshot(
            &destination.0,
            original,
            acyclic_fs::IdempotencyKey::from_bytes(
                phase_operation(operation, b"pi-writeback-fork").into_bytes(),
            ),
            acyclic_fs::PublicationPermit::Unrestricted,
            &mut accounting,
        )
        .await
        .map_err(crate::pi_fork_host::native_lazy_error)?;
    let workspace = baseline.workspace();
    let mut guard = {
        let root = std::sync::Arc::clone(root);
        move |work: &acyclic_fs::NativeMaterializationWork| {
            let root = std::sync::Arc::clone(&root);
            let work = work.clone();
            async move { root.verify(&work).await }
        }
    };
    let from = if let Some(generation) = workspace
        .operation_generation(baseline_key, native_work, native_work.cancellation())
        .await?
    {
        generation
    } else {
        for absolute in &absolute_paths.0 {
            root.verify(native_work).await?;
            let found = baseline.lookup(absolute, &mut accounting).await;
            match found {
                Ok(lookup) => {
                    let directory =
                        lookup.0.stat().kind == acyclic_fs::demand::SourceNodeKind::Directory;
                    baseline
                        .retire_lookup(lookup, &mut accounting)
                        .map_err(crate::pi_fork_host::native_lazy_error)?;
                    if directory {
                        baseline
                            .exactify(
                                absolute,
                                acyclic_fs::PublicationPermit::Unrestricted,
                                &mut accounting,
                            )
                            .await
                            .map_err(crate::pi_fork_host::native_lazy_error)?;
                    } else {
                        let committed = baseline
                            .promote(
                                absolute,
                                native_work.remaining()?.source_bytes_read,
                                acyclic_fs::IdempotencyKey::from_bytes(
                                    phase_operation(operation, absolute.as_bytes()).into_bytes(),
                                ),
                                acyclic_fs::PublicationPermit::Unrestricted,
                                &mut accounting,
                            )
                            .await
                            .map_err(crate::pi_fork_host::native_lazy_error)?;
                        if !matches!(
                            committed,
                            acyclic_fs::TransactionCommit::Committed(_)
                                | acyclic_fs::TransactionCommit::AlreadyCommitted(_)
                        ) {
                            return Err(selected_invalid(
                                "selected baseline publication conflicted",
                            ));
                        }
                    }
                }
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => {}
                Err(error) => return Err(crate::pi_fork_host::native_lazy_error(error)),
            }
        }
        let mut transaction = workspace
            .begin_transaction(baseline_key, native_work, native_work.cancellation())
            .await?;
        match transaction
            .commit_with_native_work::<Error, _, _>(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                native_work,
                native_work.cancellation(),
                &mut guard,
            )
            .await?
        {
            acyclic_fs::TransactionCommit::Committed(generation)
            | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => return Err(selected_invalid("selected baseline publication conflicted")),
        }
    };
    let candidate = if let Some(generation) = workspace
        .operation_generation(candidate_key, native_work, native_work.cancellation())
        .await?
    {
        generation
    } else {
        match workspace
            .restore_paths_from_with_native_work::<Error, _, _>(
                imported,
                &absolute_paths.0,
                from.id(),
                candidate_key,
                acyclic_fs::PublicationPermit::Unrestricted,
                native_work,
                native_work.cancellation(),
                &mut guard,
            )
            .await?
        {
            acyclic_fs::TransactionCommit::Committed(generation)
            | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => {
                return Err(selected_invalid(
                    "selected candidate publication conflicted",
                ));
            }
        }
    };
    root.verify(native_work).await?;
    Ok(PiWritebackGenerations {
        from,
        candidate,
        original_snapshot: original,
        imported: imported.id(),
        operation,
        paths,
    })
}

fn phase_operation(
    operation: acyclic_harness::OperationId,
    phase: &[u8],
) -> acyclic_harness::OperationId {
    use sha2::Digest as _;
    let mut hash = sha2::Sha256::new();
    hash.update(phase);
    hash.update(operation.into_bytes());
    let digest = hash.finalize();
    let mut bytes = [0; 16];
    for (target, byte) in bytes.iter_mut().zip(digest) {
        *target = byte;
    }
    acyclic_harness::OperationId::from_bytes(bytes)
}
