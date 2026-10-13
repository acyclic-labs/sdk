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
        || actual_root.physical.source.inner().root_identity() != root.identity
        || paths.is_empty()
        || paths.len() > MAX_PRIVATE_DIRECTORY_PAGE
    {
        return Err(selected_unauthorized(
            "writeback baseline is not the ORIGINAL creator's frozen native root",
        ));
    }
    let (names, mut names_custody) =
        crate::pi_native::pi_native_vec::<&str>(paths.len(), native_work)?;
    let (absolute, mut custody) = crate::pi_native::pi_native_vec(paths.len(), native_work)?;
    custody.absorb(&mut names_custody)?;
    let mut absolute_paths = (absolute, names, custody);
    absolute_paths.1.extend(paths.iter().map(String::as_str));
    absolute_paths.1.sort_unstable();
    if absolute_paths.1.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(selected_invalid("writeback repeats a selected path"));
    }
    for path in paths {
        validate_content_path(path, &mut Some(&mut grant_io))?;
        let mut absolute = native_work.display(&format_args!("/{path}"))?;
        absolute_paths.2.absorb(&mut absolute.1)?;
        absolute_paths.0.push(absolute.0);
    }
    let destination = native_work.display(&format_args!("pi-writeback-{operation}-baseline"))?;
    let publication_guard = std::sync::Arc::clone(root);
    let (from, candidate) = actual_root
        .workspace
        .exactify_snapshot_selected_at_generation_with_native_work_guarded(
            original,
            imported,
            &absolute_paths.0,
            &destination.0,
            acyclic_fs::IdempotencyKey::from_bytes(
                phase_operation(operation, b"pi-writeback-baseline").into_bytes(),
            ),
            acyclic_fs::IdempotencyKey::from_bytes(
                phase_operation(operation, b"pi-writeback-candidate").into_bytes(),
            ),
            acyclic_fs::PublicationPermit::Unrestricted,
            native_work,
            native_work.cancellation(),
            move |original: &acyclic_fs::NativeMaterializationWork| {
                let publication_guard = std::sync::Arc::clone(&publication_guard);
                let original = original.clone();
                async move { publication_guard.verify(&original).await }
            },
        )
        .await?;
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
    bytes.copy_from_slice(&digest[..16]);
    acyclic_harness::OperationId::from_bytes(bytes)
}
