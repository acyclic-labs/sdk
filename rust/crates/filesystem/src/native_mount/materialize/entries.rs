use super::*;

type KnownFileLinks = Vec<(FileId, HeldHostPath)>;
type PendingDirectoryMetadata = Vec<(HeldNamespacePath, HeldHostPath)>;
#[cfg(any(target_os = "macos", windows))]
type KnownPayload = ((u64, ObjectId, ObjectId), HeldHostPath);

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one entry needs the reader, host root, both paths, its record, options, budget, cancellation, and the shared link and pending maps"
)]
pub(super) async fn materialize_entry<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<(HostRoot, crate::NativeAllocation)>,
    path: &NamespacePath,
    host_path: &HeldHostPath,
    directory: Option<HeldNamespacePath>,
    record: &crate::kernel::FileRecord,
    options: &MaterializeOptions,
    cancellation: &CancellationToken,
    known_files: &mut KnownFileLinks,
    #[cfg(any(target_os = "macos", windows))] known_payloads: &mut Vec<KnownPayload>,
    pending: &mut PendingDirectoryMetadata,
    receipt: &mut GenerationReceipt,
    mode: MaterializeMode,
    before_effect: &mut Option<&mut F>,
) -> Result<(), E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let mut work = receipt.custody.work().clone();
    let crate::kernel::FileRecord {
        file_id,
        kind,
        payload,
        metadata: _metadata_id,
        ..
    } = *record;
    receipt
        .custody
        .work()
        .with_original_leaf(|work| {
            work.check()?;
            work.work.admit(
                &WorkCounters {
                    materializations: 1,
                    ..WorkCounters::UNCHARGED
                },
                &work.budget,
            )?;
            Ok::<_, DemandError>(())
        })
        .map_err(MaterializeError::from)?;
    for (identity, existing) in known_files.iter() {
        receipt
            .custody
            .work()
            .charge(crate::materializer::NativeResourceCredit::Items.delta(1))
            .map_err(MaterializeError::from)?;
        if *identity == file_id {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            host_root
                .0
                .hard_link(
                    #[cfg(unix)]
                    super::work::host_path(&existing.0),
                    #[cfg(windows)]
                    &existing.0,
                    #[cfg(unix)]
                    super::work::host_path(&host_path.0),
                    #[cfg(windows)]
                    &host_path.0,
                    #[cfg(unix)]
                    &mut &work,
                    #[cfg(windows)]
                    &mut work,
                )
                .map_err(MaterializeError::from)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.files = receipt
                .value
                .files
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt
                .custody
                .work()
                .admit(WorkCounters::UNCHARGED)
                .map_err(MaterializeError::from)?;
            return Ok(());
        }
    }
    let mut authenticated_metadata = None;
    match (kind, payload) {
        (FileKind::Directory, FilePayload::Directory { .. }) => {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            {
                #[cfg(unix)]
                {
                    host_root
                        .0
                        .create_dir(super::work::host_path(&host_path.0), &mut work)
                }
                #[cfg(windows)]
                {
                    host_root.0.create_dir(&host_path.0, &mut work)
                }
            }
            .map_err(MaterializeError::from)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            let directory = directory.ok_or(MaterializeError::InvalidOptions)?;
            receipt.value.directories = receipt
                .value
                .directories
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
            receipt.push(pending, (directory, host_path.clone()))?;
            receipt
                .custody
                .work()
                .admit(WorkCounters::UNCHARGED)
                .map_err(MaterializeError::from)?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            let file = create_file(&host_root.0, &host_path.0, receipt)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            let length = data.as_bytes().len();
            let original = receipt.custody.work().clone();
            let (bytes, allocation) = original
                .copy_bytes(data.as_bytes())
                .map_err(MaterializeError::from)?;
            let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
                bytes,
                allocation,
                original.runtime_meter(),
            )
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    original.runtime_error(error)
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    crate::NativeTreeMaterializationError::InvalidLayout
                }
            });
            let bytes = bytes.map_err(MaterializeError::from)?;
            let written =
                write_generation_bytes(&file, 0, bytes, 0, receipt, before_effect).await?;
            if mode == MaterializeMode::DurableOutput {
                sync_generation_file(&file, receipt, before_effect).await?;
            }
            drop(file);
            account_file(receipt, length as u64, written)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (
            FileKind::Regular,
            FilePayload::Regular {
                logical_bytes,
                extents,
            },
        ) => {
            #[cfg(not(any(target_os = "macos", windows)))]
            let _ = extents;
            #[cfg(any(target_os = "macos", windows))]
            let (cloned, payload_known) = clone_known_payload(
                &host_root.0,
                &host_path.0,
                (logical_bytes, extents, _metadata_id),
                known_payloads,
                receipt,
                before_effect,
            )
            .await?;
            #[cfg(not(any(target_os = "macos", windows)))]
            let cloned = false;
            if !cloned {
                generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
                let mut file = create_file(&host_root.0, &host_path.0, receipt)?;
                generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                    .await?;
                #[cfg(windows)]
                {
                    generation_action(before_effect, NativeMaterializationActionPhase::Before)
                        .await?;
                    let meter = receipt.custody.work().runtime_meter();
                    let control = file
                        .control_async(
                            |file, meter| {
                                mark_sparse(file, meter.ok_or(std::io::ErrorKind::InvalidInput)?)
                            },
                            Some(meter),
                        )
                        .map_err(|error| receipt.runtime_error(error))?;
                    control
                        .await
                        .map_err(|error| receipt.runtime_error(error))?;
                    generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                        .await?;
                }
                generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
                let meter = receipt.custody.work().runtime_meter();
                let resize = file
                    .set_len_async(logical_bytes, Some(meter))
                    .map_err(|error| receipt.runtime_error(error))?;
                resize.await.map_err(|error| receipt.runtime_error(error))?;
                generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                    .await?;
                authenticated_metadata = Some(
                    materialize_sparse_file(
                        reader,
                        path,
                        &mut file,
                        logical_bytes,
                        options,
                        cancellation,
                        receipt,
                        before_effect,
                    )
                    .await?,
                );
                if mode == MaterializeMode::DurableOutput {
                    sync_generation_file(&file, receipt, before_effect).await?;
                }
                drop(file);
            }
            #[cfg(any(target_os = "macos", windows))]
            if cloned && mode == MaterializeMode::DurableOutput {
                #[cfg(target_os = "macos")]
                let file = host_root
                    .0
                    .open_file(super::work::host_path(&host_path.0), &mut work)
                    .map_err(MaterializeError::from)?
                    .into_std();
                #[cfg(windows)]
                let file = host_root
                    .0
                    .open_file_for_sync(&host_path.0, &mut work)
                    .map_err(MaterializeError::from)?
                    .into_std();
                let meter = receipt.custody.work().runtime_meter();
                let file = acyclic_native_runtime::NativeFile::from_file(
                    file,
                    Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)),
                )
                .map_err(|(error, file)| {
                    drop(file);
                    receipt.runtime_error(error)
                })?;
                sync_generation_file(&file, receipt, before_effect).await?;
                drop(file);
            }
            account_file(receipt, logical_bytes, 0)?;
            #[cfg(any(target_os = "macos", windows))]
            if !payload_known {
                receipt.push(
                    known_payloads,
                    ((logical_bytes, extents, _metadata_id), host_path.clone()),
                )?;
            }
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (FileKind::SymbolicLink, FilePayload::SymbolicLink { target_bytes, .. }) => {
            materialize_symlink(
                (reader, cancellation),
                path,
                &host_root.0,
                &host_path.0,
                target_bytes,
                receipt,
                before_effect,
            )
            .await?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (
            kind @ (FileKind::Fifo
            | FileKind::Socket
            | FileKind::CharacterDevice
            | FileKind::BlockDevice),
            payload,
        ) => {
            materialize_special(
                &host_root.0,
                &host_path.0,
                kind,
                payload,
                receipt,
                before_effect,
            )
            .await?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (unsupported, _) => return Err(MaterializeError::UnsupportedKind(unsupported).into()),
    }
    apply_metadata(
        reader,
        host_root,
        path,
        host_path,
        authenticated_metadata,
        receipt,
        before_effect,
    )
    .await?;
    receipt
        .custody
        .work()
        .admit(WorkCounters::UNCHARGED)
        .map_err(MaterializeError::from)?;
    Ok(())
}

#[cfg(any(target_os = "macos", windows))]
async fn clone_known_payload<E, F, Fut>(
    host_root: &HostRoot,
    host_path: &[HostPathUnit],
    payload: (u64, ObjectId, ObjectId),
    known_payloads: &[KnownPayload],
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(bool, bool), E>
where
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let mut work = receipt.custody.work().clone();
    let mut cloned = false;
    let mut payload_known = false;
    for (key, source) in known_payloads {
        receipt
            .custody
            .work()
            .charge(crate::materializer::NativeResourceCredit::Items.delta(1))
            .map_err(MaterializeError::from)?;
        if *key == payload {
            payload_known = true;
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            cloned = {
                #[cfg(target_os = "macos")]
                {
                    host_root.clone_file_from(
                        host_root,
                        super::work::host_path(&source.0),
                        super::work::host_path(host_path),
                        &mut work,
                    )
                }
                #[cfg(windows)]
                {
                    host_root.clone_file_from(host_root, &source.0, host_path, &mut work)
                }
            }
            .map_err(MaterializeError::from)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            break;
        }
    }
    Ok((cloned, payload_known))
}

async fn materialize_symlink<A, O, E, F, Fut>(
    source: (&PinnedReader<A, O>, &CancellationToken),
    path: &NamespacePath,
    host_root: &HostRoot,
    host_path: &[HostPathUnit],
    target_bytes: u64,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(), E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let (reader, cancellation) = source;
    let target = read_source!(reader, receipt, remaining, value,
        reader.read_symbolic_link(path, receipt.custody.work(), cancellation) => value,
        reader.read_symbolic_link(path, remaining, cancellation) => {
            receipt.adopt(value.len())?;
            value
        },
    );
    if target.len() as u64 != target_bytes {
        return Err(MaterializeError::Engine("symbolic-link length mismatch").into());
    }
    generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
    create_symlink(host_root, &target, host_path, receipt)?;
    generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
    drop(target);
    if !reader.validate_native_work(receipt.custody.work()).is_ok() {
        receipt
            .custody
            .release(target_bytes)
            .map_err(MaterializeError::from)?;
    }
    receipt.value.symbolic_links = receipt
        .value
        .symbolic_links
        .checked_add(1)
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    account_materialization(receipt, 0)?;

    Ok(())
}

async fn materialize_special<E, F, Fut>(
    host_root: &HostRoot,
    host_path: &[HostPathUnit],
    kind: FileKind,
    payload: FilePayload,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(), E>
where
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let device = match (kind, payload) {
        (FileKind::Fifo | FileKind::Socket, FilePayload::Empty) => None,
        (
            FileKind::CharacterDevice | FileKind::BlockDevice,
            FilePayload::Device { major, minor },
        ) => Some((major, minor)),
        _ => return Err(MaterializeError::UnsupportedKind(kind).into()),
    };
    generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
    create_special(host_root, host_path, kind, device, receipt)?;
    generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
    receipt.value.special_files = receipt
        .value
        .special_files
        .checked_add(1)
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    account_materialization(receipt, 0)?;

    Ok(())
}
