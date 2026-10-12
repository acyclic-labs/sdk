use super::*;

pub(super) async fn apply_metadata<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<(HostRoot, crate::NativeAllocation)>,
    path: &NamespacePath,
    host_path: &Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    metadata: Option<FileMetadata>,
    cancellation: &CancellationToken,
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
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => read_source!(reader, receipt, remaining, value,
            reader.read_metadata(path, receipt.custody.work(), cancellation) => value,
            reader.read_metadata(path, remaining, cancellation) => value,
        ),
    };
    let (metadata, named_attributes) = host_metadata(metadata);
    if let Some(root) = named_attributes {
        let read = read_source!(reader, receipt, remaining, value,
            reader.read_named_attributes(root, receipt.custody.work(), cancellation)
                => receipt.accept_source(value.0, value.1)?,
            reader.read_named_attributes(root, remaining, cancellation) => {
                receipt.adopt(vec_bytes(&value))?;
                for (name, bytes) in &value {
                    receipt.adopt(name.retained_bytes())?;
                    receipt.adopt(bytes.len())?;
                }
                value
            },
        );
        let source_array_bytes = vec_bytes(&read);
        let mut attributes = Vec::new();
        let mut payload_bytes = 0_usize;
        for (name, value) in read {
            if name.class() != crate::kernel::AttributeClass::PosixXattr {
                return Err(MaterializeError::UnsupportedMetadata("named_attributes").into());
            }
            let name = name.bytes;
            let value_bytes = if reader.validate_native_work(receipt.custody.work()).is_ok() {
                0
            } else {
                value.len()
            };
            payload_bytes = payload_bytes
                .checked_add(name.capacity())
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            receipt.push(&mut attributes, (name, value))?;
        }
        receipt
            .custody
            .release(source_array_bytes as u64)
            .map_err(materialize_native_error)?;
        let host_array_bytes = vec_bytes(&attributes);
        let retained = host_array_bytes
            .checked_add(payload_bytes)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
        generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
        apply_host_attributes_offloaded(host_root, host_path, attributes, retained, receipt)
            .await?;
        generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
    }
    generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
    apply_host_metadata_offloaded(host_root, host_path, metadata, receipt).await?;
    generation_action(before_effect, NativeMaterializationActionPhase::Completed).await
}

pub(super) async fn apply_host_attributes_offloaded(
    host_root: &Arc<(HostRoot, crate::NativeAllocation)>,
    host_path: &Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    attributes: Vec<(Vec<u8>, acyclic_stream::OwnedBytes)>,
    retained: usize,
    receipt: &mut GenerationReceipt,
) -> Result<(), MaterializeError> {
    let custody = receipt
        .custody
        .split_off(retained as u64)
        .map_err(materialize_native_error)?;
    let attributes = (attributes, custody);
    if attributes.0.is_empty() {
        return Ok(());
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let root = host_root.clone();
        let path = host_path.clone();
        host_io(None, receipt.custody.work().clone(), move |work| {
            let attributes = attributes;
            #[cfg(target_os = "linux")]
            let target = root
                .0
                .open_linux_metadata_target(super::work::host_path(&path.0), work)?;
            #[cfg(target_os = "macos")]
            let target = root
                .0
                .open_macos_metadata_target(super::work::host_path(&path.0), work)?;
            #[cfg(target_os = "linux")]
            let result = crate::native_host::set_extended_attributes(&target, &attributes.0, work);
            #[cfg(target_os = "macos")]
            let result = match &target {
                crate::native_host::MacMetadataTarget::Held(file) => {
                    crate::native_host::set_extended_attributes(file, &attributes.0, work)
                }
                crate::native_host::MacMetadataTarget::Noop { .. } => {
                    work.check()
                        .and(Err(crate::native_host::NativeMetadataError(
                            "named_attributes on a non-ordinary file kind",
                        )
                        .into()))
                }
            };
            #[cfg(target_os = "macos")]
            {
                let target_bytes = target.logical_owned_bytes();
                drop(target);
                crate::native_host::NativeHostWork::release(work, target_bytes)?;
            }
            result
        })
        .await
        .map_err(materialize_runtime_error)?
        .map_err(materialize_demand_error)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (host_root, host_path);
        Err(MaterializeError::UnsupportedMetadata("named_attributes"))
    }
}

pub(super) async fn apply_host_metadata_offloaded(
    host_root: &Arc<(HostRoot, crate::NativeAllocation)>,
    host_path: &Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    metadata: FileMetadata,
    receipt: &mut GenerationReceipt,
) -> Result<(), MaterializeError> {
    #[cfg(unix)]
    reject_unbaselined_unix_metadata(metadata)?;
    let root = host_root.clone();
    let path = host_path.clone();
    host_io(None, receipt.custody.work().clone(), move |work| {
        if metadata == FileMetadata::default() {
            #[cfg(unix)]
            root.0.stat(super::work::host_path(&path.0), work)?;
            #[cfg(windows)]
            root.0.stat_wide(&path.0, work)?;
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        let target = root
            .0
            .open_linux_metadata_target(super::work::host_path(&path.0), work)?;
        #[cfg(target_os = "macos")]
        let target = root
            .0
            .open_macos_metadata_target(super::work::host_path(&path.0), work)?;
        #[cfg(windows)]
        let target = root.0.open_windows_metadata_target_wide(&path.0, work)?;
        #[cfg(target_os = "linux")]
        {
            crate::native_host::apply_linux_metadata(&target, metadata, work)
        }
        #[cfg(target_os = "macos")]
        {
            target.apply(metadata, work)
        }
        #[cfg(windows)]
        {
            crate::native_host::apply_windows_metadata(target, metadata, work)
        }
    })
    .await
    .map_err(materialize_runtime_error)?
    .map_err(materialize_demand_error)
}

#[cfg(unix)]
pub(super) fn reject_unbaselined_unix_metadata(
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    if matches!(metadata.changed_ns, MetadataField::Value(_)) {
        return Err(MaterializeError::UnsupportedMetadata(
            "changed_ns without native-view baseline",
        ));
    }
    #[cfg(target_os = "linux")]
    if matches!(metadata.created_ns, MetadataField::Value(_)) {
        return Err(MaterializeError::UnsupportedMetadata(
            "created_ns without native-view baseline",
        ));
    }
    Ok(())
}

pub(super) fn validate_options(
    options: &MaterializeOptions,
    receipt: &mut GenerationReceipt,
) -> Result<HostRoot, MaterializeError> {
    let root = receipt
        .run(|work| HostRoot::open(&options.destination, work))
        .map_err(|failure| {
            let error = match failure {
                error @ (MaterializeError::Work(_) | MaterializeError::Cancelled(_)) => error,
                _ => MaterializeError::InvalidDestination,
            };
            error
        })?;
    if let Some(expected) = options.expected_root_identity {
        let actual = root.identity();
        if actual != expected {
            return Err(MaterializeError::AllocationChanged { expected, actual });
        }
    }
    let (mut entries, reader_allocation) =
        receipt.run(|work| root.read_dir_stats(Path::new(""), work))?;
    #[cfg(unix)]
    let next = receipt.run(|work| entries.next(work));
    #[cfg(windows)]
    let next = receipt.run(|work| entries.next_wide(work));
    drop(entries);
    receipt.run(|work| crate::native_host::NativeHostWork::release(work, reader_allocation))?;
    if let Some((entry, allocation)) = next? {
        drop(entry);
        receipt.run(|work| crate::native_host::NativeHostWork::release(work, allocation))?;
        return Err(MaterializeError::InvalidDestination);
    }
    Ok(root)
}

pub(super) fn validate_bounds(options: &MaterializeOptions) -> Result<(), MaterializeError> {
    if options.maximum_directory_entries == 0
        || options.maximum_extent_spans == 0
        || options.transfer_bytes == 0
        || usize::try_from(options.transfer_bytes).is_err()
    {
        return Err(MaterializeError::InvalidOptions);
    }
    Ok(())
}

pub(super) fn create_file(
    host_root: &HostRoot,
    path: &Vec<HostPathUnit>,
    receipt: &mut GenerationReceipt,
) -> Result<acyclic_native_runtime::NativeFile, MaterializeError> {
    #[cfg(windows)]
    let file = receipt.run(|work| host_root.create_overlapped_file_wide(&path, work))?;
    #[cfg(unix)]
    let file = receipt.run(|work| host_root.create_file(super::work::host_path(path), work))?;
    let meter = Some(acyclic_native_runtime::NativeFileMeter::Owned(
        receipt.custody.work().runtime_meter(),
    ));
    #[cfg(windows)]
    #[allow(
        unsafe_code,
        reason = "the newly created held handle is overlapped and exclusively transferred"
    )]
    // SAFETY: the held-root-relative constructor provides a fresh, unassociated overlapped handle.
    let native =
        unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file, meter) };
    #[cfg(unix)]
    let native = acyclic_native_runtime::NativeFile::from_file(file, meter);
    native
        .map_err(|(error, file)| {
            drop(file);
            error
        })
        .map_err(materialize_runtime_error)
}

pub(super) fn account_written(
    receipt: &mut GenerationReceipt,
    bytes: u64,
) -> Result<(), MaterializeError> {
    receipt.value.written_bytes = receipt
        .value
        .written_bytes
        .checked_add(bytes)
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    receipt.run(|work| {
        work.with_original_leaf(|work| work.work.verify(work.budget).map_err(DemandError::from))
    })
}

pub(super) fn account_materialization(
    receipt: &mut GenerationReceipt,
    bytes: u64,
) -> Result<(), MaterializeError> {
    account_written(receipt, bytes)?;
    receipt.run(|work| {
        work.record(WorkCounters {
            materializations: 1,
            ..WorkCounters::UNCHARGED
        })
    })
}

pub(super) fn account_file(
    receipt: &mut GenerationReceipt,
    logical: u64,
    written: u64,
) -> Result<(), MaterializeError> {
    receipt.value.files = receipt
        .value
        .files
        .checked_add(1)
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    receipt.value.logical_file_bytes = receipt
        .value
        .logical_file_bytes
        .checked_add(logical)
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    account_materialization(receipt, written)
}

pub(super) fn create_symlink_measured(
    host_root: &HostRoot,
    target: &acyclic_stream::OwnedBytes,
    destination: &Vec<HostPathUnit>,
    receipt: &mut GenerationReceipt,
) -> Result<(), MaterializeError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        receipt.run(|work| {
            host_root.symlink(
                OsStr::from_bytes(target),
                super::work::host_path(destination),
                work,
            )
        })
    }
    #[cfg(windows)]
    {
        let decoded = crate::kernel::types::utf16le_units(target)
            .ok_or_else(|| MaterializeError::UnrepresentableName)?;
        let mut units = Vec::new();
        receipt.reserve(&mut units, target.len() / 2)?;
        receipt.run(|work| {
            work.charge(WorkCounters {
                bytes_copied: target.len() as u64,
                ..WorkCounters::UNCHARGED
            })
        })?;
        units.extend(decoded);
        let result = receipt.run(|work| host_root.symlink_file_wide(&units, &destination, work));
        let retained = units.capacity() * std::mem::size_of::<u16>();
        drop(units);
        receipt
            .custody
            .release(retained as u64)
            .map_err(materialize_native_error)?;
        result
    }
}

#[allow(
    clippy::useless_conversion,
    reason = "libc file-type constants have different widths on Linux and macOS"
)]
pub(super) fn create_special_measured(
    host_root: &HostRoot,
    destination: &Vec<HostPathUnit>,
    kind: FileKind,
    device: Option<(u32, u32)>,
    receipt: &mut GenerationReceipt,
) -> Result<(), MaterializeError> {
    #[cfg(unix)]
    {
        match kind {
            FileKind::Fifo => receipt.run(|work| {
                host_root.create_fifo_held(super::work::host_path(destination), 0o600, work)
            }),
            FileKind::CharacterDevice | FileKind::BlockDevice => {
                let (major, minor) =
                    device.ok_or_else(|| MaterializeError::UnsupportedKind(kind))?;
                let file_type = if kind == FileKind::CharacterDevice {
                    u32::from(libc::S_IFCHR)
                } else {
                    u32::from(libc::S_IFBLK)
                };
                let device_number = super::device::join_device(major, minor).map_err(|errno| {
                    MaterializeError::Io(std::io::Error::from_raw_os_error(errno))
                })?;
                receipt.run(|work| {
                    host_root.create_device_held(
                        super::work::host_path(destination),
                        file_type | 0o600,
                        device_number,
                        work,
                    )
                })
            }
            FileKind::Socket => receipt
                .run(|work| host_root.bind_unix_socket(super::work::host_path(destination), work)),
            _ => Err(MaterializeError::UnsupportedKind(kind)),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (host_root, destination, device);
        Err(MaterializeError::UnsupportedKind(kind))
    }
}
