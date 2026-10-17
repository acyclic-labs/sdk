use super::probe_guard::{ProbeGuard, with_guard};
use super::*;

pub(super) fn path_from(pointer: PCWSTR) -> Option<MountPath> {
    if pointer.is_null() {
        return Some(MountPath::root());
    }
    let mut length = 0_usize;
    // SAFETY: ProjFS supplies a NUL-terminated UTF-16 callback path.
    unsafe {
        while *pointer.0.add(length) != 0 {
            length = length.checked_add(1)?;
        }
        let wide = std::slice::from_raw_parts(pointer.0, length);
        Some(MountPath {
            components: super::callback_path::components(wide)?,
        })
    }
}

pub(super) unsafe fn empty_destination(pointer: PCWSTR) -> bool {
    pointer.is_null() || *pointer.0 == 0
}

pub(super) fn decode_utf16_name(bytes: &[u8]) -> Option<Vec<u16>> {
    Some(crate::kernel::types::utf16le_units(bytes)?.collect())
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct HostWindowsMetadata {
    pub(super) identity: crate::NativeRootIdentity,
    pub(super) links: Option<u32>,
    pub(super) size: u64,
    pub(super) attributes: u32,
    pub(super) created: i64,
    pub(super) modified: i64,
}

#[derive(Clone, Copy)]
pub(super) struct ReadOnlyBinding {
    pub(super) host: HostWindowsMetadata,
    pub(super) file_id: FileId,
    pub(super) basis: ReadBasis,
}

#[derive(Clone, Copy)]
pub(super) struct OpenMetadataState {
    pub(super) baseline: HostWindowsMetadata,
    pub(super) open_handles: usize,
}

pub(super) enum MetadataClose {
    Pending,
    Final(Option<HostWindowsMetadata>),
}

pub(super) fn host_windows_metadata(
    root: &HostRoot,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
    probes: Option<&Mutex<HashMap<MountPath, usize>>>,
) -> Result<HostWindowsMetadata, MountSourceError> {
    let guard = || {
        #[cfg(test)]
        if super::tests::read_bindings::bypass_probe_registration() {
            return None;
        }
        probes.map(|probes| MetadataProbeGuard::enter(probes, path))
    };
    let query = |path: &[u16], work: &mut &crate::NativeMaterializationWork| {
        if let Some(metadata) = query_metadata(root, path, work, &guard)? {
            return Ok(metadata);
        }
        with_guard(&guard, || {
            #[cfg(test)]
            let _replay = super::tests::read_bindings::replay_fallback();
            root.stat(path, work)
        })
    };
    let metadata = if let Some(units) = super::callback_path::wide_components(path.components()) {
        crate::native_host::with_wide_units(units, &mut &*work, query)?
    } else {
        crate::native_host::with_wide_path(&host_relative_path(path)?, &mut &*work, query)?
    };
    Ok(HostWindowsMetadata {
        identity: crate::NativeRootIdentity {
            device: u64::from(metadata.volume_serial_number),
            object: u64::from_ne_bytes(metadata.raw.FileId.to_ne_bytes()),
        },
        links: Some(metadata.raw.NumberOfLinks),
        size: metadata.len(),
        attributes: metadata.raw.FileAttributes,
        created: if metadata.raw.CreationTime < 0 {
            return Err(MountSourceError::Invalid(
                "Windows creation time exceeds i64".into(),
            ));
        } else {
            metadata.raw.CreationTime
        },
        modified: if metadata.raw.LastWriteTime < 0 {
            return Err(MountSourceError::Invalid(
                "Windows write time exceeds i64".into(),
            ));
        } else {
            metadata.raw.LastWriteTime
        },
    })
}

fn query_metadata<G>(
    root: &HostRoot,
    path: &[u16],
    work: &mut &crate::NativeMaterializationWork,
    before_open: impl FnOnce() -> G,
) -> Result<Option<crate::native_host::HostStat>, crate::NativeTreeMaterializationError> {
    query_metadata_using(root, path, work, before_open, |name, volume| {
        crate::native_host::stat_windows_name(&root.directory, name, volume, true)
    })
}

fn query_metadata_using<G>(
    root: &HostRoot,
    path: &[u16],
    work: &mut &crate::NativeMaterializationWork,
    before_open: impl FnOnce() -> G,
    query: impl FnOnce(
        &windows::Win32::Foundation::UNICODE_STRING,
        u32,
    ) -> std::io::Result<Option<crate::native_host::HostStat>>,
) -> Result<Option<crate::native_host::HostStat>, crate::NativeTreeMaterializationError> {
    NativeHostWork::check(work)?;
    // Only leaf names use the query. Parent traversal retains the held-handle
    // validator, including its rejection of every reparse-tagged directory.
    let (Ok(length), Ok(volume)) = (
        u16::try_from(path.len().saturating_mul(2)),
        u32::try_from(root.identity().device),
    ) else {
        return Ok(None);
    };
    if matches!(path, [] | [46] | [46, 46])
        || path.iter().any(|unit| matches!(unit, 0 | 47 | 58 | 92))
    {
        return Ok(None);
    }
    // Name queries do not open files; only held-handle fallbacks can reenter
    // FILE_OPENED / CLOSED callbacks and need a probe registration.
    let fallback = |work: &mut &crate::NativeMaterializationWork| {
        with_guard(before_open, || stat_leaf_metadata(root, path, volume, work))
    };
    // Two reads fit a held-root open + stat, but cannot also pay for an
    // unsupported name query. Choose that path before attempting any I/O.
    if work.remaining()?.backend_read_operations == 2 {
        return fallback(work).map(Some);
    }
    let charge = crate::WorkCounters {
        items_examined: path.len() as u64,
        backend_read_operations: 1,
        ..crate::WorkCounters::UNCHARGED
    };
    let name = windows::Win32::Foundation::UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(path.as_ptr().cast_mut()),
    };
    crate::native_host::query_windows_stat(
        work,
        charge,
        || query(&name, volume),
        |work| fallback(&mut &work.recheck_admitted()),
    )
    .map(Some)
}

fn stat_leaf_metadata(
    root: &HostRoot,
    path: &[u16],
    volume: u32,
    work: &mut &crate::NativeMaterializationWork,
) -> Result<crate::native_host::HostStat, crate::NativeTreeMaterializationError> {
    use windows::Wdk::Storage::FileSystem as nt;
    use windows::Win32::Storage::FileSystem as fs;

    // The caller already restricted this to one leaf. The held root can be
    // borrowed directly; no parent traversal or duplicate handle is needed.
    let file = crate::native_host::open_windows_at(
        &root.directory,
        path,
        fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        nt::FILE_OPEN,
        nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?;
    crate::native_host::stat_windows_file(&file, volume, work)
}

pub(super) fn host_relative_path(path: &MountPath) -> Result<PathBuf, MountSourceError> {
    let mut host_path = PathBuf::new();
    for component in path.components() {
        let name = decode_utf16_name(component).ok_or_else(|| {
            MountSourceError::Invalid("ProjFS path component is malformed".into())
        })?;
        host_path.push(std::ffi::OsString::from_wide(&name));
    }
    Ok(host_path)
}

pub(super) fn capture_missing_host_ancestors(
    runtime: &Runtime,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    let mut parent = MountPath::root();
    for component in path
        .components()
        .iter()
        .take(path.components().len().saturating_sub(1))
    {
        parent = parent.child(component.clone());
        if runtime.source.lookup_with_meter(&parent, work)?.is_none() {
            runtime.source.capture_host_paths_with_meter(
                &runtime.root,
                &[parent.clone()],
                runtime.metadata_root.identity(),
                work,
            )?;
        }
    }
    Ok(())
}

pub(super) type MetadataProbeGuard<'a> = ProbeGuard<'a, MountPath>;

pub(super) fn probe_host_windows_metadata(
    root: &HostRoot,
    path: &MountPath,
    probes: &Mutex<HashMap<MountPath, usize>>,
    work: &crate::NativeMaterializationWork,
) -> Result<HostWindowsMetadata, MountSourceError> {
    host_windows_metadata(root, path, work, Some(probes))
}

pub(super) fn close_metadata_handle(
    baselines: &Mutex<HashMap<u128, OpenMetadataState>>,
    file_id: u128,
) -> MetadataClose {
    let mut baselines = lock_recover(baselines);
    let Some(state) = baselines.get_mut(&file_id) else {
        return MetadataClose::Final(None);
    };
    if state.open_handles > 1 {
        state.open_handles -= 1;
        MetadataClose::Pending
    } else {
        MetadataClose::Final(baselines.remove(&file_id).map(|state| state.baseline))
    }
}

pub(super) fn record_metadata_open(
    baselines: &Mutex<HashMap<u128, OpenMetadataState>>,
    file_id: u128,
    baseline: HostWindowsMetadata,
) -> Result<(), MountSourceError> {
    let mut baselines = lock_recover(baselines);
    if let Some(state) = baselines.get_mut(&file_id) {
        if state.open_handles == 0 {
            state.open_handles = 1;
        } else {
            state.open_handles = state.open_handles.checked_add(1).ok_or_else(|| {
                MountSourceError::Invalid("ProjFS open-handle count overflow".into())
            })?;
        }
    } else {
        baselines.insert(
            file_id,
            OpenMetadataState {
                baseline,
                open_handles: 1,
            },
        );
    }
    Ok(())
}

pub(super) fn refresh_metadata_baseline(
    baselines: &Mutex<HashMap<u128, OpenMetadataState>>,
    file_id: u128,
    baseline: HostWindowsMetadata,
) {
    let mut baselines = lock_recover(baselines);
    baselines
        .entry(file_id)
        .and_modify(|state| state.baseline = baseline)
        .or_insert(OpenMetadataState {
            baseline,
            open_handles: 0,
        });
}

pub(super) fn metadata_changed_since_open(
    baseline: HostWindowsMetadata,
    current: HostWindowsMetadata,
) -> bool {
    let attributes_changed =
        !crate::native_host::same_windows_attributes(baseline.attributes, current.attributes);
    attributes_changed
        || baseline.identity != current.identity
        || baseline.created != current.created
        || baseline.modified != current.modified
}

pub(super) fn capture_changed_windows_metadata(
    source: &dyn MountFilesystem,
    path: &MountPath,
    mut metadata: FileMetadata,
    baseline: HostWindowsMetadata,
    current: HostWindowsMetadata,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    let attributes_changed =
        !crate::native_host::same_windows_attributes(baseline.attributes, current.attributes);
    if attributes_changed {
        metadata.windows_attributes = MetadataField::Value(current.attributes);
    }
    if baseline.created != current.created {
        metadata.created_ns = MetadataField::Value(unix_nanoseconds(current.created)?);
    }
    if baseline.modified != current.modified {
        metadata.modified_ns = MetadataField::Value(unix_nanoseconds(current.modified)?);
    }
    source
        .set_attributes(path, metadata, None, work)
        .and_then(|()| source.flush(work))
}

pub(super) fn guid_key(guid: &GUID) -> u128 {
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(&guid.data1.to_le_bytes());
    bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    bytes[8..16].copy_from_slice(&guid.data4);
    u128::from_le_bytes(bytes)
}

pub(super) fn guid_from_key(key: u128) -> GUID {
    let bytes = key.to_le_bytes();
    GUID::from_values(
        u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u16::from_le_bytes([bytes[4], bytes[5]]),
        u16::from_le_bytes([bytes[6], bytes[7]]),
        [
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ],
    )
}

pub(super) fn file_id(data: &PRJ_CALLBACK_DATA) -> u128 {
    guid_key(&data.FileId)
}

pub(super) fn basic(
    node: MountNode,
    metadata: Option<FileMetadata>,
) -> Option<PRJ_FILE_BASIC_INFO> {
    let (directory, default_attributes, size) = match node.kind {
        MountNodeKind::Directory => (true, FILE_ATTRIBUTE_DIRECTORY.0, 0),
        MountNodeKind::Regular => (false, FILE_ATTRIBUTE_NORMAL.0, node.logical_bytes),
        MountNodeKind::SymbolicLink => (false, FILE_ATTRIBUTE_REPARSE_POINT.0, node.logical_bytes),
        MountNodeKind::Fifo
        | MountNodeKind::Socket
        | MountNodeKind::CharacterDevice
        | MountNodeKind::BlockDevice
        | MountNodeKind::Unsupported => return None,
    };
    let attributes = metadata.map_or(default_attributes, |metadata| {
        let exact = match metadata.windows_attributes {
            MetadataField::Unavailable => default_attributes,
            MetadataField::Value(value) => value,
        };
        if directory {
            exact | FILE_ATTRIBUTE_DIRECTORY.0
        } else if exact == 0 {
            FILE_ATTRIBUTE_NORMAL.0
        } else {
            exact
        }
    });
    Some(PRJ_FILE_BASIC_INFO {
        IsDirectory: directory,
        FileSize: i64::try_from(size).ok()?,
        CreationTime: metadata
            .and_then(|value| windows_time(value.created_ns))
            .unwrap_or(0),
        LastAccessTime: metadata
            .and_then(|value| windows_time(value.accessed_ns))
            .unwrap_or(0),
        LastWriteTime: metadata
            .and_then(|value| windows_time(value.modified_ns))
            .unwrap_or(0),
        ChangeTime: metadata
            .and_then(|value| windows_time(value.changed_ns))
            .unwrap_or(0),
        FileAttributes: attributes,
    })
}

pub(super) fn unix_nanoseconds(windows_ticks: i64) -> Result<i64, MountSourceError> {
    crate::native_host::filetime_to_unix_nanoseconds(windows_ticks).ok_or_else(|| {
        MountSourceError::Invalid("Windows timestamp exceeds i64 nanoseconds".into())
    })
}

pub(super) fn symlink_extended(target: &[u8]) -> Option<(HSTRING, PRJ_EXTENDED_INFO)> {
    let target = HSTRING::from_wide(&decode_utf16_name(target)?);
    let extended = PRJ_EXTENDED_INFO {
        InfoType: PRJ_EXT_INFO_TYPE_SYMLINK,
        NextInfoOffset: 0,
        Anonymous: PRJ_EXTENDED_INFO_0 {
            Symlink: PRJ_EXTENDED_INFO_0_0 {
                TargetName: PCWSTR::from_raw(target.as_ptr()),
            },
        },
    };
    Some((target, extended))
}

pub(super) fn windows_time(field: MetadataField<i64>) -> Option<i64> {
    let MetadataField::Value(unix_nanoseconds) = field else {
        return None;
    };
    Some(crate::native_host::unix_nanoseconds_to_filetime(
        unix_nanoseconds,
    ))
}

pub(super) unsafe fn start_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

#[cfg(test)]
#[path = "tests/metadata_query.rs"]
mod query_tests;

#[cfg(test)]
#[path = "tests/metadata_encoding.rs"]
mod encoding_tests;
