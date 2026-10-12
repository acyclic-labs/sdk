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
        let host_path = PathBuf::from(std::ffi::OsString::from_wide(wide));
        let mut path = MountPath::root();
        for component in host_path.components() {
            let std::path::Component::Normal(component) = component else {
                return None;
            };
            let bytes = component
                .encode_wide()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            path = path.child(bytes);
        }
        Some(path)
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
) -> Result<HostWindowsMetadata, MountSourceError> {
    let metadata = root.stat(&host_relative_path(path)?, &mut &*work)?;
    Ok(HostWindowsMetadata {
        identity: crate::NativeRootIdentity {
            device: u64::from(metadata.volume_serial_number()),
            object: metadata.file_index(),
        },
        links: Some(metadata.nlink() as u32),
        size: metadata.len(),
        attributes: metadata.file_attributes(),
        created: i64::try_from(metadata.creation_time())
            .map_err(|_| MountSourceError::Invalid("Windows creation time exceeds i64".into()))?,
        modified: i64::try_from(metadata.last_write_time())
            .map_err(|_| MountSourceError::Invalid("Windows write time exceeds i64".into()))?,
    })
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

pub(super) struct MetadataProbeGuard<'a> {
    pub(super) probes: &'a Mutex<HashMap<MountPath, usize>>,
    pub(super) path: MountPath,
}

impl<'a> MetadataProbeGuard<'a> {
    pub(super) fn enter(probes: &'a Mutex<HashMap<MountPath, usize>>, path: &MountPath) -> Self {
        let mut active = lock_recover(probes);
        *active.entry(path.clone()).or_default() += 1;
        Self {
            probes,
            path: path.clone(),
        }
    }

    pub(super) fn is_active(probes: &Mutex<HashMap<MountPath, usize>>, path: &MountPath) -> bool {
        lock_recover(probes)
            .keys()
            .any(|active| matches!(projfs_path_suffix(active, path), Some([])))
    }
}

impl Drop for MetadataProbeGuard<'_> {
    fn drop(&mut self) {
        let mut active = lock_recover(self.probes);
        let Some(count) = active.get_mut(&self.path) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            active.remove(&self.path);
        }
    }
}

pub(super) fn probe_host_windows_metadata(
    root: &HostRoot,
    path: &MountPath,
    probes: &Mutex<HashMap<MountPath, usize>>,
    work: &crate::NativeMaterializationWork,
) -> Result<HostWindowsMetadata, MountSourceError> {
    let _guard = MetadataProbeGuard::enter(probes, path);
    host_windows_metadata(root, path, work)
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
