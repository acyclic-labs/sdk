//! Audited Windows `ProjFS` FFI with exact close-boundary authored capture.
//!
//! `ProjFS` does not expose write byte ranges, so modified and newly created
//! files are streamed from final host state only after the corresponding file
//! handle closes. Rename and hard-link notifications preserve stable identity,
//! and every acknowledged authored notification seals the checkout generation.

#![allow(unsafe_code, unsafe_op_in_unsafe_fn)]

use super::{
    MountDirectoryEntry, MountFilesystem, MountNode, MountNodeKind, MountPath, NativeMountError,
    NativeMountRequest,
};
use crate::kernel::{FileMetadata, MetadataField};
use std::collections::{HashMap, VecDeque};
use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
};
use windows::Win32::Storage::ProjectedFileSystem::{
    PRJ_CALLBACK_DATA, PRJ_CALLBACKS, PRJ_DIR_ENTRY_BUFFER_HANDLE, PRJ_EXT_INFO_TYPE_SYMLINK,
    PRJ_EXTENDED_INFO, PRJ_EXTENDED_INFO_0, PRJ_EXTENDED_INFO_0_0, PRJ_FILE_BASIC_INFO,
    PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, PRJ_NOTIFICATION,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFICATION_FILE_OVERWRITTEN,
    PRJ_NOTIFICATION_FILE_RENAMED, PRJ_NOTIFICATION_HARDLINK_CREATED, PRJ_NOTIFICATION_MAPPING,
    PRJ_NOTIFICATION_NEW_FILE_CREATED, PRJ_NOTIFICATION_PARAMETERS, PRJ_NOTIFICATION_PRE_DELETE,
    PRJ_NOTIFICATION_PRE_RENAME, PRJ_NOTIFICATION_PRE_SET_HARDLINK,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED, PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFY_FILE_OVERWRITTEN,
    PRJ_NOTIFY_FILE_RENAMED, PRJ_NOTIFY_HARDLINK_CREATED, PRJ_NOTIFY_NEW_FILE_CREATED,
    PRJ_NOTIFY_PRE_DELETE, PRJ_NOTIFY_PRE_RENAME, PRJ_NOTIFY_PRE_SET_HARDLINK,
    PRJ_PLACEHOLDER_INFO, PRJ_STARTVIRTUALIZING_OPTIONS, PrjAllocateAlignedBuffer,
    PrjFillDirEntryBuffer, PrjFillDirEntryBuffer2, PrjFreeAlignedBuffer,
    PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing, PrjWriteFileData,
    PrjWritePlaceholderInfo, PrjWritePlaceholderInfo2,
};
use windows::core::{GUID, HRESULT, HSTRING, PCWSTR};

const HR_OK: HRESULT = HRESULT(0);
const HR_FILE_NOT_FOUND: HRESULT = HRESULT(0x8007_0002_u32.cast_signed());
const HR_ALREADY_EXISTS: HRESULT = HRESULT(0x8007_00b7_u32.cast_signed());
const HR_NOT_SAME_DEVICE: HRESULT = HRESULT(0x8007_0011_u32.cast_signed());
const HR_INVALID_DATA: HRESULT = HRESULT(0x8007_000d_u32.cast_signed());
const HR_NOT_SUPPORTED: HRESULT = HRESULT(0x8007_0032_u32.cast_signed());
const HR_OUT_OF_MEMORY: HRESULT = HRESULT(0x8007_000e_u32.cast_signed());
const HR_UNEXPECTED: HRESULT = HRESULT(0x8000_ffff_u32.cast_signed());
const HR_INSUFFICIENT_BUFFER: HRESULT = HRESULT(0x8007_007a_u32.cast_signed());
const DIRECTORY_PAGE_SIZE: u32 = 256;
const NOTIFICATION_ROOT: [u16; 1] = [0];

struct EnumState {
    path: MountPath,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    exhausted: bool,
}

struct Runtime {
    source: Arc<dyn MountFilesystem>,
    root: PathBuf,
    writable: bool,
    enumerations: Mutex<HashMap<u128, Arc<Mutex<EnumState>>>>,
}

/// One process-owned `ProjFS` virtualization context.
pub(super) struct ProjFsSession {
    context: Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>,
    runtime: Option<Box<Runtime>>,
}

// SAFETY: ProjFS contexts are opaque handles. The provider synchronizes callback
// dispatch and `stop` consumes the session before dropping callback state.
unsafe impl Send for ProjFsSession {}

impl ProjFsSession {
    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, NativeMountError> {
        let root = HSTRING::from(request.destination.as_os_str());
        let root_ptr = PCWSTR::from_raw(root.as_ptr());
        let bytes = request.mount_id.into_bytes();
        let guid = GUID::from_values(
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            u16::from_le_bytes([bytes[4], bytes[5]]),
            u16::from_le_bytes([bytes[6], bytes[7]]),
            [
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ],
        );
        // SAFETY: the destination was admitted as an existing empty directory;
        // all pointers remain valid for this synchronous call.
        unsafe { PrjMarkDirectoryAsPlaceholder(root_ptr, PCWSTR::null(), None, &raw const guid) }
            .or_else(|error| match error.code().0.cast_unsigned() {
                0x8007_1129 | 0x8007_112b => Ok(()),
                _ => Err(error),
            })
            .map_err(|error| driver_error(&error))?;

        let mut runtime = Box::new(Runtime {
            source,
            root: request.destination.clone(),
            writable: request.writable,
            enumerations: Mutex::new(HashMap::new()),
        });
        let context_ptr = (&raw mut *runtime).cast::<c_void>();
        let callbacks = callbacks();
        let mut notification_mapping = PRJ_NOTIFICATION_MAPPING {
            NotificationBitMask: PRJ_NOTIFY_NEW_FILE_CREATED
                | PRJ_NOTIFY_FILE_OVERWRITTEN
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED
                | PRJ_NOTIFY_PRE_DELETE
                | PRJ_NOTIFY_PRE_RENAME
                | PRJ_NOTIFY_FILE_RENAMED
                | PRJ_NOTIFY_PRE_SET_HARDLINK
                | PRJ_NOTIFY_HARDLINK_CREATED,
            // ProjFS requires a pointer to an empty UTF-16 string for the
            // virtualization root. An empty HSTRING may be represented by a
            // null handle, which is not the same contract as `L""`.
            NotificationRoot: PCWSTR::from_raw(NOTIFICATION_ROOT.as_ptr()),
        };
        let mut options = PRJ_STARTVIRTUALIZING_OPTIONS::default();
        if request.writable {
            options.NotificationMappings = &raw mut notification_mapping;
            options.NotificationMappingsCount = 1;
        }
        // SAFETY: `runtime` is heap-stable until `PrjStopVirtualizing` has
        // synchronously drained every callback in `stop`.
        let context = unsafe {
            PrjStartVirtualizing(
                root_ptr,
                &raw const callbacks,
                Some(context_ptr.cast_const()),
                Some(&raw const options),
            )
        }
        .map_err(|error| driver_error(&error))?;
        Ok(Self {
            context: Some(context),
            runtime: Some(runtime),
        })
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        let Some(context) = self.context.take() else {
            return Ok(());
        };
        // SAFETY: this is the sole owner and sole stop call for the context.
        unsafe { PrjStopVirtualizing(context) };
        drop(self.runtime.take());
        Ok(())
    }
}

/// Deletes only authenticated, cache-only residue from a stopped `ProjFS` root.
///
/// A `ProjFS` virtualization root is a reparse point whose descendants can
/// contain hydrated placeholders after the provider process dies. Ordinary
/// `remove_dir` therefore cannot reclaim it. This recovery path first proves
/// the root's exact `ProjFS` reparse tag, delegates the no-follow tree removal
/// to the Windows standard-library implementation, and bounds transient
/// virtualization-filter convergence to 64 attempts. A root with any other
/// reparse tag is rejected.
pub(super) fn recover_cache_only_destination(
    destination: &std::path::Path,
) -> Result<(), NativeMountError> {
    let metadata = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(NativeMountError::Driver(error.to_string())),
    };
    if !metadata.is_dir() {
        return Err(NativeMountError::InvalidDestination);
    }
    match reparse_tag(destination).map_err(|error| {
        NativeMountError::Driver(format!(
            "ProjFS root authentication failed for {}: {error}",
            destination.display()
        ))
    })? {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {}
        Some(tag) => {
            return Err(NativeMountError::Driver(format!(
                "destination has non-ProjFS reparse tag 0x{tag:08x}"
            )));
        }
        None => return Ok(()),
    }
    let mut last_transient = None;
    for _ in 0..64 {
        match std::fs::remove_dir_all(destination) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) if matches!(error.raw_os_error(), Some(145 | 369)) => {
                last_transient = Some(error);
            }
            Err(error) => {
                return Err(NativeMountError::Driver(format!(
                    "authenticated ProjFS root removal failed for {}: {error}",
                    destination.display()
                )));
            }
        }
    }
    Err(NativeMountError::Driver(format!(
        "authenticated ProjFS root removal did not converge for {} after 64 bounded attempts: {}",
        destination.display(),
        last_transient.map_or_else(
            || "unknown transient failure".to_owned(),
            |error| error.to_string()
        )
    )))
}

#[allow(unsafe_code)]
fn reparse_tag(path: &std::path::Path) -> Result<Option<u32>, NativeMountError> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::{
        ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE, ERROR_NOT_A_REPARSE_POINT, HANDLE,
    };
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, MAXIMUM_REPARSE_DATA_BUFFER_SIZE,
    };
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let output_len = usize::try_from(MAXIMUM_REPARSE_DATA_BUFFER_SIZE)
        .map_err(|_| NativeMountError::Driver("reparse buffer size overflow".to_owned()))?;
    let mut output = vec![0_u8; output_len];
    let mut returned = 0_u32;
    // SAFETY: the handle and output buffer remain valid for the synchronous
    // control call; the output length exactly matches the allocated buffer.
    let result = unsafe {
        DeviceIoControl(
            HANDLE(file.as_raw_handle()),
            FSCTL_GET_REPARSE_POINT,
            None,
            0,
            Some(output.as_mut_ptr().cast()),
            MAXIMUM_REPARSE_DATA_BUFFER_SIZE,
            Some(&raw mut returned),
            None,
        )
    };
    if let Err(error) = result {
        let code = error.code().0.cast_unsigned();
        if code == 0x8007_0000_u32 | ERROR_NOT_A_REPARSE_POINT.0 {
            return Ok(None);
        }
        if code == 0x8007_0000_u32 | ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0 {
            return Ok(Some(
                windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS,
            ));
        }
        return Err(NativeMountError::Driver(error.to_string()));
    }
    if returned < 4 {
        return Err(NativeMountError::Driver(
            "reparse response omitted its tag".to_owned(),
        ));
    }
    Ok(Some(u32::from_le_bytes([
        output[0], output[1], output[2], output[3],
    ])))
}

fn driver_error(error: &windows::core::Error) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

unsafe fn runtime<'a>(
    data: *const PRJ_CALLBACK_DATA,
) -> Option<(&'a PRJ_CALLBACK_DATA, &'a Runtime)> {
    let data = data.as_ref()?;
    let pointer = data.InstanceContext.cast::<Runtime>();
    pointer.as_ref().map(|value| (data, value))
}

fn path_from(pointer: PCWSTR) -> Option<MountPath> {
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

unsafe fn empty_destination(pointer: PCWSTR) -> bool {
    pointer.is_null() || *pointer.0 == 0
}

fn decode_utf16_name(bytes: &[u8]) -> Option<Vec<u16>> {
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    Some(
        bytes
            .chunks_exact(2)
            .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
            .collect(),
    )
}

fn enum_id(pointer: *const GUID) -> Option<u128> {
    // SAFETY: callback ABI supplies a GUID pointer for the callback duration.
    let guid = unsafe { pointer.as_ref()? };
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(&guid.data1.to_le_bytes());
    bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    bytes[8..16].copy_from_slice(&guid.data4);
    Some(u128::from_le_bytes(bytes))
}

fn basic(node: MountNode, metadata: Option<FileMetadata>) -> Option<PRJ_FILE_BASIC_INFO> {
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

fn symlink_extended(target: &[u8]) -> Option<(HSTRING, PRJ_EXTENDED_INFO)> {
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

fn windows_time(field: MetadataField<i64>) -> Option<i64> {
    const WINDOWS_EPOCH_OFFSET_SECONDS: i128 = 11_644_473_600;
    const HUNDRED_NANOSECONDS_PER_SECOND: i128 = 10_000_000;
    let MetadataField::Value(unix_nanoseconds) = field else {
        return None;
    };
    let ticks = WINDOWS_EPOCH_OFFSET_SECONDS
        .checked_mul(HUNDRED_NANOSECONDS_PER_SECOND)?
        .checked_add(i128::from(unix_nanoseconds).div_euclid(100))?;
    i64::try_from(ticks).ok()
}

unsafe extern "system" fn start_directory(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let Some(id) = enum_id(enumeration_id) else {
        return HR_INVALID_DATA;
    };
    lock_recover(&runtime.enumerations).insert(
        id,
        Arc::new(Mutex::new(EnumState {
            path,
            cursor: None,
            entries: VecDeque::new(),
            exhausted: false,
        })),
    );
    HR_OK
}

unsafe extern "system" fn end_directory(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
) -> HRESULT {
    let Some((_data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(id) = enum_id(enumeration_id) else {
        return HR_INVALID_DATA;
    };
    lock_recover(&runtime.enumerations).remove(&id);
    HR_OK
}

unsafe extern "system" fn get_directory(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
    _search_expression: PCWSTR,
    buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    let Some((_data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(id) = enum_id(enumeration_id) else {
        return HR_INVALID_DATA;
    };
    let Some(state) = lock_recover(&runtime.enumerations).get(&id).cloned() else {
        return HR_INVALID_DATA;
    };
    loop {
        let need_page = {
            let state = lock_recover(&state);
            state.entries.is_empty() && !state.exhausted
        };
        if need_page {
            let (path, cursor) = {
                let state = lock_recover(&state);
                (state.path.clone(), state.cursor.clone())
            };
            let Ok(page) =
                runtime
                    .source
                    .read_directory(&path, cursor.as_deref(), DIRECTORY_PAGE_SIZE)
            else {
                return HR_UNEXPECTED;
            };
            let mut state = lock_recover(&state);
            state.cursor = page.next_cursor;
            state.exhausted = state.cursor.is_none();
            state.entries.extend(page.entries);
            if state.entries.is_empty() && !state.exhausted {
                continue;
            }
        }
        let entry = lock_recover(&state).entries.front().cloned();
        let Some(entry) = entry else {
            return HR_OK;
        };
        let Some(info) = basic(entry.node, Some(entry.metadata)) else {
            return HR_NOT_SUPPORTED;
        };
        let Some(wide) = decode_utf16_name(&entry.name) else {
            return HR_INVALID_DATA;
        };
        let wide = HSTRING::from_wide(&wide);
        let symlink = if entry.node.kind == MountNodeKind::SymbolicLink {
            let child = lock_recover(&state).path.child(entry.name.clone());
            let Ok(target) = runtime.source.read_link(&child) else {
                return HR_UNEXPECTED;
            };
            let Some(target) = symlink_extended(&target) else {
                return HR_INVALID_DATA;
            };
            Some(target)
        } else {
            None
        };
        let result = if let Some((_target, extended)) = symlink.as_ref() {
            PrjFillDirEntryBuffer2(
                buffer,
                PCWSTR::from_raw(wide.as_ptr()),
                Some(&raw const info),
                Some(extended),
            )
        } else {
            PrjFillDirEntryBuffer(
                PCWSTR::from_raw(wide.as_ptr()),
                Some(&raw const info),
                buffer,
            )
        };
        match result {
            Ok(()) => {
                lock_recover(&state).entries.pop_front();
            }
            Err(error) if error.code() == HR_INSUFFICIENT_BUFFER => return HR_OK,
            Err(error) => return error.code(),
        }
    }
}

unsafe extern "system" fn placeholder(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let node = match runtime.source.lookup(&path) {
        Ok(Some(node)) => node,
        Ok(None) | Err(super::MountSourceError::NotFound) => return HR_FILE_NOT_FOUND,
        Err(_) => return HR_UNEXPECTED,
    };
    let Some(info) = basic(node.node, Some(node.metadata)) else {
        return HR_NOT_SUPPORTED;
    };
    let placeholder = PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: info,
        ..PRJ_PLACEHOLDER_INFO::default()
    };
    let symlink = if node.node.kind == MountNodeKind::SymbolicLink {
        let target = match runtime.source.read_link(&path) {
            Ok(target) => target,
            Err(super::MountSourceError::NotFound) => return HR_FILE_NOT_FOUND,
            Err(_) => return HR_UNEXPECTED,
        };
        let Some(target) = symlink_extended(&target) else {
            return HR_INVALID_DATA;
        };
        Some(target)
    } else {
        None
    };
    let result = if let Some((_target, extended)) = symlink.as_ref() {
        PrjWritePlaceholderInfo2(
            data.NamespaceVirtualizationContext,
            data.FilePathName,
            &raw const placeholder,
            u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX),
            Some(extended),
        )
    } else {
        PrjWritePlaceholderInfo(
            data.NamespaceVirtualizationContext,
            data.FilePathName,
            &raw const placeholder,
            u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX),
        )
    };
    match result {
        Ok(()) => HR_OK,
        Err(error) => error.code(),
    }
}

unsafe extern "system" fn file_data(
    callback_data: *const PRJ_CALLBACK_DATA,
    byte_offset: u64,
    length: u32,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let bytes = match runtime.source.read_range(&path, byte_offset, length) {
        Ok(bytes) if bytes.len() == length as usize => bytes,
        Ok(_) => return HR_INVALID_DATA,
        Err(super::MountSourceError::NotFound) => return HR_FILE_NOT_FOUND,
        Err(_) => return HR_UNEXPECTED,
    };
    let buffer = PrjAllocateAlignedBuffer(data.NamespaceVirtualizationContext, bytes.len());
    if buffer.is_null() {
        return HR_OUT_OF_MEMORY;
    }
    // SAFETY: ProjFS allocated `bytes.len()` aligned bytes and both buffers are
    // live and non-overlapping for the synchronous write.
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), bytes.len());
    let result = PrjWriteFileData(
        data.NamespaceVirtualizationContext,
        &raw const data.DataStreamId,
        buffer,
        byte_offset,
        length,
    );
    PrjFreeAlignedBuffer(buffer);
    match result {
        Ok(()) => HR_OK,
        Err(error) => error.code(),
    }
}

unsafe extern "system" fn query_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    match runtime.source.lookup(&path) {
        Ok(Some(_)) => HR_OK,
        Ok(None) | Err(super::MountSourceError::NotFound) => HR_FILE_NOT_FOUND,
        Err(_) => HR_UNEXPECTED,
    }
}

unsafe extern "system" fn notification(
    callback_data: *const PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: PRJ_NOTIFICATION,
    destination_filename: PCWSTR,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    if !runtime.writable {
        return HR_NOT_SUPPORTED;
    }
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    if (notification == PRJ_NOTIFICATION_PRE_RENAME
        || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK)
        && (unsafe { empty_destination(destination_filename) }
            || path_from(destination_filename).is_none())
    {
        return HR_NOT_SAME_DEVICE;
    }
    let result = if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED
        || notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN
    {
        if !operation_parameters.is_null() {
            // SAFETY: ProjFS supplies a writable notification-parameter union
            // for post-create notifications. Preserve close-boundary capture
            // even when the new item acquires a per-file notification mask.
            unsafe {
                (*operation_parameters).PostCreate.NotificationMask =
                    PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
                        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
                        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED
                        | PRJ_NOTIFY_PRE_DELETE
                        | PRJ_NOTIFY_PRE_RENAME
                        | PRJ_NOTIFY_FILE_RENAMED
                        | PRJ_NOTIFY_PRE_SET_HARDLINK
                        | PRJ_NOTIFY_HARDLINK_CREATED;
            }
        }
        if is_directory {
            runtime.source.capture_host_path(&runtime.root, &path)
        } else {
            Ok(())
        }
    } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
        || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    {
        runtime.source.capture_host_path(&runtime.root, &path)
    } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION {
        match runtime.source.lookup(&path) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => runtime.source.capture_host_path(&runtime.root, &path),
            Err(error) => Err(error),
        }
    } else if notification == PRJ_NOTIFICATION_FILE_RENAMED {
        path_from(destination_filename)
            .ok_or_else(|| {
                super::MountSourceError::Invalid("rename destination is invalid".to_owned())
            })
            .and_then(|destination| runtime.source.rename(&path, &destination, true))
            .and_then(|()| runtime.source.flush())
    } else if notification == PRJ_NOTIFICATION_HARDLINK_CREATED {
        path_from(destination_filename)
            .ok_or_else(|| {
                super::MountSourceError::Invalid("hard-link destination is invalid".to_owned())
            })
            .and_then(|destination| runtime.source.hard_link(&path, &destination))
            .and_then(|()| runtime.source.flush())
    } else if notification == PRJ_NOTIFICATION_PRE_DELETE
        || notification == PRJ_NOTIFICATION_PRE_RENAME
        || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
    {
        Ok(())
    } else {
        Err(super::MountSourceError::Unsupported(
            "ProjFS emitted an unadmitted write notification".to_owned(),
        ))
    };
    match result {
        Ok(()) => HR_OK,
        Err(super::MountSourceError::NotFound) => HR_FILE_NOT_FOUND,
        Err(super::MountSourceError::AlreadyExists) => HR_ALREADY_EXISTS,
        Err(super::MountSourceError::Invalid(_)) => HR_INVALID_DATA,
        Err(super::MountSourceError::Unsupported(_)) => HR_NOT_SUPPORTED,
        Err(super::MountSourceError::Engine(_) | super::MountSourceError::Stale) => HR_UNEXPECTED,
    }
}

unsafe extern "system" fn cancel(_callback_data: *const PRJ_CALLBACK_DATA) {}

fn callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(start_directory),
        EndDirectoryEnumerationCallback: Some(end_directory),
        GetDirectoryEnumerationCallback: Some(get_directory),
        GetPlaceholderInfoCallback: Some(placeholder),
        GetFileDataCallback: Some(file_data),
        QueryFileNameCallback: Some(query_name),
        NotificationCallback: Some(notification),
        CancelCommandCallback: Some(cancel),
    }
}
