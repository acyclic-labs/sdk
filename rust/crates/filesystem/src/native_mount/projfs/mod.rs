//! Audited Windows `ProjFS` FFI with exact close-boundary authored capture.

#![allow(
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    reason = "audited Windows ProjFS FFI"
)]

use super::provider_stack;
use super::{
    DIRECTORY_PAGE_SIZE, MountContentPin, MountFilesystem, MountLookup, MountNode, MountNodeKind,
    MountPath, MountSourceError, NativeMountError, NativeMountRequest, ViewObserver, ViewOrigin,
    ViewStamp,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use crate::native_host::HostRoot;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_ARCHIVE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
    FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT,
};
use windows::Win32::Storage::ProjectedFileSystem::{
    PRJ_CALLBACK_DATA, PRJ_CALLBACKS, PRJ_DIR_ENTRY_BUFFER_HANDLE, PRJ_EXT_INFO_TYPE_SYMLINK,
    PRJ_EXTENDED_INFO, PRJ_EXTENDED_INFO_0, PRJ_EXTENDED_INFO_0_0, PRJ_FILE_BASIC_INFO,
    PRJ_FLAG_USE_NEGATIVE_PATH_CACHE, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, PRJ_NOTIFICATION,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFICATION_FILE_OPENED,
    PRJ_NOTIFICATION_FILE_OVERWRITTEN, PRJ_NOTIFICATION_FILE_RENAMED,
    PRJ_NOTIFICATION_HARDLINK_CREATED, PRJ_NOTIFICATION_MAPPING, PRJ_NOTIFICATION_NEW_FILE_CREATED,
    PRJ_NOTIFICATION_PARAMETERS, PRJ_NOTIFICATION_PRE_RENAME, PRJ_NOTIFICATION_PRE_SET_HARDLINK,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED, PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFY_FILE_OPENED,
    PRJ_NOTIFY_FILE_OVERWRITTEN, PRJ_NOTIFY_FILE_RENAMED, PRJ_NOTIFY_HARDLINK_CREATED,
    PRJ_NOTIFY_NEW_FILE_CREATED, PRJ_NOTIFY_PRE_RENAME, PRJ_NOTIFY_PRE_SET_HARDLINK,
    PRJ_NOTIFY_TYPES, PRJ_PLACEHOLDER_INFO, PRJ_STARTVIRTUALIZING_OPTIONS,
    PRJ_UPDATE_ALLOW_READ_ONLY, PRJ_UPDATE_ALLOW_TOMBSTONE, PRJ_UPDATE_TYPES, PrjDeleteFile,
    PrjFileNameCompare, PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    PrjWritePlaceholderInfo, PrjWritePlaceholderInfo2,
};
use windows::core::{GUID, HRESULT, HSTRING, PCWSTR};

const HR_OK: HRESULT = HRESULT(0);
const HR_FILE_NOT_FOUND: HRESULT = HRESULT(0x8007_0002_u32.cast_signed());
const HR_PATH_NOT_FOUND: HRESULT = HRESULT(0x8007_0003_u32.cast_signed());
const HR_ALREADY_EXISTS: HRESULT = HRESULT(0x8007_00b7_u32.cast_signed());
const HR_FILE_EXISTS: HRESULT = HRESULT(0x8007_0050_u32.cast_signed());
const HR_DIRECTORY: HRESULT = HRESULT(0x8007_010b_u32.cast_signed());
const HR_NOT_SAME_DEVICE: HRESULT = HRESULT(0x8007_0011_u32.cast_signed());
const HR_INVALID_DATA: HRESULT = HRESULT(0x8007_000d_u32.cast_signed());
const HR_NOT_SUPPORTED: HRESULT = HRESULT(0x8007_0032_u32.cast_signed());
const HR_UNEXPECTED: HRESULT = HRESULT(0x8000_ffff_u32.cast_signed());
const HR_FILE_INVALID: HRESULT = HRESULT(0x8007_03ee_u32.cast_signed());
const HR_IO_DEVICE: HRESULT = HRESULT(0x8007_045d_u32.cast_signed());
const HR_VIRTUALIZATION_INVALID_OPERATION: HRESULT = HRESULT(0x8007_0181_u32.cast_signed());
const HR_SHARING_VIOLATION: HRESULT = HRESULT(0x8007_0020_u32.cast_signed());
const HR_DIRECTORY_NOT_EMPTY: HRESULT = HRESULT(0x8007_0091_u32.cast_signed());

const FILE_NOTIFICATIONS: PRJ_NOTIFY_TYPES = PRJ_NOTIFY_TYPES(
    PRJ_NOTIFY_NEW_FILE_CREATED.0
        | PRJ_NOTIFY_FILE_OVERWRITTEN.0
        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED.0
        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED.0
        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION.0
        | PRJ_NOTIFY_FILE_OPENED.0
        | PRJ_NOTIFY_PRE_RENAME.0
        | PRJ_NOTIFY_FILE_RENAMED.0
        | PRJ_NOTIFY_PRE_SET_HARDLINK.0
        | PRJ_NOTIFY_HARDLINK_CREATED.0,
);
const NOTIFICATION_ROOT: [u16; 1] = [0];
const WRITE_CHUNK_BYTES: u32 = 1 << 20;
const MAXIMUM_CACHED_BINDINGS: usize = 16_384;
mod directory;
use directory::*;
mod context;
pub use context::NativeMountWorkScope;
use context::*;
mod namespace;
pub(super) use namespace::*;
mod callbacks;
use callbacks::*;
mod ownership;
use ownership::*;
mod projector_state;
use projector_state::*;
mod placeholders;
mod projector;
use placeholders::*;
mod file_writer;
use file_writer::*;
mod staging;
use staging::*;
mod replacement;
use replacement::*;
mod session;
pub(super) use session::*;
mod lifecycle;
mod recovery;
pub(super) use recovery::*;
mod recovery_control;
pub(super) use recovery_control::*;
mod locks;
use locks::*;
mod metadata;
use metadata::*;
mod notifications;
use notifications::*;

#[cfg(test)]
mod tests;
