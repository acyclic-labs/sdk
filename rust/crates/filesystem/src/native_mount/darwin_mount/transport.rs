use super::*;

#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct NativeStat {
    pub(super) inode: u64,
    pub(super) logical_bytes: u64,
    pub(super) blocks: u64,
    pub(super) accessed_seconds: i64,
    pub(super) accessed_nanoseconds: u32,
    pub(super) modified_seconds: i64,
    pub(super) modified_nanoseconds: u32,
    pub(super) changed_seconds: i64,
    pub(super) changed_nanoseconds: u32,
    pub(super) created_seconds: i64,
    pub(super) created_nanoseconds: u32,
    pub(super) mode: u32,
    pub(super) link_count: u32,
    pub(super) uid: u32,
    pub(super) gid: u32,
    pub(super) device: u64,
    pub(super) block_size: u32,
    pub(super) flags: u32,
    /// NFS change attribute: see [`ChangeLabels`].
    pub(super) change: u64,
}

#[repr(C)]
pub(super) struct NativeTimes {
    pub(super) accessed_seconds: i64,
    pub(super) accessed_nanoseconds: i64,
    pub(super) modified_seconds: i64,
    pub(super) modified_nanoseconds: i64,
}

pub(super) type DirectoryFiller = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const libc::stat,
    libc::off_t,
    c_int,
) -> c_int;

unsafe extern "C" {
    pub(super) fn acyclic_fs_darwin_mount_session_allocation_size() -> usize;
    pub(super) fn acyclic_fs_darwin_mount_session_new() -> *mut c_void;
    pub(super) fn acyclic_fs_darwin_mount_session_free(session: *mut c_void);
    pub(super) fn acyclic_fs_darwin_mount_run(
        session: *mut c_void,
        argc: c_int,
        argv: *const *const c_char,
        mountpoint: *const c_char,
        context: usize,
        mounted: unsafe extern "C" fn(*mut c_void),
        mounted_argument: *mut c_void,
        interrupted_before_namespace: *mut c_int,
    ) -> c_int;
    pub(super) fn acyclic_fs_darwin_mount_interrupt(session: *mut c_void);
    pub(super) fn acyclic_fs_darwin_mount_invalidate(session: *mut c_void, path: *const c_char) -> c_int;
    pub(super) fn acyclic_fs_darwin_mount_attribute_timeout() -> u32;
    pub(super) fn acyclic_fs_darwin_mount_fill_directory(
        buffer: *mut c_void,
        filler: DirectoryFiller,
        name: *const c_char,
        attributes: *const NativeStat,
        next_offset: i64,
    ) -> c_int;
}
