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
    /// NFS change attribute.
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
    pub(super) fn acyclic_fs_darwin_mount_invalidate(
        session: *mut c_void,
        path: *const c_char,
    ) -> c_int;
    pub(super) fn acyclic_fs_darwin_mount_attribute_timeout() -> u32;
    pub(super) fn acyclic_fs_darwin_mount_fill_directory(
        buffer: *mut c_void,
        filler: DirectoryFiller,
        name: *const c_char,
        attributes: *const NativeStat,
        next_offset: i64,
    ) -> c_int;
}

impl DarwinMountContext {
    pub(super) fn attributes(
        &self,
        path: &MountPath,
        handle: u64,
        work: &Work,
    ) -> Result<NativeStat, NativeMountError> {
        let stamp = self.cache_stamp(work)?;
        let lookup = self.lookup_handle(path, handle, work)?;
        self.attributes_from_lookup(path, lookup, stamp, work)
    }

    pub(super) fn attributes_from_lookup(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>,
        work: &Work,
    ) -> Result<NativeStat, NativeMountError> {
        let node = lookup.node;
        let file_kind = match node.kind {
            MountNodeKind::Regular => libc::S_IFREG as u32,
            MountNodeKind::Directory => libc::S_IFDIR as u32,
            MountNodeKind::SymbolicLink => libc::S_IFLNK as u32,
            MountNodeKind::Fifo => libc::S_IFIFO as u32,
            MountNodeKind::Socket => libc::S_IFSOCK as u32,
            MountNodeKind::CharacterDevice => libc::S_IFCHR as u32,
            MountNodeKind::BlockDevice => libc::S_IFBLK as u32,
            MountNodeKind::Unsupported => return Err(driver_errno(libc::EOPNOTSUPP)),
        };
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        let mode = metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777;
        let (accessed_seconds, accessed_nanoseconds) = metadata_time(lookup.metadata.accessed_ns);
        let (modified_seconds, modified_nanoseconds) = metadata_time(lookup.metadata.modified_ns);
        let (changed_seconds, changed_nanoseconds) = metadata_time(lookup.metadata.changed_ns);
        let (created_seconds, created_nanoseconds) = metadata_time(lookup.metadata.created_ns);
        Ok(NativeStat {
            inode: self.inode(node.file_id, work)?,
            logical_bytes: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            accessed_seconds,
            accessed_nanoseconds,
            modified_seconds,
            modified_nanoseconds,
            changed_seconds,
            changed_nanoseconds,
            created_seconds,
            created_nanoseconds,
            mode: file_kind | mode,
            link_count: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, self.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, self.mount_gid),
            device: node
                .device
                .map(|(major, minor)| {
                    crate::native_mount::device::join_device(major, minor)
                        .map(|device| u64::from(u32::from_ne_bytes(device.to_ne_bytes())))
                })
                .transpose()
                .map_err(driver_errno)?
                .unwrap_or(0),
            block_size: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
            change: self.change_label(path, lookup, stamp, work)?,
        })
    }
}
