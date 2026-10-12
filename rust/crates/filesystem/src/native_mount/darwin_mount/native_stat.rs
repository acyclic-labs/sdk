use super::*;

impl DarwinMountContext {
    pub(super) fn attributes(
        &self,
        path: &MountPath,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<NativeStat, NativeMountError> {
        // Sampled before observing: a change after it cannot keep the label.
        let stamp = self.cache_stamp(work)?;
        let lookup = self.lookup_handle(path, handle, work)?;
        self.attributes_from_lookup(path, lookup, stamp, work)
    }

    pub(super) fn attributes_from_lookup(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<NativeStat, NativeMountError> {
        let node = lookup.node;
        let file_kind = match node.kind {
            MountNodeKind::Regular => mode::IFREG,
            MountNodeKind::Directory => mode::IFDIR,
            MountNodeKind::SymbolicLink => mode::IFLNK,
            MountNodeKind::Fifo => mode::IFIFO,
            MountNodeKind::Socket => mode::IFSOCK,
            MountNodeKind::CharacterDevice => mode::IFCHR,
            MountNodeKind::BlockDevice => mode::IFBLK,
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
