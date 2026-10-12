use super::*;

impl FuseProjection {
    fn reply_directory(&self, inode: u64, offset: u64, reply: DirectoryReply, span: &tracing::Span) {
        let mut reply = Some(reply);
        let result = self.core.request(|work| self.list_directory(inode, offset, &mut reply, work));
        if let Err(error) = self.core.finish(result) {
            if let Some(reply) = reply { reply.error(errno_on(span, error)); }
        }
    }
}

#[allow(clippy::too_many_arguments, reason = "fuser fixes callback signatures")]
impl Filesystem for FuseProjection {
    fn init(&mut self, _: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let result = self.core.request(|work| {
            let source = self.core.source.as_ref();
            let skips_opendir = config.capabilities().contains(InitFlags::FUSE_NO_OPENDIR_SUPPORT)
                && ProjectionState::directory_open_flags(source, work)?
                    .contains(FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE);
            let skips_open = config.capabilities().contains(InitFlags::FUSE_NO_OPEN_SUPPORT)
                && source.view_stamp_with_meter(work)?.is_some() && !source.flush_on_handle_close();
            config.add_capabilities(REQUESTED_CAPABILITIES & config.capabilities())
                .map_err(NativeMountError::FuseUnsupportedCapabilities)?;
            Ok((skips_opendir, skips_open))
        });
        (self.skips_opendir, self.skips_open) = self.core.finish(result)
            .map_err(std::io::Error::from_raw_os_error)?;
        Ok(())
    }

    fn lookup(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let span = callback_span!(TRACE, lookup, ino = parent.0);
        respond!(self.core, reply, span, work, self.lookup_entry(parent.0, name, work),
            |entry| reply.entry(&entry.ttl, &entry.attr, Generation(0)));
    }

    fn getattr(&self, _: &Request, inode: INodeNo, fh: Option<FuseFileHandle>, reply: ReplyAttr) {
        let span = callback_span!(TRACE, getattr, ino = inode.0);
        respond!(self.core, reply, span, work,
            self.node_attributes(inode.0, fh.map(|fh| fh.0).filter(|fh| *fh != 0), work),
            |(attr, ttl)| reply.attr(&ttl, &attr));
    }

    fn forget(&self, _: &Request, inode: INodeNo, nlookup: u64) {
        let _span = callback_span!(TRACE, forget, ino = inode.0);
        let result = self.core.request(|work|
            self.core.state_to_finish().release_lookup_reference(inode.0, nlookup, work));
        let _ = self.core.finish(result);
    }

    fn readlink(&self, _: &Request, inode: INodeNo, reply: ReplyData) {
        let span = callback_span!(TRACE, readlink, ino = inode.0);
        respond!(self.core, reply, span, work, self.read_link(inode.0, work),
            |target| reply.data(&target.bytes));
    }

    fn setattr(&self, _: &Request, inode: INodeNo, mode: Option<u32>, uid: Option<u32>,
        gid: Option<u32>, size: Option<u64>, atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>, fh: Option<FuseFileHandle>, crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>, bkuptime: Option<SystemTime>, flags: Option<BsdFileFlags>, reply: ReplyAttr,
    ) {
        let span = callback_span!(DEBUG, setattr, ino = inode.0);
        let changes = AttributeChanges { mode, uid, gid, size, atime, mtime,
            changed: chgtime.or(ctime), crtime, flags: flags.map(|flags| flags.bits()) };
        respond!(self.core, reply, span, work, {
            if bkuptime.is_some() { return Err(libc::EOPNOTSUPP.into()); }
            self.set_attributes(inode.0, fh.map(|fh| fh.0).filter(|fh| *fh != 0), &changes, work)
        }, |(attr, ttl)| reply.attr(&ttl, &attr));
    }

    fn mkdir(&self, request: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let span = callback_span!(TRACE, mkdir, ino = parent.0);
        let metadata = create_metadata(mode & !umask, S_IFDIR, request.uid(), request.gid());
        respond!(self.core, reply, span, work,
            self.create_node(parent.0, name, |path, work|
                self.core.source.create_directory(path, metadata, work).map_err(NativeMountError::from), work),
            |entry| reply.entry(&entry.ttl, &entry.attr, Generation(0)));
    }

    fn mknod(&self, request: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32,
        rdev: u32, reply: ReplyEntry,
    ) {
        let span = callback_span!(TRACE, mknod, ino = parent.0);
        let metadata = create_metadata(mode & !umask, mode & S_IFMT, request.uid(), request.gid());
        respond!(self.core, reply, span, work, {
            let (kind, device) = match mode & S_IFMT {
                S_IFIFO => (MountNodeKind::Fifo, None),
                S_IFSOCK => (MountNodeKind::Socket, None),
                S_IFCHR => (MountNodeKind::CharacterDevice, Some(crate::native_mount::device::split_device(u64::from(rdev)))),
                S_IFBLK => (MountNodeKind::BlockDevice, Some(crate::native_mount::device::split_device(u64::from(rdev)))),
                _ => return Err(libc::EOPNOTSUPP.into()),
            };
            self.create_node(parent.0, name, |path, work|
                self.core.source.create_special(path, kind, device, metadata, work).map_err(NativeMountError::from), work)
        }, |entry| reply.entry(&entry.ttl, &entry.attr, Generation(0)));
    }

    fn symlink(&self, request: &Request, parent: INodeNo, name: &OsStr, target: &Path, reply: ReplyEntry) {
        let span = callback_span!(TRACE, symlink, ino = parent.0);
        let metadata = create_metadata(0o777, S_IFLNK, request.uid(), request.gid());
        respond!(self.core, reply, span, work, self.create_node(parent.0, name, |path, work| {
            let target = fuse_source_chunk(target.as_os_str().as_bytes(), work)?;
            self.core.source.create_symbolic_link(path, target, metadata, work).map_err(NativeMountError::from)
        }, work), |entry| reply.entry(&entry.ttl, &entry.attr, Generation(0)));
    }

    fn unlink(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let span = callback_span!(TRACE, unlink, ino = parent.0);
        respond!(self.core, reply, span, work, self.remove_name(parent.0, name, work), |()| reply.ok());
    }

    fn rmdir(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let span = callback_span!(TRACE, rmdir, ino = parent.0);
        respond!(self.core, reply, span, work, self.remove_name(parent.0, name, work), |()| reply.ok());
    }

    fn rename(&self, _: &Request, parent: INodeNo, name: &OsStr, new_parent: INodeNo,
        new_name: &OsStr, flags: RenameFlags, reply: ReplyEmpty,
    ) {
        let span = callback_span!(DEBUG, rename, ino = parent.0);
        respond!(self.core, reply, span, work,
            self.rename_name(parent.0, name, new_parent.0, new_name, flags.bits(), work), |()| reply.ok());
    }

    fn link(&self, _: &Request, inode: INodeNo, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let span = callback_span!(TRACE, link, ino = inode.0);
        respond!(self.core, reply, span, work, self.link_name(inode.0, parent.0, name, work),
            |entry| reply.entry(&entry.ttl, &entry.attr, Generation(0)));
    }

    fn open(&self, _: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let span = callback_span!(TRACE, open, ino = inode.0);
        respond!(self.core, reply, span, work, {
            if self.skips_open { return Err(libc::ENOSYS.into()); }
            let _names = self.core.names()?;
            self.open_node(inode.0, flags.0, true, work)
        }, |(handle, flags)| reply.opened(FuseFileHandle(handle), flags));
    }

    fn read(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, offset: u64, size: u32,
        _: OpenFlags, _: Option<LockOwner>, reply: ReplyData,
    ) {
        let span = callback_span!(TRACE, read, ino = inode.0, fh = fh.0, offset = offset, len = size);
        respond!(self.core, reply, span, work, self.read_through(inode.0, fh.0, offset, size, work),
            |bytes| reply.data(&bytes.bytes));
    }

    fn write(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, offset: u64, data: &[u8],
        _: WriteFlags, _: OpenFlags, _: Option<LockOwner>, reply: ReplyWrite,
    ) {
        let span = callback_span!(TRACE, write, ino = inode.0, fh = fh.0, offset = offset, len = data.len());
        respond!(self.core, reply, span, work, self.with_handle(inode.0, fh.0, Access::Write,
            |handle| self.write_handle(inode.0, handle, offset, data, work), work), |written| reply.written(written));
    }

    fn fsync(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, _: bool, reply: ReplyEmpty) {
        let span = callback_span!(DEBUG, fsync, ino = inode.0, fh = fh.0);
        respond!(self.core, reply, span, work, self.with_handle(inode.0, fh.0, Access::Read,
            |handle| self.flush_handle(inode.0, handle, true, false, work), work), |()| reply.ok());
    }

    fn flush(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, _: LockOwner, reply: ReplyEmpty) {
        let span = callback_span!(DEBUG, flush, ino = inode.0, fh = fh.0);
        respond!(self.core, reply, span, work, {
            if self.skips_open && fh.0 == 0 { return Err(libc::ENOSYS.into()); }
            self.flush_handle(inode.0, fh.0, false, false, work)
        }, |()| reply.ok());
    }

    fn release(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, _: OpenFlags,
        _: Option<LockOwner>, _: bool, reply: ReplyEmpty,
    ) {
        let span = callback_span!(DEBUG, release, ino = inode.0, fh = fh.0);
        respond!(self.core, reply, span, work, self.flush_handle(inode.0, fh.0, false, true, work), |()| reply.ok());
    }

    fn opendir(&self, _: &Request, inode: INodeNo, _: OpenFlags, reply: ReplyOpen) {
        let span = callback_span!(TRACE, opendir, ino = inode.0);
        respond!(self.core, reply, span, work, {
            if self.skips_opendir { return Err(libc::ENOSYS.into()); }
            ProjectionState::directory_open_flags(self.core.source.as_ref(), work)
        }, |flags| reply.opened(FuseFileHandle(0), flags));
    }

    fn readdir(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, offset: u64, reply: ReplyDirectory) {
        let span = callback_span!(TRACE, readdir, ino = inode.0, fh = fh.0, offset = offset);
        self.reply_directory(inode.0, offset, DirectoryReply::Basic(reply), &span);
    }

    fn readdirplus(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, offset: u64, reply: ReplyDirectoryPlus) {
        let span = callback_span!(TRACE, readdirplus, ino = inode.0, fh = fh.0, offset = offset);
        self.reply_directory(inode.0, offset, DirectoryReply::Plus(reply), &span);
    }

    fn releasedir(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, _: OpenFlags, reply: ReplyEmpty) {
        let span = callback_span!(TRACE, releasedir, ino = inode.0, fh = fh.0);
        respond!(self.core, reply, span, _work, Ok::<_, NativeMountError>(()), |()| reply.ok());
    }

    fn fsyncdir(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, _: bool, reply: ReplyEmpty) {
        let span = callback_span!(TRACE, fsyncdir, ino = inode.0, fh = fh.0);
        respond!(self.core, reply, span, work,
            self.source_write(work, || self.core.source.flush(work).map_err(NativeMountError::from)), |()| reply.ok());
    }

    fn setxattr(&self, _: &Request, inode: INodeNo, name: &OsStr, value: &[u8], flags: i32,
        position: u32, reply: ReplyEmpty,
    ) {
        let span = callback_span!(TRACE, setxattr, ino = inode.0);
        respond!(self.core, reply, span, work,
            self.write_named_attribute(inode.0, name, value, flags, position, work), |()| reply.ok());
    }

    fn getxattr(&self, _: &Request, inode: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let span = callback_span!(TRACE, getxattr, ino = inode.0);
        respond!(self.core, reply, span, work, self.read_named_attribute(inode.0, name, work),
            |value| reply_xattr(reply, &value.bytes, size, &span));
    }

    fn listxattr(&self, _: &Request, inode: INodeNo, size: u32, reply: ReplyXattr) {
        let span = callback_span!(TRACE, listxattr, ino = inode.0);
        respond!(self.core, reply, span, work, self.list_named_attributes(inode.0, work),
            |value| reply_xattr(reply, &value, size, &span));
    }

    fn removexattr(&self, _: &Request, inode: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let span = callback_span!(TRACE, removexattr, ino = inode.0);
        respond!(self.core, reply, span, work, self.remove_named_attribute(inode.0, name, work), |()| reply.ok());
    }

    fn fallocate(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, offset: u64, length: u64,
        mode: i32, reply: ReplyEmpty,
    ) {
        let span = callback_span!(TRACE, fallocate, ino = inode.0, fh = fh.0, offset = offset, len = length);
        respond!(self.core, reply, span, work, self.with_handle(inode.0, fh.0, Access::Write,
            |handle| self.allocate(inode.0, handle, offset, length, mode, work), work), |()| reply.ok());
    }

    fn lseek(&self, _: &Request, inode: INodeNo, fh: FuseFileHandle, offset: i64, whence: i32, reply: ReplyLseek) {
        let span = callback_span!(TRACE, lseek, ino = inode.0, fh = fh.0, offset = offset);
        respond!(self.core, reply, span, work, self.with_handle(inode.0, fh.0, Access::Read,
            |handle| self.seek(inode.0, handle, offset, whence, work), work), |found| reply.offset(found));
    }

    fn copy_file_range(&self, _: &Request, inode_in: INodeNo, fh_in: FuseFileHandle, offset_in: u64,
        inode_out: INodeNo, fh_out: FuseFileHandle, offset_out: u64, length: u64, flags: CopyFileRangeFlags, reply: ReplyWrite,
    ) {
        let span = callback_span!(TRACE, copy_file_range, len = length);
        respond!(self.core, reply, span, work, self.with_handle(inode_in.0, fh_in.0, Access::Read, |handle_in|
            self.with_handle(inode_out.0, fh_out.0, Access::Write, |handle_out|
                self.copy_range(inode_in.0, handle_in, offset_in, inode_out.0, handle_out, offset_out, length, flags.bits(), work), work),
            work), |written| reply.written(written));
    }

    fn create(&self, request: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        let span = callback_span!(DEBUG, create, ino = parent.0);
        respond!(self.core, reply, span, work, self.create_file(request, parent.0, name, mode & !umask, flags, work),
            |(entry, handle, flags)| reply.created(&entry.ttl, &entry.attr, Generation(0), FuseFileHandle(handle), flags));
    }
}
