//! Linux FUSE projection over the common callback contract.

use super::{
    MountDirectoryEntry, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest,
};
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen, ReplyWrite,
    ReplyXattr, Request, TimeOrNow,
};
use std::collections::{HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const ROOT_INODE: u64 = 1;
const TTL: Duration = Duration::from_secs(1);
/// FUSE RENAME2 wire flag; equals Linux renameat2's `RENAME_NOREPLACE`.
const RENAME_NOREPLACE: u32 = 1;
// FUSE hands modes as `u32` while Darwin's `mode_t` is `u16`; widen the file
// type masks once so match arms and metadata stay wire-width on every host.
#[allow(clippy::unnecessary_cast)]
mod mode {
    pub(super) const S_IFMT: u32 = libc::S_IFMT as u32;
    pub(super) const S_IFIFO: u32 = libc::S_IFIFO as u32;
    pub(super) const S_IFSOCK: u32 = libc::S_IFSOCK as u32;
    pub(super) const S_IFCHR: u32 = libc::S_IFCHR as u32;
    pub(super) const S_IFBLK: u32 = libc::S_IFBLK as u32;
    pub(super) const S_IFDIR: u32 = libc::S_IFDIR as u32;
    pub(super) const S_IFLNK: u32 = libc::S_IFLNK as u32;
    pub(super) const S_IFREG: u32 = libc::S_IFREG as u32;
}
use mode::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFREG, S_IFSOCK};
const DIRECTORY_PAGE_SIZE: u32 = 256;
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;
const DETACHED_COPY_CHUNK_BYTES: u32 = 1024 * 1024;

struct DirectoryHandle {
    path: MountPath,
    parent_inode: u64,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    exhausted: bool,
    emitted: i64,
}

struct InodeEntry {
    bindings: Vec<MountPath>,
    lookup: MountLookup,
    lookup_references: u64,
    open_handles: u64,
}

struct FileHandle {
    inode: u64,
    open_file: Arc<dyn MountOpenFile>,
}

struct FuseProjection {
    source: Arc<dyn MountFilesystem>,
    writable: bool,
    fallback_uid: u32,
    fallback_gid: u32,
    next_inode: u64,
    next_handle: u64,
    by_inode: HashMap<u64, InodeEntry>,
    inode_by_file: HashMap<crate::FileId, u64>,
    files: HashMap<u64, FileHandle>,
    directories: HashMap<u64, DirectoryHandle>,
}

/// One background libfuse session.
pub(super) struct FuseSession {
    session: Option<BackgroundSession>,
}

impl FuseSession {
    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, NativeMountError> {
        let root = source
            .lookup(&MountPath::root())
            .map_err(source_error)?
            .ok_or_else(|| NativeMountError::Driver("volume root is absent".to_owned()))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(NativeMountError::Driver(
                "volume root is not a directory".to_owned(),
            ));
        }
        let mut by_inode = HashMap::new();
        by_inode.insert(
            ROOT_INODE,
            InodeEntry {
                bindings: vec![MountPath::root()],
                lookup: root,
                lookup_references: 1,
                open_handles: 0,
            },
        );
        let mut inode_by_file = HashMap::new();
        inode_by_file.insert(root.node.file_id, ROOT_INODE);
        let destination_metadata = request
            .destination
            .metadata()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let filesystem = FuseProjection {
            source,
            writable: request.writable,
            fallback_uid: destination_metadata.uid(),
            fallback_gid: destination_metadata.gid(),
            next_inode: ROOT_INODE + 1,
            next_handle: 1,
            by_inode,
            inode_by_file,
            files: HashMap::new(),
            directories: HashMap::new(),
        };
        let mut options = vec![
            MountOption::FSName("acyclic-fs".to_owned()),
            MountOption::DefaultPermissions,
            MountOption::NoAtime,
        ];
        if !request.writable {
            options.push(MountOption::RO);
        }
        let session = fuser::spawn_mount2(filesystem, &request.destination, &options)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Ok(Self {
            session: Some(session),
        })
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        drop(self.session.take());
        Ok(())
    }
}

impl FuseProjection {
    fn allocate_file_handle(&mut self, inode: u64) -> Result<u64, i32> {
        let path = self.path(inode)?.to_owned();
        let open_file = self.source.open_file(&path).map_err(errno)?;
        let handle = self.next_handle;
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        if self.files.contains_key(&handle) {
            return Err(libc::EMFILE);
        }
        self.files.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        self.files.insert(handle, FileHandle { inode, open_file });
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        entry.open_handles = entry.open_handles.saturating_add(1);
        Ok(handle)
    }

    fn open_handle(&self, inode: u64, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        Ok(Arc::clone(&file.open_file))
    }

    fn open_inode(&self, inode: u64) -> Option<Arc<dyn MountOpenFile>> {
        self.files
            .values()
            .find(|file| file.inode == inode)
            .map(|file| Arc::clone(&file.open_file))
    }

    fn refresh_handle(&mut self, inode: u64, handle: Option<u64>) -> Result<MountLookup, i32> {
        if let Some(handle) = handle {
            let lookup = self.open_handle(inode, handle)?.lookup().map_err(errno)?;
            self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?.lookup = lookup;
            return Ok(lookup);
        }
        self.refresh(inode)
    }

    fn retain_detached_handles(&mut self, inode: u64, detached: &Arc<dyn MountOpenFile>) {
        let mut assigned = 0_u64;
        for file in self.files.values_mut().filter(|file| file.inode == inode) {
            file.open_file = Arc::clone(detached);
            assigned = assigned.saturating_add(1);
        }
        let expected = self
            .by_inode
            .get(&inode)
            .map_or(0, |entry| entry.open_handles);
        debug_assert_eq!(assigned, expected);
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_open_range(
        &mut self,
        source_inode: u64,
        source_handle: u64,
        source_offset: u64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: u64,
        length: u64,
    ) -> Result<u32, i32> {
        let source_open = self.open_handle(source_inode, source_handle)?;
        let destination_open = self.open_handle(destination_inode, destination_handle)?;
        let source_lookup = source_open.lookup().map_err(errno)?;
        let destination_lookup = destination_open.lookup().map_err(errno)?;
        let source_size = source_lookup.node.logical_bytes;
        let copy_length = length.min(source_size.saturating_sub(source_offset));
        if copy_length == 0 {
            return Ok(0);
        }
        if source_lookup.node.link_count != 0 && destination_lookup.node.link_count != 0 {
            self.source
                .clone_range_by_id(
                    source_lookup.node.file_id,
                    source_offset,
                    destination_lookup.node.file_id,
                    destination_offset,
                    copy_length,
                )
                .map_err(errno)?;
            let _ = self.refresh(destination_inode);
            return u32::try_from(copy_length).map_err(|_| libc::EOVERFLOW);
        }
        let same_open_file = Arc::ptr_eq(&source_open, &destination_open);
        let backwards = same_open_file
            && destination_offset > source_offset
            && destination_offset < source_offset.saturating_add(copy_length);
        let mut transferred = 0_u64;
        while transferred < copy_length {
            let remaining = copy_length - transferred;
            let chunk = remaining.min(u64::from(DETACHED_COPY_CHUNK_BYTES));
            let relative = if backwards {
                remaining - chunk
            } else {
                transferred
            };
            let chunk = u32::try_from(chunk).unwrap_or(DETACHED_COPY_CHUNK_BYTES);
            let sparse = match source_open.read_sparse_range(source_offset + relative, chunk) {
                Ok(sparse) => sparse,
                Err(error) if transferred == 0 => return Err(errno(error)),
                Err(_) => break,
            };
            let copied = sparse.logical_bytes;
            if let Err(error) = destination_open
                .write_sparse_range(destination_offset + relative, &sparse)
                .map_err(errno)
            {
                if transferred == 0 {
                    return Err(error);
                }
                break;
            }
            transferred = transferred.saturating_add(copied);
            if copied < u64::from(chunk) {
                break;
            }
        }
        let _ = self.refresh_handle(destination_inode, Some(destination_handle));
        u32::try_from(transferred).map_err(|_| libc::EOVERFLOW)
    }

    fn path(&self, inode: u64) -> Result<&MountPath, i32> {
        self.by_inode
            .get(&inode)
            .and_then(|entry| entry.bindings.first())
            .ok_or(libc::ESTALE)
    }

    fn node(&self, inode: u64) -> Result<MountNode, i32> {
        self.by_inode
            .get(&inode)
            .map(|entry| entry.lookup.node)
            .ok_or(libc::ESTALE)
    }

    fn child_path(&self, parent: u64, name: &OsStr) -> Result<MountPath, i32> {
        let parent = self.path(parent)?;
        let name = name.as_bytes();
        if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
            return Err(libc::EINVAL);
        }
        Ok(parent.child(name.to_vec()))
    }

    fn intern_with_reference(
        &mut self,
        path: MountPath,
        lookup: &MountLookup,
        lookup_reference: bool,
    ) -> Result<u64, i32> {
        intern_projected(
            &mut self.next_inode,
            &mut self.by_inode,
            &mut self.inode_by_file,
            path,
            lookup,
            lookup_reference,
        )
    }

    fn intern(&mut self, path: MountPath, lookup: &MountLookup) -> Result<u64, i32> {
        self.intern_with_reference(path, lookup, true)
    }

    fn intern_enumerated(&mut self, path: MountPath, lookup: &MountLookup) -> Result<u64, i32> {
        self.intern_with_reference(path, lookup, false)
    }

    fn refresh(&mut self, inode: u64) -> Result<MountLookup, i32> {
        let (file_id, bindings) = self
            .by_inode
            .get(&inode)
            .map(|entry| (entry.lookup.node.file_id, entry.bindings.clone()))
            .ok_or(libc::ESTALE)?;
        for path in bindings {
            if let Some(lookup) = self.source.lookup(&path).map_err(errno)? {
                if lookup.node.file_id != file_id {
                    continue;
                }
                self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?.lookup = lookup;
                return Ok(lookup);
            }
            self.remove_binding(inode, &path);
        }
        Err(libc::ENOENT)
    }

    fn attr(&self, inode: u64, lookup: &MountLookup) -> Result<FileAttr, i32> {
        let node = lookup.node;
        let kind = match node.kind {
            MountNodeKind::Regular => FileType::RegularFile,
            MountNodeKind::Directory => FileType::Directory,
            MountNodeKind::SymbolicLink => FileType::Symlink,
            MountNodeKind::Fifo => FileType::NamedPipe,
            MountNodeKind::Socket => FileType::Socket,
            MountNodeKind::CharacterDevice => FileType::CharDevice,
            MountNodeKind::BlockDevice => FileType::BlockDevice,
            MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
        };
        let fallback_mode = if self.writable { 0o755 } else { 0o555 };
        Ok(FileAttr {
            ino: inode,
            size: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            atime: metadata_time(lookup.metadata.accessed_ns),
            mtime: metadata_time(lookup.metadata.modified_ns),
            ctime: metadata_time(lookup.metadata.changed_ns),
            crtime: metadata_time(lookup.metadata.created_ns),
            kind,
            perm: u16::try_from(metadata_u32(lookup.metadata.posix_mode, fallback_mode) & 0o7777)
                .unwrap_or(0o7777),
            nlink: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_u32(lookup.metadata.posix_uid, self.fallback_uid),
            gid: metadata_u32(lookup.metadata.posix_gid, self.fallback_gid),
            rdev: node
                .device
                .map(|(major, minor)| native_device_number(major, minor))
                .transpose()?
                .unwrap_or(0),
            blksize: 4096,
            flags: u32::try_from(metadata_u64(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }

    fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    fn remove_path_cache(&mut self, path: &MountPath) {
        let affected = self
            .by_inode
            .iter()
            .filter_map(|(inode, entry)| entry.bindings.contains(path).then_some(*inode))
            .collect::<Vec<_>>();
        for inode in affected {
            self.remove_binding(inode, path);
        }
    }

    fn remove_binding(&mut self, inode: u64, path: &MountPath) {
        let remove_inode = if let Some(entry) = self.by_inode.get_mut(&inode) {
            entry.bindings.retain(|candidate| candidate != path);
            entry.bindings.is_empty() && entry.lookup_references == 0 && entry.open_handles == 0
        } else {
            false
        };
        if remove_inode && let Some(entry) = self.by_inode.remove(&inode) {
            self.inode_by_file.remove(&entry.lookup.node.file_id);
        }
    }

    fn invalidate_prefix(&mut self, prefix: &MountPath) {
        let affected = self
            .by_inode
            .iter()
            .flat_map(|(inode, entry)| {
                entry
                    .bindings
                    .iter()
                    .filter(|path| has_prefix(path, prefix))
                    .cloned()
                    .map(|path| (*inode, path))
            })
            .collect::<Vec<_>>();
        for (inode, path) in affected {
            self.remove_binding(inode, &path);
        }
    }

    fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath) {
        for entry in self.by_inode.values_mut() {
            for binding in &mut entry.bindings {
                if let Some(rebased) = replace_prefix(binding, source, destination) {
                    *binding = rebased;
                }
            }
        }
        for directory in self.directories.values_mut() {
            if let Some(rebased) = replace_prefix(&directory.path, source, destination) {
                directory.path = rebased;
            }
        }
    }
}

fn intern_projected(
    next_inode: &mut u64,
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    path: MountPath,
    lookup: &MountLookup,
    lookup_reference: bool,
) -> Result<u64, i32> {
    if let Some(inode) = inode_by_file.get(&lookup.node.file_id).copied() {
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if !entry.bindings.contains(&path) {
            if u64::try_from(entry.bindings.len()).unwrap_or(u64::MAX) >= lookup.node.link_count {
                return Err(libc::EIO);
            }
            entry.bindings.try_reserve(1).map_err(|_| libc::ENOMEM)?;
            entry.bindings.push(path);
        }
        entry.lookup = *lookup;
        if lookup_reference {
            entry.lookup_references = entry.lookup_references.saturating_add(1);
        }
        return Ok(inode);
    }
    let inode = *next_inode;
    if inode <= ROOT_INODE {
        return Err(libc::EOVERFLOW);
    }
    let following = inode.checked_add(1).ok_or(libc::EOVERFLOW)?;
    *next_inode = following;
    inode_by_file.insert(lookup.node.file_id, inode);
    by_inode.insert(
        inode,
        InodeEntry {
            bindings: vec![path],
            lookup: *lookup,
            lookup_references: u64::from(lookup_reference),
            open_handles: 0,
        },
    );
    Ok(inode)
}

impl Filesystem for FuseProjection {
    fn lookup(&mut self, _request: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        match self.source.lookup(&path) {
            Ok(Some(lookup)) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(error),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&TTL, &attr, 0),
                    Err(error) => reply.error(error),
                }
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn getattr(&mut self, _request: &Request<'_>, inode: u64, fh: Option<u64>, reply: ReplyAttr) {
        match self
            .refresh_handle(inode, fh)
            .and_then(|node| self.attr(inode, &node))
        {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(error) => reply.error(error),
        }
    }

    fn forget(&mut self, _request: &Request<'_>, inode: u64, nlookup: u64) {
        if inode == ROOT_INODE {
            return;
        }
        let remove = if let Some(entry) = self.by_inode.get_mut(&inode) {
            entry.lookup_references = entry.lookup_references.saturating_sub(nlookup);
            entry.lookup_references == 0 && entry.open_handles == 0
        } else {
            false
        };
        if remove && let Some(entry) = self.by_inode.remove(&inode) {
            self.inode_by_file.remove(&entry.lookup.node.file_id);
        }
    }

    fn readlink(&mut self, _request: &Request<'_>, inode: u64, reply: ReplyData) {
        let path = match self.path(inode) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        match self.source.read_link(path) {
            Ok(target) => reply.data(&target),
            Err(error) => reply.error(errno(error)),
        }
    }

    #[allow(clippy::similar_names, clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if self.admit_write().is_err() || bkuptime.is_some() {
            return reply.error(libc::EOPNOTSUPP);
        }
        let open_file = match fh {
            Some(handle) => match self.open_handle(inode, handle) {
                Ok(open_file) => Some(open_file),
                Err(error) => return reply.error(error),
            },
            None => None,
        };
        let current = match open_file.as_ref() {
            Some(open_file) => open_file.lookup().map_err(errno),
            None => self.refresh(inode),
        };
        let current = match current {
            Ok(current) => current,
            Err(error) => return reply.error(error),
        };
        let mut metadata = current.metadata;
        if let Some(mode) = mode {
            metadata.posix_mode = MetadataField::Value(mode);
        }
        if let Some(uid) = uid {
            metadata.posix_uid = MetadataField::Value(uid);
        }
        if let Some(gid) = gid {
            metadata.posix_gid = MetadataField::Value(gid);
        }
        if let Some(atime) = atime {
            metadata.accessed_ns = match time_or_now_ns(atime) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(error),
            };
        }
        if let Some(mtime) = mtime {
            metadata.modified_ns = match time_or_now_ns(mtime) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(error),
            };
        }
        let changed = chgtime.or(ctime);
        if let Some(changed) = changed {
            metadata.changed_ns = match system_time_ns(changed) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(error),
            };
        }
        if let Some(created) = crtime {
            metadata.created_ns = match system_time_ns(created) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(error),
            };
        }
        if let Some(flags) = flags {
            metadata.posix_flags = MetadataField::Value(u64::from(flags));
        }
        if mode.is_none()
            && uid.is_none()
            && gid.is_none()
            && size.is_none()
            && atime.is_none()
            && mtime.is_none()
            && ctime.is_none()
            && crtime.is_none()
            && chgtime.is_none()
            && flags.is_none()
        {
            return match self.attr(inode, &current) {
                Ok(attr) => reply.attr(&TTL, &attr),
                Err(error) => reply.error(error),
            };
        }
        let updated = if let Some(open_file) = open_file {
            open_file
                .set_attributes(metadata, size)
                .and_then(|()| open_file.lookup())
                .map_err(errno)
        } else {
            let path = match self.path(inode) {
                Ok(path) => path.to_owned(),
                Err(error) => return reply.error(error),
            };
            self.source
                .set_attributes(&path, metadata, size)
                .map_err(errno)
                .and_then(|()| self.refresh(inode))
        };
        match updated.and_then(|node| self.attr(inode, &node)) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(error) => reply.error(error),
        }
    }

    fn mkdir(
        &mut self,
        request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let metadata = create_metadata(request, mode & !umask, S_IFDIR);
        match self.source.create_directory(&path, metadata) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(error),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&TTL, &attr, 0),
                    Err(error) => reply.error(error),
                }
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn mknod(
        &mut self,
        request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let (kind, device) = match mode & S_IFMT {
            S_IFIFO => (MountNodeKind::Fifo, None),
            S_IFSOCK => (MountNodeKind::Socket, None),
            S_IFCHR => (
                MountNodeKind::CharacterDevice,
                Some(native_device_parts(rdev)),
            ),
            S_IFBLK => (MountNodeKind::BlockDevice, Some(native_device_parts(rdev))),
            _ => return reply.error(libc::EOPNOTSUPP),
        };
        let metadata = create_metadata(request, mode & !umask, mode & S_IFMT);
        match self.source.create_special(&path, kind, device, metadata) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(error),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&TTL, &attr, 0),
                    Err(error) => reply.error(error),
                }
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn symlink(
        &mut self,
        request: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let path = match self.child_path(parent, link_name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let metadata = create_metadata(request, 0o777, S_IFLNK);
        match self.source.create_symbolic_link(
            &path,
            Bytes::copy_from_slice(target.as_os_str().as_bytes()),
            metadata,
        ) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(error),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&TTL, &attr, 0),
                    Err(error) => reply.error(error),
                }
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn unlink(&mut self, request: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_callback(request, parent, name, reply);
    }

    fn rmdir(&mut self, request: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_callback(request, parent, name, reply);
    }

    fn rename(
        &mut self,
        _request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        if flags & !RENAME_NOREPLACE != 0 {
            return reply.error(libc::EOPNOTSUPP);
        }
        let source = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let destination = match self.child_path(new_parent, new_name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let replace = flags & RENAME_NOREPLACE == 0;
        let replaced = if replace {
            match self.source.lookup(&destination) {
                Ok(Some(lookup))
                    if lookup.node.kind == MountNodeKind::Regular
                        && lookup.node.link_count == 1 =>
                {
                    if let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied()
                        && self
                            .by_inode
                            .get(&inode)
                            .is_some_and(|entry| entry.open_handles != 0)
                    {
                        match self.source.detach_file(&destination) {
                            Ok(detached) => Some((inode, detached)),
                            Err(error) => return reply.error(errno(error)),
                        }
                    } else {
                        None
                    }
                }
                Ok(_) => None,
                Err(error) => return reply.error(errno(error)),
            }
        } else {
            None
        };
        match self.source.rename(&source, &destination, replace) {
            Ok(()) => {
                if let Some((inode, detached)) = replaced {
                    self.retain_detached_handles(inode, &detached);
                }
                if replace {
                    self.invalidate_prefix(&destination);
                }
                self.rename_prefix(&source, &destination);
                reply.ok();
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn link(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let source = match self.path(inode) {
            Ok(path) => path.to_owned(),
            Err(error) => return reply.error(error),
        };
        let destination = match self.child_path(new_parent, new_name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let mut projected = match self.by_inode.get(&inode) {
            Some(entry) => entry.lookup,
            None => return reply.error(libc::ESTALE),
        };
        projected.node.link_count = match projected.node.link_count.checked_add(1) {
            Some(count) => count,
            None => return reply.error(libc::EMLINK),
        };
        let attr = match self.attr(inode, &projected) {
            Ok(attr) => attr,
            Err(error) => return reply.error(error),
        };
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            return reply.error(libc::ESTALE);
        };
        if entry.bindings.contains(&destination) {
            return reply.error(libc::EEXIST);
        }
        if entry.bindings.try_reserve(1).is_err() {
            return reply.error(libc::ENOMEM);
        }
        match self.source.hard_link(&source, &destination) {
            Ok(()) => {
                entry.bindings.push(destination);
                entry.lookup = projected;
                entry.lookup_references = entry.lookup_references.saturating_add(1);
                reply.entry(&TTL, &attr, 0);
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn open(&mut self, _request: &Request<'_>, inode: u64, _flags: i32, reply: ReplyOpen) {
        match self.node(inode) {
            Ok(node) if node.kind == MountNodeKind::Regular => {
                match self.allocate_file_handle(inode) {
                    Ok(handle) => reply.opened(handle, 0),
                    Err(error) => reply.error(error),
                }
            }
            Ok(_) => reply.error(libc::EISDIR),
            Err(error) => reply.error(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn read(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Ok(offset) = u64::try_from(offset) else {
            return reply.error(libc::EINVAL);
        };
        let open_file = match self.open_handle(inode, fh) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(error),
        };
        let lookup = open_file.lookup().map_err(errno);
        let node = match lookup {
            Ok(lookup) => lookup.node,
            Err(error) => return reply.error(error),
        };
        let length = node
            .logical_bytes
            .saturating_sub(offset)
            .min(u64::from(size));
        let Ok(length) = u32::try_from(length) else {
            return reply.error(libc::EOVERFLOW);
        };
        if length == 0 {
            return reply.data(&[]);
        }
        let read = open_file.read_range(offset, length);
        match read {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(errno(error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let Ok(offset) = u64::try_from(offset) else {
            return reply.error(libc::EINVAL);
        };
        let open_file = match self.open_handle(inode, fh) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(error),
        };
        let bytes = Bytes::copy_from_slice(data);
        let write = open_file.write_range(offset, bytes);
        match write {
            Ok(()) => {
                let _ = self.refresh_handle(inode, Some(fh));
                reply.written(u32::try_from(data.len()).unwrap_or(u32::MAX));
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn fsync(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.source.flush() {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn flush(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.source.flush() {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn release(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.writable
            && let Err(error) = self.source.flush()
        {
            return reply.error(errno(error));
        }
        let Some(file) = self.files.remove(&handle) else {
            return reply.error(libc::ESTALE);
        };
        if file.inode != inode {
            self.files.insert(handle, file);
            return reply.error(libc::ESTALE);
        }
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            return reply.error(libc::ESTALE);
        };
        if entry.open_handles == 0 {
            return reply.error(libc::EIO);
        }
        entry.open_handles -= 1;
        if entry.bindings.is_empty()
            && entry.lookup_references == 0
            && entry.open_handles == 0
            && let Some(entry) = self.by_inode.remove(&inode)
        {
            self.inode_by_file.remove(&entry.lookup.node.file_id);
        }
        reply.ok();
    }

    fn opendir(&mut self, _request: &Request<'_>, inode: u64, _flags: i32, reply: ReplyOpen) {
        let path = match self.path(inode) {
            Ok(path) => path.to_owned(),
            Err(error) => return reply.error(error),
        };
        let handle = self.next_handle;
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        let parent_inode = parent_inode(self, inode);
        self.directories.insert(
            handle,
            DirectoryHandle {
                path,
                parent_inode,
                cursor: None,
                entries: VecDeque::new(),
                exhausted: false,
                emitted: 0,
            },
        );
        reply.opened(handle, 0);
    }

    fn readdir(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(directory) = self.directories.get_mut(&handle) else {
            return reply.error(libc::ESTALE);
        };
        if offset != directory.emitted {
            return reply.error(libc::EINVAL);
        }
        if directory.emitted == 0 {
            if reply.add(inode, 1, FileType::Directory, ".") {
                return reply.ok();
            }
            directory.emitted = 1;
            if reply.add(directory.parent_inode, 2, FileType::Directory, "..") {
                return reply.ok();
            }
            directory.emitted = 2;
        }
        loop {
            let next_entry = {
                let Some(directory) = self.directories.get_mut(&handle) else {
                    return reply.error(libc::ESTALE);
                };
                if directory.entries.is_empty() && !directory.exhausted {
                    match self.source.read_directory(
                        &directory.path,
                        directory.cursor.as_deref(),
                        DIRECTORY_PAGE_SIZE,
                    ) {
                        Ok(page) => {
                            directory.cursor = page.next_cursor;
                            directory.exhausted = directory.cursor.is_none();
                            directory.entries.extend(page.entries);
                        }
                        Err(error) => return reply.error(errno(error)),
                    }
                }
                directory
                    .entries
                    .front()
                    .cloned()
                    .map(|entry| (entry, directory.path.clone(), directory.emitted))
            };
            let Some((entry, directory_path, emitted)) = next_entry else {
                return reply.ok();
            };
            let child_path = directory_path.child(entry.name.clone());
            let child_inode = match self.intern_enumerated(
                child_path,
                &MountLookup {
                    node: entry.node,
                    metadata: entry.metadata,
                },
            ) {
                Ok(inode) => inode,
                Err(error) => return reply.error(error),
            };
            let kind = match entry.node.kind {
                MountNodeKind::Regular => FileType::RegularFile,
                MountNodeKind::Directory => FileType::Directory,
                MountNodeKind::SymbolicLink => FileType::Symlink,
                MountNodeKind::Fifo => FileType::NamedPipe,
                MountNodeKind::Socket => FileType::Socket,
                MountNodeKind::CharacterDevice => FileType::CharDevice,
                MountNodeKind::BlockDevice => FileType::BlockDevice,
                MountNodeKind::Unsupported => return reply.error(libc::EOPNOTSUPP),
            };
            let next = emitted.saturating_add(1);
            if reply.add(
                child_inode,
                next,
                kind,
                OsString::from_vec(entry.name.clone()),
            ) {
                return reply.ok();
            }
            let Some(directory) = self.directories.get_mut(&handle) else {
                return reply.error(libc::ESTALE);
            };
            directory.entries.pop_front();
            directory.emitted = next;
        }
    }

    fn releasedir(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        handle: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        self.directories.remove(&handle);
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.source.flush() {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn setxattr(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        if position != 0 || flags & !(libc::XATTR_CREATE | libc::XATTR_REPLACE) != 0 {
            return reply.error(libc::EOPNOTSUPP);
        }
        if flags & libc::XATTR_CREATE != 0 && flags & libc::XATTR_REPLACE != 0 {
            return reply.error(libc::EINVAL);
        }
        let mode = if flags & libc::XATTR_CREATE != 0 {
            super::MountAttributeWriteMode::Create
        } else if flags & libc::XATTR_REPLACE != 0 {
            super::MountAttributeWriteMode::Replace
        } else {
            super::MountAttributeWriteMode::Upsert
        };
        let written = match self.open_inode(inode) {
            Some(open_file) => {
                open_file.write_attribute(name.as_bytes(), Bytes::copy_from_slice(value), mode)
            }
            None => match self.path(inode) {
                Ok(path) => self.source.write_attribute(
                    path,
                    name.as_bytes(),
                    Bytes::copy_from_slice(value),
                    mode,
                ),
                Err(error) => return reply.error(error),
            },
        };
        match written {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn getxattr(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let value = match self.open_inode(inode) {
            Some(open_file) => open_file.read_attribute(name.as_bytes()),
            None => match self.path(inode) {
                Ok(path) => self.source.read_attribute(path, name.as_bytes()),
                Err(error) => return reply.error(error),
            },
        };
        match value {
            Ok(Some(value)) if size == 0 => {
                reply.size(u32::try_from(value.len()).unwrap_or(u32::MAX));
            }
            Ok(Some(value)) if value.len() <= size as usize => reply.data(&value),
            Ok(Some(_)) => reply.error(libc::ERANGE),
            Ok(None) | Err(MountSourceError::NotFound) => reply.error(libc::ENODATA),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn listxattr(&mut self, _request: &Request<'_>, inode: u64, size: u32, reply: ReplyXattr) {
        let open_file = self.open_inode(inode);
        let path = if open_file.is_none() {
            match self.path(inode) {
                Ok(path) => Some(path.to_owned()),
                Err(error) => return reply.error(error),
            }
        } else {
            None
        };
        let mut cursor: Option<Vec<u8>> = None;
        let mut encoded = Vec::new();
        loop {
            let page = if let Some(open_file) = open_file.as_ref() {
                open_file.list_attributes(cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
            } else {
                let Some(path) = path.as_ref() else {
                    return reply.error(libc::EIO);
                };
                self.source
                    .list_attributes(path, cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
            };
            let page = match page {
                Ok(page) => page,
                Err(error) => return reply.error(errno(error)),
            };
            for name in page.names {
                let Some(required) = name.len().checked_add(1) else {
                    return reply.error(libc::EOVERFLOW);
                };
                if encoded.len().saturating_add(required) > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return reply.error(libc::E2BIG);
                }
                if encoded.try_reserve(required).is_err() {
                    return reply.error(libc::ENOMEM);
                }
                encoded.extend_from_slice(&name);
                encoded.push(0);
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            if cursor.as_ref() == Some(&next) {
                return reply.error(libc::EIO);
            }
            cursor = Some(next);
        }
        if size == 0 {
            reply.size(u32::try_from(encoded.len()).unwrap_or(u32::MAX));
        } else if encoded.len() <= size as usize {
            reply.data(&encoded);
        } else {
            reply.error(libc::ERANGE);
        }
    }

    fn removexattr(&mut self, _request: &Request<'_>, inode: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let removed = match self.open_inode(inode) {
            Some(open_file) => open_file.remove_attribute(name.as_bytes()),
            None => match self.path(inode) {
                Ok(path) => self.source.remove_attribute(path, name.as_bytes()),
                Err(error) => return reply.error(error),
            },
        };
        match removed {
            Ok(()) => reply.ok(),
            Err(MountSourceError::NotFound) => reply.error(libc::ENODATA),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn fallocate(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        const KEEP_SIZE: i32 = 0x01;
        const PUNCH_HOLE: i32 = 0x02;
        const ZERO_RANGE: i32 = 0x10;
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let (Ok(offset), Ok(length)) = (u64::try_from(offset), u64::try_from(length)) else {
            return reply.error(libc::EINVAL);
        };
        if length == 0 {
            return reply.error(libc::EINVAL);
        }
        let operation = if mode == (PUNCH_HOLE | KEEP_SIZE) {
            crate::MountRangeAllocation::PunchHole
        } else if mode == ZERO_RANGE || mode == (ZERO_RANGE | KEEP_SIZE) {
            crate::MountRangeAllocation::ZeroRange {
                extend: mode == ZERO_RANGE,
            }
        } else if mode == 0 || mode == KEEP_SIZE {
            crate::MountRangeAllocation::Preallocate {
                keep_size: mode == KEEP_SIZE,
            }
        } else {
            return reply.error(libc::EOPNOTSUPP);
        };
        let open_file = match self.open_handle(inode, handle) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(error),
        };
        let allocated = open_file.allocate_range(offset, length, operation);
        match allocated {
            Ok(()) => {
                let _ = self.refresh_handle(inode, Some(handle));
                reply.ok();
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lseek(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        file_handle: u64,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let Ok(offset) = u64::try_from(offset) else {
            return reply.error(libc::EINVAL);
        };
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return reply.error(libc::EINVAL),
        };
        let open_file = match self.open_handle(inode, file_handle) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(error),
        };
        let found = open_file.seek(offset, target);
        match found {
            Ok(Some(found)) => match i64::try_from(found) {
                Ok(found) => reply.offset(found),
                Err(_) => reply.error(libc::EOVERFLOW),
            },
            Ok(None) => reply.error(libc::ENXIO),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn copy_file_range(
        &mut self,
        _request: &Request<'_>,
        source_inode: u64,
        source_handle: u64,
        source_offset: i64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: i64,
        length: u64,
        flags: u32,
        reply: ReplyWrite,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let (Ok(source_offset), Ok(destination_offset), Ok(_)) = (
            u64::try_from(source_offset),
            u64::try_from(destination_offset),
            u32::try_from(length),
        ) else {
            return reply.error(libc::EOVERFLOW);
        };
        if flags != 0 {
            return reply.error(libc::EOPNOTSUPP);
        }
        if length == 0 {
            return reply.written(0);
        }
        match self.copy_open_range(
            source_inode,
            source_handle,
            source_offset,
            destination_inode,
            destination_handle,
            destination_offset,
            length,
        ) {
            Ok(transferred) => reply.written(transferred),
            Err(error) => reply.error(error),
        }
    }

    fn create(
        &mut self,
        request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let metadata = create_metadata(request, mode & !umask, S_IFREG);
        match self.source.create_file(&path, metadata) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(error),
                };
                let handle = match self.allocate_file_handle(inode) {
                    Ok(handle) => handle,
                    Err(error) => return reply.error(error),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.created(&TTL, &attr, 0, handle, 0),
                    Err(error) => reply.error(error),
                }
            }
            Err(error) => reply.error(errno(error)),
        }
    }
}

impl FuseProjection {
    fn remove_callback(
        &mut self,
        _request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) {
        if let Err(error) = self.admit_write() {
            return reply.error(error);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(error),
        };
        let current = match self.source.lookup(&path) {
            Ok(current) => current,
            Err(error) => return reply.error(errno(error)),
        };
        let expected = current.map(|lookup| lookup.node.file_id);
        let detached = if let Some(lookup) = current
            && lookup.node.kind == MountNodeKind::Regular
            && lookup.node.link_count == 1
            && let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied()
            && self
                .by_inode
                .get(&inode)
                .is_some_and(|entry| entry.open_handles != 0)
        {
            match self.source.detach_file(&path) {
                Ok(detached) => Some((inode, detached)),
                Err(error) => return reply.error(errno(error)),
            }
        } else {
            None
        };
        match self.source.remove(&path, expected) {
            Ok(()) => {
                if let Some((inode, detached)) = detached {
                    self.retain_detached_handles(inode, &detached);
                }
                self.remove_path_cache(&path);
                reply.ok();
            }
            Err(error) => reply.error(errno(error)),
        }
    }
}

fn has_prefix(path: &MountPath, prefix: &MountPath) -> bool {
    path.components().starts_with(prefix.components())
}

fn replace_prefix(
    path: &MountPath,
    source: &MountPath,
    destination: &MountPath,
) -> Option<MountPath> {
    if !has_prefix(path, source) {
        return None;
    }
    let mut rebased = destination.clone();
    for component in &path.components()[source.components().len()..] {
        rebased = rebased.child(component.clone());
    }
    Some(rebased)
}

fn metadata_u32(field: MetadataField<u32>, unavailable: u32) -> u32 {
    match field {
        MetadataField::Unavailable => unavailable,
        MetadataField::Value(value) => value,
    }
}

fn metadata_u64(field: MetadataField<u64>, unavailable: u64) -> u64 {
    match field {
        MetadataField::Unavailable => unavailable,
        MetadataField::Value(value) => value,
    }
}

fn metadata_time(field: MetadataField<i64>) -> SystemTime {
    match field {
        MetadataField::Unavailable => SystemTime::UNIX_EPOCH,
        MetadataField::Value(value) if value >= 0 => {
            SystemTime::UNIX_EPOCH + Duration::from_nanos(value.unsigned_abs())
        }
        MetadataField::Value(value) => {
            SystemTime::UNIX_EPOCH - Duration::from_nanos(value.unsigned_abs())
        }
    }
}

fn system_time_ns(value: SystemTime) -> Result<i64, i32> {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_nanos()).map_err(|_| libc::EOVERFLOW),
        Err(error) => i64::try_from(error.duration().as_nanos())
            .map(|nanos| -nanos)
            .map_err(|_| libc::EOVERFLOW),
    }
}

fn time_or_now_ns(value: TimeOrNow) -> Result<i64, i32> {
    system_time_ns(match value {
        TimeOrNow::SpecificTime(value) => value,
        TimeOrNow::Now => SystemTime::now(),
    })
}

fn create_metadata(request: &Request<'_>, mode: u32, kind: u32) -> FileMetadata {
    let now = system_time_ns(SystemTime::now()).unwrap_or(i64::MAX);
    FileMetadata {
        posix_mode: MetadataField::Value((mode & 0o7777) | kind),
        posix_uid: MetadataField::Value(request.uid()),
        posix_gid: MetadataField::Value(request.gid()),
        posix_flags: MetadataField::Value(0),
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Value(now),
        modified_ns: MetadataField::Value(now),
        accessed_ns: MetadataField::Value(now),
        changed_ns: MetadataField::Value(now),
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn native_device_parts(device: u32) -> (u32, u32) {
    super::device::split_device(u64::from(device))
}

#[cfg(target_os = "linux")]
fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    super::device::join_device(major, minor)
        .and_then(|device| u32::try_from(device).map_err(|_| libc::EOVERFLOW))
}

#[cfg(target_os = "macos")]
fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    super::device::join_device(major, minor).map(|device| u32::from_ne_bytes(device.to_ne_bytes()))
}

fn parent_inode(filesystem: &FuseProjection, inode: u64) -> u64 {
    let Ok(path) = filesystem.path(inode) else {
        return ROOT_INODE;
    };
    let Some((_last, components)) = path.components().split_last() else {
        return ROOT_INODE;
    };
    let mut parent = MountPath::root();
    for component in components {
        parent = parent.child(component.clone());
    }
    filesystem
        .by_inode
        .iter()
        .find_map(|(candidate, entry)| entry.bindings.contains(&parent).then_some(*candidate))
        .unwrap_or(ROOT_INODE)
}

#[allow(clippy::needless_pass_by_value)]
fn source_error(error: MountSourceError) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn errno(error: MountSourceError) -> i32 {
    match error {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) => libc::EOPNOTSUPP,
        MountSourceError::Engine(_) => libc::EIO,
        MountSourceError::Stale => libc::ESTALE,
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        InodeEntry, MountLookup, MountNode, MountNodeKind, MountPath, ROOT_INODE, intern_projected,
    };
    use crate::kernel::FileMetadata;
    use std::collections::HashMap;

    #[test]
    fn rename_noreplace_matches_linux_libc() {
        assert_eq!(super::RENAME_NOREPLACE, libc::RENAME_NOREPLACE);
    }

    #[test]
    fn enumerated_entries_receive_nonzero_inodes_without_fake_lookup_references() -> Result<(), i32>
    {
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::<u64, InodeEntry>::new();
        let mut inode_by_file = HashMap::new();
        let lookup = MountLookup {
            node: MountNode {
                file_id: crate::FileId::new(),
                kind: MountNodeKind::Regular,
                logical_bytes: 4,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        };
        let path = MountPath::root().child(b"preexisting.bin".to_vec());

        let enumerated = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut inode_by_file,
            path.clone(),
            &lookup,
            false,
        )?;
        assert_ne!(enumerated, 0);
        assert_eq!(by_inode[&enumerated].lookup_references, 0);

        let looked_up = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut inode_by_file,
            path,
            &lookup,
            true,
        )?;
        assert_eq!(looked_up, enumerated);
        assert_eq!(by_inode[&enumerated].lookup_references, 1);

        let retained_by_inode_count = by_inode.len();
        let retained_inode_by_file_count = inode_by_file.len();
        next_inode = u64::MAX;
        let overflow = MountLookup {
            node: MountNode {
                file_id: crate::FileId::new(),
                ..lookup.node
            },
            ..lookup
        };
        assert_eq!(
            intern_projected(
                &mut next_inode,
                &mut by_inode,
                &mut inode_by_file,
                MountPath::root().child(b"overflow.bin".to_vec()),
                &overflow,
                false,
            ),
            Err(libc::EOVERFLOW)
        );
        assert_eq!(next_inode, u64::MAX);
        assert_eq!(by_inode.len(), retained_by_inode_count);
        assert_eq!(inode_by_file.len(), retained_inode_by_file_count);
        assert_eq!(by_inode[&enumerated].lookup_references, 1);
        assert_eq!(inode_by_file[&lookup.node.file_id], enumerated);
        Ok(())
    }
}
