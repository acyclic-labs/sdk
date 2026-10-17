use super::*;

pub(super) struct Resolved {
    pub(super) lookup: MountLookup,
    pub(super) stamp: Option<ViewStamp>,
    pub(super) through: Option<FuseOwned<MountPath>>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "Resolved facts stay inline; boxing would add an allocation before callback admission."
)]
pub(super) enum NodeFacts {
    Current(Resolved),
    Handle(FuseOpenFile),
    Names {
        file_id: Option<crate::FileId>,
        paths: FuseOwned<Vec<FuseOwned<MountPath>>>,
    },
}

pub(super) enum AttributeTarget {
    Handle(FuseOpenFile),
    Path(FuseOwned<MountPath>),
}

pub(super) struct ChildName {
    pub(super) spelled: FuseOwned<MountPath>,
    pub(super) folded: Option<FuseOwned<MountPath>>,
}

impl ChildName {
    pub(super) fn new(
        source: &dyn MountFilesystem,
        spelled: FuseOwned<MountPath>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let folded = source
            .folded_path_with_meter(&spelled, work)?
            .map(|(path, bytes)| FuseOwned::adopt(path, bytes, work))
            .transpose()?;
        Ok(Self { spelled, folded })
    }
    pub(super) fn key(&self) -> &MountPath {
        self.folded.as_deref().unwrap_or(&self.spelled)
    }
}

pub(super) struct Entry {
    pub(super) attr: FileAttr,
    pub(super) ttl: Duration,
}

pub(super) struct ProjectionState {
    pub(super) writable: bool,
    pub(super) mount_uid: u32,
    pub(super) mount_gid: u32,
    pub(super) next_inode: u64,
    pub(super) next_handle: u64,
    pub(super) by_inode: HashMap<u64, InodeEntry>,
    pub(super) inode_by_path: HashMap<FuseOwned<MountPath>, u64>,
    pub(super) inode_by_file: HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    pub(super) files: HashMap<u64, FileHandle>,
    pub(super) streams: DirectoryStreams,
    pub(super) invalidation: InvalidationQueue,
    pub(super) superseded_attributes:
        FuseOwned<Vec<(u64, crate::materializer::NativeWorkReservation)>>,
    /// Numbers listing replies, so a reply can tell an inode it already
    /// carries (see [`InodeEntry::listed_in_reply`]).
    pub(super) listing_replies: u64,
}

impl ProjectionState {
    pub(super) fn invalidate_attributes(
        &mut self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        fuse_reserve(&mut self.superseded_attributes, 1, work)?;
        let credit = work.reserve_work(crate::WorkCounters {
            backend_write_operations: 1,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        self.superseded_attributes.push((inode, credit));
        Ok(())
    }

    pub(super) fn new(writable: bool, mount_uid: u32, mount_gid: u32) -> Self {
        Self {
            writable,
            mount_uid,
            mount_gid,
            next_inode: ROOT_INODE + 1,
            next_handle: 1,
            by_inode: HashMap::new(),
            inode_by_path: HashMap::new(),
            inode_by_file: HashMap::new(),
            files: HashMap::new(),
            streams: DirectoryStreams::default(),
            invalidation: InvalidationQueue::default(),
            superseded_attributes: FuseOwned::default(),
            listing_replies: 0,
        }
    }

    pub(super) fn path(&self, inode: u64) -> Result<&MountPath, i32> {
        self.by_inode
            .get(&inode)
            .and_then(|entry| entry.bindings.first())
            .map(Binding::native_path)
            .ok_or(libc::ESTALE)
    }

    pub(super) fn retain_spelling(
        &mut self,
        inode: u64,
        name: ChildName,
    ) -> Result<FuseOwned<MountPath>, NativeMountError> {
        if let Some(key) = name.folded {
            self.by_inode
                .get_mut(&inode)
                .ok_or(libc::ESTALE)?
                .binding_mut(&key)
                .ok_or(libc::ESTALE)?
                .spelled = (*key != *name.spelled).then_some(name.spelled);
            Ok(key)
        } else {
            Ok(name.spelled)
        }
    }

    pub(super) fn child_path(
        &self,
        parent: u64,
        name: &OsStr,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<MountPath>, NativeMountError> {
        work.charge(Items.delta(1))?;
        let parent = self.path(parent)?;
        let name = name.as_bytes();
        let mut contains_separator = false;
        for byte in name {
            work.charge(Items.delta(1))?;
            contains_separator |= *byte == b'/';
        }
        if name.is_empty() || name == b"." || name == b".." || contains_separator {
            return Err(libc::EINVAL.into());
        }
        fuse_child_path(parent, name, work)
    }

    pub(super) fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    #[allow(
        clippy::mutable_key_type,
        reason = "FuseOwned hashes and compares only its immutable path, never its accounting owner."
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "The admitted lookup facts and their existing work owner are passed directly."
    )]
    pub(super) fn intern(
        &mut self,
        source: &dyn MountFilesystem,
        path: FuseOwned<MountPath>,
        lookup: &MountLookup,
        stamp: Option<ViewStamp>,
        lookup_reference: bool,
        confirmed: &HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        work.charge(Items.delta(1))?;
        if let Some(previous) = self.inode_by_path.get(&*path).copied()
            && self
                .by_inode
                .get(&previous)
                .is_some_and(|entry| entry.lookup.node.file_id != lookup.node.file_id)
        {
            let held = match self.by_inode.get(&previous) {
                Some(entry) => entry
                    .binding(&path)
                    .is_some_and(|binding| binding.kernel.is_some()),
                None => false,
            };
            if held
                && let Some(parent) = fuse_parent(&path, work)?
                && let Some(parent) = self.inode_by_path.get(&*parent).copied()
            {
                let name = fuse_copy_bytes(path.components().last().ok_or(libc::ESTALE)?, work)?;
                fuse_reserve(&mut self.invalidation.deferred, 1, work)?;
                self.invalidation
                    .deferred
                    .push(KernelCacheItem::Entry { parent, name });
            }
            self.remove_binding(previous, &path, work)?;
        }
        if let Some(inode) = self
            .inode_by_file
            .get(&lookup.node.file_id)
            .map(|(inode, _)| *inode)
        {
            self.forget_unbound_names(source, confirmed, inode, &path, work)?;
            if inode != ROOT_INODE
                && self
                    .by_inode
                    .get(&inode)
                    .is_some_and(|entry| entry.bindings.is_empty())
            {
                self.forget_file(lookup.node.file_id, inode);
            }
        }
        self.intern_projected(path, lookup, stamp, lookup_reference, work)
    }

    pub(super) fn unvouched_names(
        &self,
        source: &dyn MountFilesystem,
        file_id: crate::FileId,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<Vec<FuseOwned<MountPath>>>, NativeMountError> {
        let mut names = fuse_vec(0, work)?;
        let Some(entry) = self
            .inode_by_file
            .get(&file_id)
            .and_then(|(inode, _)| self.by_inode.get(inode))
        else {
            return Ok(names);
        };
        for binding in entry.bindings.iter() {
            work.charge(Items.delta(1))?;
            if binding.native_path() == path {
                continue;
            }
            if let Some(stamp) = binding.verified
                && source.unchanged_since_with_meter(binding.native_path(), None, stamp, work)?
            {
                continue;
            }
            let name = fuse_clone_path(binding.native_path(), work)?;
            fuse_reserve(&mut names, 1, work)?;
            names.push(name);
        }
        Ok(names)
    }

    pub(super) fn release_lookup_reference(&mut self, inode: u64, references: u64) {
        if inode == ROOT_INODE {
            return;
        }
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            entry.lookup_references = entry.lookup_references.saturating_sub(references);
        }
        if self
            .by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references == 0 && entry.open_handles == 0)
            && let Some(entry) = self.by_inode.remove(&inode)
        {
            for binding in entry.bindings.iter() {
                if self.inode_by_path.get(&binding.path) == Some(&inode) {
                    self.inode_by_path.remove(&binding.path);
                }
            }
            self.forget_file(entry.lookup.node.file_id, inode);
        }
    }

    pub(super) fn admissible(
        &self,
        source: &dyn MountFilesystem,
        anchor: &MountPath,
        file_id: Option<crate::FileId>,
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let Some(stamp) = stamp else {
            return Ok(false);
        };
        // While the kernel may hold entries the invalidator could not drop,
        // nothing new is cached.
        if self.invalidation.denied {
            return Ok(false);
        }
        if file_id.is_some_and(|file_id| !source.reports_changes_to(file_id)) {
            return Ok(false);
        }
        if self
            .invalidation
            .scanned
            .is_none_or(|scanned| scanned <= stamp)
        {
            return Ok(true);
        }
        Ok(source.unchanged_since_with_meter(anchor, file_id, stamp, work)?)
    }

    pub(super) fn admit_attributes(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Duration, NativeMountError> {
        work.charge(Items.delta(1))?;
        let Some(entry) = self.by_inode.get(&inode) else {
            return Ok(Duration::ZERO);
        };
        let admitted = self.admissible(
            source,
            entry.anchor(),
            Some(entry.lookup.node.file_id),
            stamp,
            work,
        )?;
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        }
        if let Some(entry) = self.by_inode.get(&inode)
            && entry.lookup.node.kind == MountNodeKind::Regular
        {
            let handed = ContentVersion::of(&entry.lookup);
            if entry.kernel_content.is_some_and(|held| held != handed) {
                self.invalidate_attributes(inode, work)?;
            }
            if let Some(entry) = self.by_inode.get_mut(&inode) {
                entry.kernel_content = Some(handed);
            }
        }
        Ok(if admitted { CACHE_TTL } else { Duration::ZERO })
    }

    pub(super) fn admit_entry(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        name: ChildName,
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Duration, NativeMountError> {
        let folded = name.folded.is_some();
        let key = self.retain_spelling(inode, name)?;
        let attributes = self.admit_attributes(source, inode, stamp, work)?;
        if folded {
            return Ok(Duration::ZERO);
        }
        let Some(entry) = self.by_inode.get(&inode) else {
            return Ok(Duration::ZERO);
        };
        let admitted =
            self.admissible(source, &key, Some(entry.lookup.node.file_id), stamp, work)?;
        if let Some(entry) = self.by_inode.get_mut(&inode)
            && let Some(binding) = entry.binding_mut(&key)
        {
            hold(&mut binding.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        }
        Ok(if admitted { attributes } else { Duration::ZERO })
    }

    pub(super) fn negative_entry(
        &mut self,
        source: &dyn MountFilesystem,
        parent: u64,
        name: &ChildName,
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Entry, NativeMountError> {
        let stamp = stamp.ok_or(libc::ENOENT)?;
        if name.folded.is_some() || !self.admissible(source, name.key(), None, Some(stamp), work)? {
            return Err(libc::ENOENT.into());
        }
        let component = name.key().components().last().ok_or(libc::ENOENT)?;
        let entry = self.by_inode.get_mut(&parent).ok_or(libc::ENOENT)?;
        if !entry.remember_negative_child(component, stamp, work)? {
            return Err(libc::ENOENT.into());
        }
        let lookup = self.by_inode.get(&ROOT_INODE).ok_or(libc::ESTALE)?.lookup;
        let mut attr = self.attr(ROOT_INODE, &lookup).map_err(|_| libc::ESTALE)?;
        attr.ino = INodeNo(0);
        Ok(Entry {
            attr,
            ttl: CACHE_TTL,
        })
    }

    pub(super) fn absent_since(&self, parent: u64, name: &ChildName) -> Option<ViewStamp> {
        if name.folded.is_some() {
            return None;
        }
        let component = name.key().components().last()?;
        self.by_inode
            .get(&parent)?
            .negative_children
            .get(component.as_slice())
            .copied()
    }

    pub(super) fn admit_listing(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let Some(entry) = self.by_inode.get(&inode) else {
            return Ok(());
        };
        let admitted = self.admissible(
            source,
            entry.anchor(),
            Some(entry.lookup.node.file_id),
            stamp,
            work,
        )?;
        if !admitted {
            fuse_reserve(&mut self.invalidation.deferred, 1, work)?;
        }
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        }
        if !admitted {
            self.invalidation
                .deferred
                .push(KernelCacheItem::Inode(inode));
        }
        Ok(())
    }

    pub(super) fn apply(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        resolved: &Resolved,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(FileAttr, Duration), NativeMountError> {
        let lookup = resolved.lookup;
        if inode == ROOT_INODE {
            work.charge(Items.delta(1))?;
            if lookup.node.kind != MountNodeKind::Directory {
                return Err(libc::EIO.into());
            }
            let previous = self
                .by_inode
                .get(&ROOT_INODE)
                .ok_or(libc::ESTALE)?
                .lookup
                .node
                .file_id;
            if previous != lookup.node.file_id {
                if self
                    .inode_by_file
                    .get(&lookup.node.file_id)
                    .is_some_and(|(inode, _)| *inode != ROOT_INODE)
                {
                    return Err(libc::EIO.into());
                }
                let storage = work.claim(
                    std::mem::size_of::<(crate::FileId, (u64, crate::NativeAllocation))>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )?;
                self.inode_by_file
                    .try_reserve(1)
                    .map_err(NativeMountError::Allocation)?;
                self.forget_file(previous, ROOT_INODE);
                self.inode_by_file
                    .insert(lookup.node.file_id, (ROOT_INODE, storage));
            }
        }
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if inode != ROOT_INODE && entry.lookup.node.file_id != lookup.node.file_id {
            return Err(libc::ESTALE.into());
        }
        entry.lookup = lookup;
        entry.facts = resolved.stamp;
        if let Some(path) = resolved.through.as_ref()
            && let Some(binding) = entry.binding_mut(path)
        {
            binding.verified = resolved.stamp;
        }
        let ttl = self.admit_attributes(source, inode, resolved.stamp, work)?;
        Ok((self.attr(inode, &lookup)?, ttl))
    }

    pub(super) fn node_facts(
        &self,
        source: &dyn MountFilesystem,
        inode: u64,
        handle: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<NodeFacts, NativeMountError> {
        work.charge(Items.delta(1))?;
        let entry = self.by_inode.get(&inode).ok_or(libc::ESTALE)?;
        if let Some(handle) = handle {
            let open_file = self.file_handle(inode, handle)?.open_file.clone();
            return Ok(match entry.current_facts(source, work)? {
                Some(stamp) => NodeFacts::Current(Resolved {
                    lookup: entry.lookup,
                    stamp: Some(stamp),
                    through: None,
                }),
                None => NodeFacts::Handle(open_file),
            });
        }
        if let Some(stamp) = entry.current_facts(source, work)? {
            let through = entry
                .bindings
                .first()
                .map(|binding| fuse_clone_path(binding.native_path(), work))
                .transpose()?;
            return Ok(NodeFacts::Current(Resolved {
                lookup: entry.lookup,
                stamp: Some(stamp),
                through,
            }));
        }
        if entry.bindings.is_empty() {
            return self
                .open_inode(inode)
                .map(NodeFacts::Handle)
                .ok_or(libc::ESTALE.into());
        }
        let mut paths = fuse_vec(entry.bindings.len(), work)?;
        for binding in entry.bindings.iter() {
            work.charge(Items.delta(1))?;
            paths.push(fuse_clone_path(binding.native_path(), work)?);
        }
        Ok(NodeFacts::Names {
            file_id: (inode != ROOT_INODE).then_some(entry.lookup.node.file_id),
            paths,
        })
    }

    pub(super) fn attr(&self, inode: u64, lookup: &MountLookup) -> Result<FileAttr, i32> {
        let node = lookup.node;
        let kind = file_type(node.kind)?;
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        Ok(FileAttr {
            ino: INodeNo(inode),
            size: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            atime: metadata_time(lookup.metadata.accessed_ns),
            mtime: metadata_time(lookup.metadata.modified_ns),
            ctime: metadata_time(lookup.metadata.changed_ns),
            crtime: metadata_time(lookup.metadata.created_ns),
            kind,
            perm: u16::try_from(metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777)
                .unwrap_or(0o7777),
            nlink: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, self.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, self.mount_gid),
            rdev: node
                .device
                .map(|(major, minor)| {
                    crate::native_mount::device::join_device(major, minor)
                        .and_then(|device| u32::try_from(device).map_err(|_| libc::EOVERFLOW))
                })
                .transpose()?
                .unwrap_or(0),
            blksize: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }
}
