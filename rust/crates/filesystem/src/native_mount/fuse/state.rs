use super::*;

#[derive(Clone, Copy)]
pub(super) struct AttributeDefaults {
    pub(super) writable: bool,
    pub(super) mount_uid: u32,
    pub(super) mount_gid: u32,
}

pub(super) struct Resolved {
    pub(super) lookup: MountLookup,
    pub(super) stamp: Option<ViewStamp>,
    pub(super) through: Option<FuseOwned<MountPath>>,
}

pub(super) enum NodeFacts {
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
    pub(super) fn new(source: &dyn MountFilesystem, spelled: FuseOwned<MountPath>,
        work: &crate::NativeMaterializationWork) -> Result<Self, NativeMountError>
    {
        let folded = source.folded_path_with_meter(&spelled, work)?
            .map(|(path, bytes)| FuseOwned::adopt(path, bytes, work)).transpose()?;
        Ok(Self { spelled, folded })
    }

    pub(super) fn key(&self) -> &MountPath {
        self.folded.as_deref().unwrap_or(&self.spelled)
    }

    pub(super) fn is_exact(&self) -> bool {
        self.folded.is_none()
    }
}


pub(super) struct Entry {
    pub(super) attr: FileAttr,
    pub(super) ttl: Duration,
}

pub(super) struct ProjectionState {
    pub(super) defaults: AttributeDefaults,
    pub(super) next_inode: u64,
    pub(super) next_handle: u64,
    pub(super) by_inode: HashMap<u64, InodeEntry>,
    pub(super) inode_by_path: HashMap<FuseOwned<MountPath>, u64>,
    pub(super) inode_by_file: HashMap<crate::FileId, u64>,
    pub(super) files: HashMap<u64, FileHandle>,
    pub(super) implicit: HashMap<u64, (u64, crate::NativeAllocation)>,
    pub(super) streams: DirectoryStreams,
    pub(super) invalidation: InvalidationQueue,
    pub(super) page_stores: PageStores,
}

impl ProjectionState {
    pub(super) fn new(defaults: AttributeDefaults) -> Self {
        Self {
            defaults, next_inode: ROOT_INODE + 1, next_handle: 1,
            by_inode: HashMap::new(), inode_by_path: HashMap::new(), inode_by_file: HashMap::new(),
            files: HashMap::new(), implicit: HashMap::new(),
            streams: DirectoryStreams::default(), invalidation: InvalidationQueue::default(),
            page_stores: PageStores::default(),
        }
    }

    pub(super) fn path(&self, inode: u64) -> Result<&MountPath, i32> {
        self.by_inode
            .get(&inode)
            .and_then(|entry| entry.bindings.first())
            .map(|binding| &*binding.path)
            .ok_or(libc::ESTALE)
    }

    pub(super) fn node(&self, inode: u64) -> Result<MountNode, i32> {
        self.by_inode
            .get(&inode)
            .map(|entry| entry.lookup.node)
            .ok_or(libc::ESTALE)
    }

    pub(super) fn child_path(&self, parent: u64, name: &OsStr, work: &crate::NativeMaterializationWork)
        -> Result<FuseOwned<MountPath>, NativeMountError>
    {
        fuse_item(work)?;
        let parent = self.path(parent)?;
        let name = name.as_bytes();
        let mut contains_separator = false;
        for byte in name {
            fuse_item(work)?;
            contains_separator |= *byte == b'/';
        }
        if name.is_empty() || name == b"." || name == b".." || contains_separator {
            return Err(libc::EINVAL.into());
        }
        fuse_child_path(parent, name, work)
    }

    pub(super) fn admit_write(&self) -> Result<(), i32> {
        self.defaults.writable.then_some(()).ok_or(libc::EROFS)
    }

    pub(super) fn intern(
        &mut self, source: &dyn MountFilesystem, path: FuseOwned<MountPath>, lookup: &MountLookup,
        stamp: Option<ViewStamp>, lookup_reference: bool, confirmed: &HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        fuse_item(work)?;
        if let Some(previous) = self.inode_by_path.get(&*path).copied()
            && self.by_inode.get(&previous)
                .is_some_and(|entry| entry.lookup.node.file_id != lookup.node.file_id)
        {
            let held = match self.by_inode.get(&previous) {
                Some(entry) => entry.binding(&path, work)?.is_some_and(|binding| binding.kernel.is_some()),
                None => false,
            };
            if held {
                if let Some(parent) = fuse_parent(&path, work)? {
                    if let Some(parent) = self.inode_by_path.get(&*parent).copied() {
                        let name = fuse_copy_bytes(path.components().last().ok_or(libc::ESTALE)?, work)?;
                        fuse_reserve(&mut self.invalidation.deferred, 1, work)?;
                        self.invalidation.deferred.push(KernelCacheItem::Entry { parent, name });
                    }
                }
            }
            self.remove_binding(previous, &path, work)?;
        }
        if let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied() {
            forget_unbound_names(
                &mut self.by_inode, &mut self.inode_by_path, inode, &path,
                |name, verified| {
                    if name_is_bound(confirmed, source, lookup.node.file_id, name, work)? { return Ok(true); }
                    match verified {
                        Some(stamp) => Ok(source.unchanged_since_with_meter(name, None, stamp, work)?),
                        None => Ok(false),
                    }
                },
                &mut self.invalidation.deferred, work,
            )?;
            retire_reused_identity(&mut self.by_inode, &mut self.inode_by_file, inode);
        }
        intern_projected(&mut self.next_inode, &mut self.by_inode,
            &mut self.inode_by_path, &mut self.inode_by_file, path, lookup, stamp, lookup_reference, work)
    }

    pub(super) fn unvouched_names(&self, source: &dyn MountFilesystem, file_id: crate::FileId,
        path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<FuseOwned<Vec<FuseOwned<MountPath>>>, NativeMountError>
    {
        let mut names = fuse_vec(0, work)?;
        let Some(entry) = self.inode_by_file.get(&file_id).and_then(|inode| self.by_inode.get(inode))
            else { return Ok(names); };
        for binding in entry.bindings.iter() {
            fuse_item(work)?;
            if *binding.path == *path { continue; }
            if let Some(stamp) = binding.verified {
                if source.unchanged_since_with_meter(&binding.path, None, stamp, work)? { continue; }
            }
            let name = fuse_clone_path(&binding.path, work)?;
            fuse_reserve(&mut names, 1, work)?;
            names.push(name);
        }
        Ok(names)
    }

    pub(super) fn release_lookup_reference(&mut self, inode: u64, references: u64,
        work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError>
    {
        fuse_item(work)?;
        if let Some(entry) = self.by_inode.get(&inode) {
            let remaining = entry.lookup_references.saturating_sub(references);
            let implicit = u64::from(remaining == 0 && self.implicit.contains_key(&inode));
            if inode != ROOT_INODE && remaining == 0 && entry.open_handles <= implicit {
                for _binding in entry.bindings.iter() { fuse_item(work)?; }
            }
        }
        self.release_lookup_reference_administrative(inode, references);
        Ok(())
    }

    pub(super) fn release_lookup_reference_administrative(&mut self, inode: u64, references: u64) {
        if inode == ROOT_INODE {
            return;
        }
        let forgotten = self.by_inode.get_mut(&inode).is_some_and(|entry| {
            entry.lookup_references = entry.lookup_references.saturating_sub(references);
            entry.lookup_references == 0
        });
        if forgotten && let Some(implicit) = self.implicit.remove(&inode) {
            let _ = self.discard_file_handle(inode, implicit.0);
        }
        let remove = self
            .by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references == 0 && entry.open_handles == 0);
        if remove && let Some(mut entry) = self.by_inode.remove(&inode) {
            for binding in entry.bindings.iter() {
                if self.inode_by_path.get(&binding.path) == Some(&inode) {
                    self.inode_by_path.remove(&binding.path);
                }
            }
            forget_file(&mut self.inode_by_file, entry.lookup.node.file_id, inode, &mut entry.identity_storage);
        }
    }

    pub(super) fn admissible(&self, source: &dyn MountFilesystem, anchor: &MountPath,
        file_id: Option<crate::FileId>, stamp: Option<ViewStamp>, work: &crate::NativeMaterializationWork)
        -> Result<bool, NativeMountError>
    {
        let Some(stamp) = stamp else { return Ok(false); };
        if file_id.is_some_and(|file_id| !source.reports_changes_to(file_id)) { return Ok(false); }
        if self.invalidation.scanned.is_none_or(|scanned| scanned <= stamp) { return Ok(true); }
        Ok(source.unchanged_since_with_meter(anchor, file_id, stamp, work)?)
    }

    pub(super) fn admit_attributes(&mut self, source: &dyn MountFilesystem, inode: u64,
        stamp: Option<ViewStamp>, work: &crate::NativeMaterializationWork) -> Result<Duration, NativeMountError>
    {
        fuse_item(work)?;
        let Some(entry) = self.by_inode.get(&inode) else { return Ok(Duration::ZERO); };
        let admitted = self.admissible(source, entry.anchor(), Some(entry.lookup.node.file_id), stamp, work)?;
        if let Some(entry) = self.by_inode.get_mut(&inode) { hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN)); }
        Ok(if admitted { CACHE_TTL } else { Duration::ZERO })
    }

    pub(super) fn admit_entry(&mut self, source: &dyn MountFilesystem, inode: u64, name: &ChildName,
        stamp: Option<ViewStamp>, work: &crate::NativeMaterializationWork) -> Result<Duration, NativeMountError>
    {
        let attributes = self.admit_attributes(source, inode, stamp, work)?;
        if !name.is_exact() { return Ok(Duration::ZERO); }
        let Some(entry) = self.by_inode.get(&inode) else { return Ok(Duration::ZERO); };
        let admitted = self.admissible(source, name.key(), Some(entry.lookup.node.file_id), stamp, work)?;
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            if let Some(binding) = entry.binding_mut(name.key(), work)? {
                hold(&mut binding.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
            }
        }
        Ok(if admitted { attributes } else { Duration::ZERO })
    }

    pub(super) fn empty_since(&self, inode: u64) -> Option<ViewStamp> {
        self.by_inode.get(&inode)?.empty_since
    }

    pub(super) fn negative_entry(
        &mut self,
        source: &dyn MountFilesystem,
        parent: u64,
        name: &ChildName,
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Entry, NativeMountError> {
        let ttl = self
            .admit_negative(source, parent, name, stamp, work)?
            .ok_or(libc::ENOENT)?;
        let mut attr = self.node_attr(ROOT_INODE).map_err(|_| libc::ESTALE)?;
        attr.ino = INodeNo(0);
        Ok(Entry { attr, ttl })
    }

    pub(super) fn absent_since(&self, parent: u64, name: &ChildName) -> Option<ViewStamp> {
        let component = name.key().components().last()?;
        name.is_exact()
            .then(|| {
                self.by_inode
                    .get(&parent)?
                    .negative_children
                    .get(component.as_slice())
                    .copied()
            })
            .flatten()
    }

    pub(super) fn admit_negative(&mut self, source: &dyn MountFilesystem, parent: u64, name: &ChildName,
        stamp: Option<ViewStamp>, work: &crate::NativeMaterializationWork) -> Result<Option<Duration>, NativeMountError>
    {
        let Some(stamp) = stamp else { return Ok(None); };
        if !name.is_exact() || !self.admissible(source, name.key(), None, Some(stamp), work)? { return Ok(None); }
        let Some(component) = name.key().components().last() else { return Ok(None); };
        let Some(entry) = self.by_inode.get_mut(&parent) else { return Ok(None); };
        Ok(entry.remember_negative_child(component, stamp, work)?.then_some(CACHE_TTL))
    }

    pub(super) fn admit_listing(&mut self, source: &dyn MountFilesystem, inode: u64,
        stamp: Option<ViewStamp>, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError>
    {
        let Some(entry) = self.by_inode.get(&inode) else { return Ok(()); };
        let admitted = self.admissible(source, entry.anchor(), Some(entry.lookup.node.file_id), stamp, work)?;
        if !admitted { fuse_reserve(&mut self.invalidation.deferred, 1, work)?; }
        if let Some(entry) = self.by_inode.get_mut(&inode) { hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN)); }
        if !admitted { self.invalidation.deferred.push(KernelCacheItem::Inode(inode)); }
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
            replace_root_lookup(&mut self.by_inode, &mut self.inode_by_file, lookup, work)?;
        }
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != lookup.node.file_id {
            return Err(libc::ESTALE.into());
        }
        entry.lookup = lookup;
        entry.facts = resolved.stamp;
        if let Some(path) = resolved.through.as_ref() {
            if let Some(binding) = entry.binding_mut(path, work)? {
                binding.verified = resolved.stamp;
            }
        }
        let ttl = self.admit_attributes(source, inode, resolved.stamp, work)?;
        Ok((self.attr(inode, &lookup)?, ttl))
    }

    pub(super) fn node_facts(&self, source: &dyn MountFilesystem, inode: u64, handle: Option<u64>,
        work: &crate::NativeMaterializationWork) -> Result<Result<Resolved, NodeFacts>, NativeMountError>
    {
        fuse_item(work)?;
        let entry = self.by_inode.get(&inode).ok_or(libc::ESTALE)?;
        if let Some(handle) = handle {
            let open_file = self.open_handle(inode, handle, work)?;
            return Ok(match entry.current_facts(source, work)? {
                Some(stamp) => Ok(Resolved { lookup: entry.lookup, stamp: Some(stamp), through: None }),
                None => Err(NodeFacts::Handle(open_file)),
            });
        }
        if let Some(stamp) = entry.current_facts(source, work)? {
            let through = entry.bindings.first().map(|binding| fuse_clone_path(&binding.path, work)).transpose()?;
            return Ok(Ok(Resolved { lookup: entry.lookup, stamp: Some(stamp), through }));
        }
        if entry.bindings.is_empty() {
            return self.open_inode(inode, work)?.map(|file| Err(NodeFacts::Handle(file))).ok_or(libc::ESTALE.into());
        }
        let mut paths = fuse_vec(entry.bindings.len(), work)?;
        for binding in entry.bindings.iter() { fuse_item(work)?; paths.push(fuse_clone_path(&binding.path, work)?); }
        Ok(Err(NodeFacts::Names { file_id: (inode != ROOT_INODE).then_some(entry.lookup.node.file_id), paths }))
    }

    pub(super) fn node_attr(&self, inode: u64) -> Result<FileAttr, i32> {
        let lookup = self.by_inode.get(&inode).ok_or(libc::ESTALE)?.lookup;
        self.attr(inode, &lookup)
    }

    pub(super) fn attr(&self, inode: u64, lookup: &MountLookup) -> Result<FileAttr, i32> {
        let node = lookup.node;
        let kind = file_type(node.kind)?;
        let defaults = self.defaults;
        let default_mode = if defaults.writable { 0o755 } else { 0o555 };
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
            uid: metadata_or(lookup.metadata.posix_uid, defaults.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, defaults.mount_gid),
            rdev: node
                .device
                .map(|(major, minor)| native_device_number(major, minor))
                .transpose()?
                .unwrap_or(0),
            blksize: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }

}
