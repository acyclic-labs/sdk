use super::*;

pub(super) struct Binding {
    pub(super) path: FuseOwned<MountPath>,
    pub(super) verified: Option<ViewStamp>,
    pub(super) kernel: Option<ViewStamp>,
}

pub(super) struct InodeEntry {
    pub(super) bindings: FuseOwned<Vec<Binding>>,
    pub(super) lookup: MountLookup,
    pub(super) facts: Option<ViewStamp>,
    pub(super) kernel: Option<ViewStamp>,
    pub(super) lookup_references: u64,
    pub(super) open_handles: u64,
    pub(super) cached_content: Option<ContentVersion>,
    pub(super) negative_children: HashMap<FuseOwned<Vec<u8>>, ViewStamp>,
    pub(super) empty_since: Option<ViewStamp>,
    pub(super) _storage: crate::NativeAllocation,
}

impl InodeEntry {
    pub(super) fn new(
        binding: FuseOwned<MountPath>,
        lookup: MountLookup,
        verified: Option<ViewStamp>,
        lookup_references: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let mut bindings = fuse_vec(1, work)?;
        bindings.push(Binding {
            path: binding,
            verified,
            kernel: None,
        });
        let storage = work.claim(
            std::mem::size_of::<(u64, InodeEntry)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Self {
            bindings,
            lookup,
            facts: verified,
            kernel: None,
            lookup_references,
            open_handles: 0,
            cached_content: None,
            negative_children: HashMap::new(),
            empty_since: None,
            _storage: storage,
        })
    }

    pub(super) fn binding(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<&Binding>, NativeMountError> {
        for binding in self.bindings.iter() {
            fuse_item(work)?;
            if *binding.path == *path {
                return Ok(Some(binding));
            }
        }
        Ok(None)
    }

    pub(super) fn binding_mut(
        &mut self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<&mut Binding>, NativeMountError> {
        for binding in self.bindings.iter_mut() {
            fuse_item(work)?;
            if *binding.path == *path {
                return Ok(Some(binding));
            }
        }
        Ok(None)
    }

    pub(super) fn anchor(&self) -> &MountPath {
        self.bindings
            .first()
            .map_or(&ROOT_PATH, |binding| &*binding.path)
    }

    pub(super) fn current_facts(
        &self,
        source: &dyn MountFilesystem,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<ViewStamp>, NativeMountError> {
        let Some(stamp) = self.facts else {
            return Ok(None);
        };
        let file_id = self.lookup.node.file_id;
        let current = if self.lookup.node.kind == MountNodeKind::Directory {
            match self.bindings.first() {
                Some(anchor) => {
                    source.unchanged_since_with_meter(&anchor.path, Some(file_id), stamp, work)?
                }
                None => false,
            }
        } else {
            source.node_unchanged_since_with_meter(file_id, stamp, work)?
        };
        Ok(current.then_some(stamp))
    }

    pub(super) fn lacks_named_attributes(
        &self,
        source: &dyn MountFilesystem,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        Ok(
            self.lookup.metadata.named_attributes == MetadataField::Unavailable
                && self.current_facts(source, work)?.is_some(),
        )
    }

    pub(super) fn remember_negative_child(
        &mut self,
        name: &[u8],
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        fuse_item(work)?;
        if let Some(held) = self.negative_children.get_mut(name) {
            *held = (*held).min(stamp);
            return Ok(true);
        }
        if self.negative_children.len() >= MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY {
            return Ok(false);
        }
        let mut name = fuse_copy_bytes(name, work)?;
        name.claim(
            std::mem::size_of::<(FuseOwned<Vec<u8>>, ViewStamp)>() as u64,
            work,
        )?;
        self.negative_children
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        self.negative_children.insert(name, stamp);
        Ok(true)
    }
}

pub(super) fn hold(held: &mut Option<ViewStamp>, stamp: ViewStamp) {
    *held = Some(held.map_or(stamp, |held| held.min(stamp)));
}

impl ProjectionState {
    pub(super) fn remove_path_cache(
        &mut self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        fuse_item(work)?;
        if let Some(inode) = self.inode_by_path.get(path).copied() {
            self.remove_binding(inode, path, work)?;
        }
        Ok(())
    }

    pub(super) fn remove_binding(
        &mut self,
        inode: u64,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        unbind(
            &mut self.by_inode,
            &mut self.inode_by_file,
            inode,
            path,
            work,
        )?;
        if self.inode_by_path.get(path) == Some(&inode) {
            self.inode_by_path.remove(path);
        }
        Ok(())
    }

    pub(super) fn invalidate_prefix(
        &mut self,
        prefix: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut affected = fuse_vec(0, work)?;
        for (inode, entry) in &self.by_inode {
            for binding in entry.bindings.iter() {
                fuse_item(work)?;
                if fuse_has_prefix(&binding.path, prefix, work)? {
                    let path = fuse_clone_path(&binding.path, work)?;
                    fuse_reserve(&mut affected, 1, work)?;
                    affected.push((*inode, path));
                }
            }
        }
        for (inode, path) in affected.drain(..) {
            self.remove_binding(inode, &path, work)?;
        }
        Ok(())
    }

    pub(super) fn rename_prefix(
        &mut self,
        source: &MountPath,
        destination: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut replacements = fuse_vec(0, work)?;
        let mut paths = HashMap::new();
        for (inode, entry) in &self.by_inode {
            for (index, binding) in entry.bindings.iter().enumerate() {
                fuse_item(work)?;
                let rebased = fuse_replace_prefix(&binding.path, source, destination, work)?;
                let mut key = fuse_clone_path(rebased.as_deref().unwrap_or(&binding.path), work)?;
                key.claim(
                    std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64,
                    work,
                )?;
                paths.try_reserve(1).map_err(NativeMountError::Allocation)?;
                let previous = paths.insert(key, *inode);
                debug_assert!(previous.is_none(), "a path is bound to one inode");
                if let Some(path) = rebased {
                    fuse_reserve(&mut replacements, 1, work)?;
                    replacements.push((*inode, index, path));
                }
            }
        }
        // Prepare every path before changing aliases.
        self.streams.rename_prefix(source, destination, work)?;
        for (inode, index, path) in replacements.drain(..) {
            self.by_inode
                .get_mut(&inode)
                .expect("planned inode remains locked")
                .bindings[index]
                .path = path;
        }
        self.inode_by_path = paths;
        Ok(())
    }
}

pub(super) fn cached_projected_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<FuseOwned<MountPath>, u64>,
    path: &MountPath,
    unchanged_since: impl FnOnce(crate::FileId, ViewStamp) -> Result<bool, NativeMountError>,
    work: &crate::NativeMaterializationWork,
) -> Result<Option<(u64, MountLookup, ViewStamp)>, NativeMountError> {
    fuse_item(work)?;
    let Some(inode) = inode_by_path.get(path).copied() else {
        return Ok(None);
    };
    let Some(entry) = by_inode.get_mut(&inode) else {
        return Ok(None);
    };
    let Some(stamp) = entry
        .binding(path, work)?
        .and_then(|binding| binding.verified)
        .zip(entry.facts)
        .map(|(name, facts)| name.min(facts))
    else {
        return Ok(None);
    };
    if !unchanged_since(entry.lookup.node.file_id, stamp)? {
        return Ok(None);
    }
    entry.lookup_references = entry.lookup_references.saturating_add(1);
    Ok(Some((inode, entry.lookup, stamp)))
}

pub(super) fn replace_root_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    lookup: MountLookup,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    fuse_item(work)?;
    if lookup.node.kind != MountNodeKind::Directory {
        return Err(libc::EIO.into());
    }
    let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
    let previous = root.lookup.node.file_id;
    if previous != lookup.node.file_id {
        if inode_by_file
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
        inode_by_file
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        forget_file(inode_by_file, previous, ROOT_INODE);
        inode_by_file.insert(lookup.node.file_id, (ROOT_INODE, storage));
        root.lookup = lookup;
    }
    Ok(())
}

pub(super) fn forget_unbound_names(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<FuseOwned<MountPath>, u64>,
    inode: u64,
    path: &MountPath,
    still_bound: impl Fn(&MountPath, Option<ViewStamp>) -> Result<bool, NativeMountError>,
    deferred: &mut FuseOwned<Vec<KernelCacheItem>>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let mut items = fuse_vec(0, work)?;
    let Some(entry) = by_inode.get(&inode) else {
        return Ok(());
    };
    let mut removed = fuse_vec(0, work)?;
    for (index, binding) in entry.bindings.iter().enumerate() {
        fuse_item(work)?;
        if *binding.path == *path || still_bound(&binding.path, binding.verified)? {
            continue;
        }
        if binding.kernel.is_some()
            && let Some(parent) = fuse_parent(&binding.path, work)?
            && let Some(parent_inode) = inode_by_path.get(&*parent).copied()
        {
            let component = binding.path.components().last().ok_or(libc::ESTALE)?;
            let name = fuse_copy_bytes(component, work)?;
            fuse_reserve(&mut items, 1, work)?;
            items.push(KernelCacheItem::Entry {
                parent: parent_inode,
                name,
            });
        }
        fuse_reserve(&mut removed, 1, work)?;
        removed.push(index);
    }
    let width = std::mem::size_of::<Binding>();
    let mut remaining = entry.bindings.len();
    let mut copied = 0_u64;
    for index in removed.iter().rev().copied() {
        let shifted = (remaining - index - 1)
            .checked_mul(width)
            .ok_or(crate::WorkError::Overflow)? as u64;
        copied = copied
            .checked_add(shifted)
            .ok_or(crate::WorkError::Overflow)?;
        remaining -= 1;
    }
    fuse_reserve(deferred, items.len(), work)?;
    let appended = items
        .len()
        .checked_mul(std::mem::size_of::<KernelCacheItem>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    fuse_copy_admit(
        copied
            .checked_add(appended)
            .ok_or(crate::WorkError::Overflow)?,
        work,
    )?;
    deferred.append(&mut items);
    fuse_copy_record(appended, work)?;
    let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
    for index in removed.iter().rev().copied() {
        let binding = entry.bindings.remove(index);
        if inode_by_path.get(&*binding.path) == Some(&inode) {
            inode_by_path.remove(&*binding.path);
        }
    }
    fuse_copy_record(copied, work)
}

pub(super) fn bind_path(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<FuseOwned<MountPath>, u64>,
    inode_by_file: &mut HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    path: &MountPath,
    inode: u64,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    fuse_item(work)?;
    if let Some(previous) = inode_by_path.get(path).copied() {
        if previous != inode {
            unbind(by_inode, inode_by_file, previous, path, work)?;
        }
        *inode_by_path
            .get_mut(path)
            .expect("existing path remains locked") = inode;
        return Ok(());
    }
    let mut key = fuse_clone_path(path, work)?;
    key.claim(
        std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64,
        work,
    )?;
    inode_by_path
        .try_reserve(1)
        .map_err(NativeMountError::Allocation)?;
    inode_by_path.insert(key, inode);
    Ok(())
}

pub(super) fn unbind(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    inode: u64,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let Some(entry) = by_inode.get_mut(&inode) else {
        return Ok(());
    };
    let mut matched = None;
    for (index, binding) in entry.bindings.iter().enumerate() {
        fuse_item(work)?;
        if *binding.path == *path {
            matched = Some(index);
            break;
        }
    }
    if let Some(index) = matched {
        let copied = (entry.bindings.len() - index - 1)
            .checked_mul(std::mem::size_of::<Binding>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        fuse_copy_admit(copied, work)?;
        entry.bindings.remove(index);
        fuse_copy_record(copied, work)?;
    }
    if entry.bindings.is_empty() && entry.lookup_references == 0 && entry.open_handles == 0 {
        let entry = by_inode
            .remove(&inode)
            .expect("matched inode remains locked");
        forget_file(inode_by_file, entry.lookup.node.file_id, inode);
    }
    Ok(())
}

pub(super) fn forget_file(
    inode_by_file: &mut HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    file_id: crate::FileId,
    inode: u64,
) {
    if inode_by_file
        .get(&file_id)
        .is_some_and(|(held, _)| *held == inode)
    {
        inode_by_file.remove(&file_id);
    }
}

pub(super) fn retire_reused_identity(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    inode: u64,
) {
    if inode != ROOT_INODE
        && let Some(entry) = by_inode.get(&inode)
        && entry.bindings.is_empty()
    {
        forget_file(inode_by_file, entry.lookup.node.file_id, inode);
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "projection indexes are updated together"
)]
pub(super) fn intern_projected(
    next_inode: &mut u64,
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<FuseOwned<MountPath>, u64>,
    inode_by_file: &mut HashMap<crate::FileId, (u64, crate::NativeAllocation)>,
    path: FuseOwned<MountPath>,
    lookup: &MountLookup,
    stamp: Option<ViewStamp>,
    lookup_reference: bool,
    work: &crate::NativeMaterializationWork,
) -> Result<u64, NativeMountError> {
    fuse_item(work)?;
    if let Some(inode) = inode_by_file
        .get(&lookup.node.file_id)
        .map(|(inode, _)| *inode)
    {
        let needs_binding = by_inode
            .get(&inode)
            .ok_or(libc::ESTALE)?
            .binding(&path, work)?
            .is_none();
        if needs_binding {
            fuse_reserve(
                &mut by_inode.get_mut(&inode).ok_or(libc::ESTALE)?.bindings,
                1,
                work,
            )?;
        }
        bind_path(by_inode, inode_by_path, inode_by_file, &path, inode, work)?;
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if needs_binding {
            entry.bindings.push(Binding {
                path,
                verified: stamp,
                kernel: None,
            });
        } else if let Some(binding) = entry.binding_mut(&path, work)? {
            binding.verified = stamp;
        }
        entry.lookup = *lookup;
        entry.facts = stamp;
        if lookup_reference {
            entry.lookup_references = entry.lookup_references.saturating_add(1);
        }
        return Ok(inode);
    }
    let inode = *next_inode;
    if inode <= ROOT_INODE {
        return Err(libc::EOVERFLOW.into());
    }
    let following = inode.checked_add(1).ok_or(libc::EOVERFLOW)?;
    let mut key = fuse_clone_path(&path, work)?;
    key.claim(
        std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64,
        work,
    )?;
    let entry = InodeEntry::new(path, *lookup, stamp, u64::from(lookup_reference), work)?;
    let storage = work.claim(
        std::mem::size_of::<(crate::FileId, (u64, crate::NativeAllocation))>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    by_inode
        .try_reserve(1)
        .map_err(NativeMountError::Allocation)?;
    inode_by_path
        .try_reserve(1)
        .map_err(NativeMountError::Allocation)?;
    inode_by_file
        .try_reserve(1)
        .map_err(NativeMountError::Allocation)?;
    if let Some(previous) = inode_by_path.get(&*key).copied() {
        unbind(by_inode, inode_by_file, previous, &key, work)?;
    }
    inode_by_path.insert(key, inode);
    inode_by_file.insert(lookup.node.file_id, (inode, storage));
    by_inode.insert(inode, entry);
    *next_inode = following;
    Ok(inode)
}
