use super::*;

pub(super) struct Binding {
    pub(super) spelled: Option<FuseOwned<MountPath>>,
    pub(super) path: FuseOwned<MountPath>,
    pub(super) verified: Option<ViewStamp>,
    pub(super) kernel: Option<ViewStamp>,
}

impl Binding {
    pub(super) fn native_path(&self) -> &MountPath {
        self.spelled.as_deref().unwrap_or(&self.path)
    }
}

pub(super) struct InodeEntry {
    pub(super) bindings: FuseOwned<Vec<Binding>>,
    pub(super) lookup: MountLookup,
    pub(super) facts: Option<ViewStamp>,
    pub(super) kernel: Option<ViewStamp>,
    pub(super) lookup_references: u64,
    pub(super) open_handles: u64,
    pub(super) cached_content: Option<ContentVersion>,
    pub(super) kernel_content: Option<ContentVersion>,
    pub(super) write_handles: u64,
    /// The listing reply that last handed the kernel this inode's entry.
    pub(super) listed_in_reply: u64,
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
            spelled: None,
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
            kernel_content: None,
            write_handles: 0,
            listed_in_reply: 0,
            negative_children: HashMap::new(),
            empty_since: None,
            _storage: storage,
        })
    }

    pub(super) fn kernel_reads(&self, opened: &MountLookup) -> bool {
        self.kernel_content
            .is_none_or(|held| held == ContentVersion::of(opened))
    }

    pub(super) fn binding(&self, path: &MountPath) -> Option<&Binding> {
        self.bindings
            .iter()
            .find(|binding| *binding.path == *path || binding.native_path() == path)
    }

    pub(super) fn binding_mut(&mut self, path: &MountPath) -> Option<&mut Binding> {
        self.bindings
            .iter_mut()
            .find(|binding| *binding.path == *path || binding.native_path() == path)
    }

    pub(super) fn anchor(&self) -> &MountPath {
        self.bindings
            .first()
            .map_or(&ROOT_PATH, Binding::native_path)
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
                Some(anchor) => source.unchanged_since_with_meter(
                    anchor.native_path(),
                    Some(file_id),
                    stamp,
                    work,
                )?,
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
        work.charge(Items.delta(1))?;
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
    pub(super) fn remove_binding(
        &mut self,
        inode: u64,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.unbind(inode, path, work)?;
        if self.inode_by_path.get(path) == Some(&inode) {
            self.inode_by_path.remove(path);
        }
        Ok(())
    }

    #[allow(
        clippy::mutable_key_type,
        reason = "FuseOwned hashes and compares only its immutable path, never its accounting owner."
    )]
    pub(super) fn prepare_rename_prefix(
        &self,
        source: &MountPath,
        destination: &MountPath,
        spelled: &MountPath,
        parent_inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<impl FnOnce(&mut Self) + use<>, NativeMountError> {
        let mut replacements = fuse_vec(0, work)?;
        let mut paths = HashMap::new();
        let mut copied = 0u64;
        for (inode, entry) in &self.by_inode {
            let mut retained = entry.bindings.len();
            // Descending indices keep all prepared replacements valid when
            // destination bindings are removed at installation.
            for (index, binding) in entry.bindings.iter().enumerate().rev() {
                work.charge(Items.delta(1))?;
                let rebased = fuse_replace_prefix(&binding.path, source, destination, work)?;
                if rebased.is_none()
                    && source != destination
                    && has_prefix(&binding.path, destination)
                {
                    copied = copied
                        .checked_add(
                            ((retained - index - 1) * std::mem::size_of::<Binding>()) as u64,
                        )
                        .ok_or(crate::WorkError::Overflow)?;
                    retained -= 1;
                    fuse_reserve(&mut replacements, 1, work)?;
                    replacements.push((*inode, index, None));
                    continue;
                }
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
                    let native = if binding.spelled.is_some() || destination != spelled {
                        Some(fuse_rebase_path(
                            binding.native_path(),
                            source.components().len(),
                            spelled,
                            work,
                        )?)
                    } else {
                        None
                    };
                    replacements.push((*inode, index, Some((path, native))));
                }
            }
        }
        let streams = self.streams.prepare_rename_prefix(
            source,
            spelled,
            parent_inode,
            &self.by_inode,
            work,
        )?;
        work.charge(Copy.delta(copied))?;
        Ok(move |state: &mut Self| {
            streams(&mut state.streams);
            for (inode, index, replacement) in replacements.drain(..) {
                let Some(entry) = state.by_inode.get_mut(&inode) else {
                    continue;
                };
                match replacement {
                    Some((path, native)) => {
                        let binding = &mut entry.bindings[index];
                        binding.path = path;
                        binding.spelled = native;
                    }
                    None => {
                        entry.bindings.remove(index);
                    }
                }
                if entry.bindings.is_empty()
                    && entry.lookup_references == 0
                    && entry.open_handles == 0
                {
                    if let Some(entry) = state.by_inode.remove(&inode) {
                        state.forget_file(entry.lookup.node.file_id, inode);
                    }
                }
            }
            // FORGET may retire an unreferenced inode while the host mutation
            // runs; never reintroduce its precomputed path index entries.
            paths.retain(|_, inode| state.by_inode.contains_key(inode));
            state.inode_by_path = paths;
        })
    }
}

impl ProjectionState {
    #[allow(
        clippy::mutable_key_type,
        reason = "FuseOwned hashes and compares only its immutable path, never its accounting owner."
    )]
    pub(super) fn forget_unbound_names(
        &mut self,
        source: &dyn MountFilesystem,
        confirmed: &HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
        inode: u64,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut items = fuse_vec(0, work)?;
        let Some(entry) = self.by_inode.get(&inode) else {
            return Ok(());
        };
        let mut removed = fuse_vec(0, work)?;
        for (index, binding) in entry.bindings.iter().enumerate() {
            work.charge(Items.delta(1))?;
            if *binding.path == *path
                || name_is_bound(
                    confirmed,
                    source,
                    entry.lookup.node.file_id,
                    binding.native_path(),
                    work,
                )?
                || match binding.verified {
                    Some(stamp) => source.unchanged_since_with_meter(
                        binding.native_path(),
                        None,
                        stamp,
                        work,
                    )?,
                    None => false,
                }
            {
                continue;
            }
            if binding.kernel.is_some()
                && let Some(parent) = fuse_parent(&binding.path, work)?
                && let Some(parent_inode) = self.inode_by_path.get(&*parent).copied()
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
        fuse_reserve(&mut self.invalidation.deferred, items.len(), work)?;
        let appended = items
            .len()
            .checked_mul(std::mem::size_of::<KernelCacheItem>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        work.admit(
            Copy.delta(
                copied
                    .checked_add(appended)
                    .ok_or(crate::WorkError::Overflow)?,
            ),
        )?;
        self.invalidation.deferred.append(&mut items);
        work.record_admitted(Copy.delta(appended))?;
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        for index in removed.iter().rev().copied() {
            let binding = entry.bindings.remove(index);
            if self.inode_by_path.get(&*binding.path) == Some(&inode) {
                self.inode_by_path.remove(&*binding.path);
            }
        }
        work.record_admitted(Copy.delta(copied)).map_err(Into::into)
    }

    pub(super) fn bind_path(
        &mut self,
        path: &MountPath,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        work.charge(Items.delta(1))?;
        if let Some(previous) = self.inode_by_path.get(path).copied() {
            if previous != inode {
                self.unbind(previous, path, work)?;
            }
            *self.inode_by_path.get_mut(path).ok_or(libc::EIO)? = inode;
            return Ok(());
        }
        let mut key = fuse_clone_path(path, work)?;
        key.claim(
            std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64,
            work,
        )?;
        self.inode_by_path
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        self.inode_by_path.insert(key, inode);
        Ok(())
    }

    fn unbind(
        &mut self,
        inode: u64,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            return Ok(());
        };
        let mut matched = None;
        for (index, binding) in entry.bindings.iter().enumerate() {
            work.charge(Items.delta(1))?;
            if *binding.path == *path {
                matched = Some(index);
                break;
            }
        }
        if let Some(index) = matched {
            let copied = (entry.bindings.len() - index - 1)
                .checked_mul(std::mem::size_of::<Binding>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            work.admit(Copy.delta(copied))?;
            entry.bindings.remove(index);
            work.record_admitted(Copy.delta(copied))?;
        }
        if entry.bindings.is_empty() && entry.lookup_references == 0 && entry.open_handles == 0 {
            let entry = self.by_inode.remove(&inode).ok_or(libc::EIO)?;
            self.forget_file(entry.lookup.node.file_id, inode);
        }
        Ok(())
    }

    pub(super) fn forget_file(&mut self, file_id: crate::FileId, inode: u64) {
        if self
            .inode_by_file
            .get(&file_id)
            .is_some_and(|(held, _)| *held == inode)
        {
            self.inode_by_file.remove(&file_id);
        }
    }

    pub(super) fn intern_projected(
        &mut self,
        path: FuseOwned<MountPath>,
        lookup: &MountLookup,
        stamp: Option<ViewStamp>,
        lookup_reference: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        work.charge(Items.delta(1))?;
        if let Some(inode) = self
            .inode_by_file
            .get(&lookup.node.file_id)
            .map(|(inode, _)| *inode)
        {
            let needs_binding = self
                .by_inode
                .get(&inode)
                .ok_or(libc::ESTALE)?
                .binding(&path)
                .is_none();
            if needs_binding {
                fuse_reserve(
                    &mut self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?.bindings,
                    1,
                    work,
                )?;
            }
            self.bind_path(&path, inode, work)?;
            let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
            if needs_binding {
                entry.bindings.push(Binding {
                    spelled: None,
                    path,
                    verified: stamp,
                    kernel: None,
                });
            } else if let Some(binding) = entry.binding_mut(&path) {
                binding.verified = stamp;
            }
            entry.lookup = *lookup;
            entry.facts = stamp;
            if lookup_reference {
                entry.lookup_references = entry.lookup_references.saturating_add(1);
            }
            return Ok(inode);
        }
        let inode = self.next_inode;
        if inode < ROOT_INODE
            || (inode == ROOT_INODE && (!path.components().is_empty() || !self.by_inode.is_empty()))
        {
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
        self.by_inode
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        self.inode_by_path
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        self.inode_by_file
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        if let Some(previous) = self.inode_by_path.get(&*key).copied() {
            self.unbind(previous, &key, work)?;
        }
        self.inode_by_path.insert(key, inode);
        self.inode_by_file
            .insert(lookup.node.file_id, (inode, storage));
        self.by_inode.insert(inode, entry);
        self.next_inode = following;
        Ok(inode)
    }
}
