use super::*;

pub(super) fn confirm_name(
    names: &mut HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
    file_id: crate::FileId,
    mut name: FuseOwned<MountPath>,
    stamp: Option<ViewStamp>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    for ((known_id, known_name), held) in names.iter_mut() {
        work.charge(Items.delta(1))?;
        if *known_id == file_id && **known_name == *name {
            *held = stamp;
            return Ok(());
        }
    }
    name.claim(
        std::mem::size_of::<((crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>)>() as u64,
        work,
    )?;
    names.try_reserve(1).map_err(NativeMountError::Allocation)?;
    names.insert((file_id, name), stamp);
    Ok(())
}

pub(super) fn name_is_bound(
    names: &HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
    source: &dyn MountFilesystem,
    file_id: crate::FileId,
    name: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<bool, NativeMountError> {
    for ((known_id, known_name), stamp) in names {
        work.charge(Items.delta(1))?;
        if *known_id == file_id && **known_name == *name {
            return match stamp {
                Some(stamp) => Ok(source.unchanged_since_with_meter(name, None, *stamp, work)?),
                None => Ok(true),
            };
        }
    }
    Ok(false)
}

pub(super) struct DirectoryStream {
    pub(super) path: FuseOwned<MountPath>,
    pub(super) parent_inode: u64,
    pub(super) binding_epoch: Option<u64>,
    pub(super) page: Option<crate::native_mount::MountDirectoryPage>,
    pub(super) entry_position: usize,
    pub(super) entries_stamp: Option<ViewStamp>,
    pub(super) confirmed: HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
    pub(super) emitted: u64,
    pub(super) parked: u64,
}

#[derive(Default)]
pub(super) struct DirectoryStreams {
    pub(super) waiting: HashMap<(u64, u64), (DirectoryStream, crate::NativeAllocation)>,
    pub(super) parked: u64,
}

impl DirectoryStreams {
    pub(super) fn take(&mut self, inode: u64, offset: u64) -> Option<DirectoryStream> {
        self.waiting
            .remove(&(inode, offset))
            .map(|(stream, _)| stream)
    }

    pub(super) fn prepare_park(
        &mut self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(crate::NativeAllocation, Option<(u64, u64)>), NativeMountError> {
        work.charge(Items.delta(1))?;
        let storage = work.claim(
            std::mem::size_of::<((u64, u64), (DirectoryStream, crate::NativeAllocation))>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        self.waiting
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        let mut oldest = None;
        if self.waiting.len() >= MAXIMUM_DIRECTORY_STREAMS {
            for (key, (candidate, _)) in &self.waiting {
                work.charge(Items.delta(1))?;
                if oldest.is_none_or(|(_, parked)| candidate.parked < parked) {
                    oldest = Some((*key, candidate.parked));
                }
            }
        }
        Ok((storage, oldest.map(|(key, _)| key)))
    }

    pub(super) fn park(
        &mut self,
        inode: u64,
        mut stream: DirectoryStream,
        (storage, replacement): (crate::NativeAllocation, Option<(u64, u64)>),
    ) {
        if let Some(key) = replacement {
            self.waiting.remove(&key);
        }
        self.parked = self.parked.wrapping_add(1);
        stream.parked = self.parked;
        self.waiting
            .insert((inode, stream.emitted), (stream, storage));
    }

    pub(super) fn rename_prefix(
        &mut self,
        source: &MountPath,
        destination: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut replacements = fuse_vec(0, work)?;
        for (key, (stream, _)) in &self.waiting {
            work.charge(Items.delta(1))?;
            if let Some(path) = fuse_replace_prefix(&stream.path, source, destination, work)? {
                fuse_reserve(&mut replacements, 1, work)?;
                replacements.push((*key, path));
            }
        }
        for (key, path) in replacements.drain(..) {
            self.waiting
                .get_mut(&key)
                .expect("planned stream remains locked")
                .0
                .path = path;
        }
        Ok(())
    }
}

impl ProjectionState {
    pub(super) fn directory_open_flags(
        source: &dyn MountFilesystem,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FopenFlags, NativeMountError> {
        Ok(if source.view_stamp_with_meter(work)?.is_some() {
            FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE
        } else {
            FopenFlags::empty()
        })
    }

    pub(super) fn directory_stream(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        offset: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<DirectoryStream, NativeMountError> {
        work.charge(Items.delta(1))?;
        if let Some(stream) = self.streams.take(inode, offset) {
            return Ok(stream);
        }
        if !source.view_is_stable_with_meter(work)? {
            return Err(libc::ESTALE.into());
        }
        let path = fuse_clone_path(self.path(inode)?, work)?;
        let parent_inode = fuse_parent(&path, work)?
            .and_then(|parent| self.inode_by_path.get(&*parent).copied())
            .unwrap_or(ROOT_INODE);
        Ok(DirectoryStream {
            path,
            parent_inode,
            binding_epoch: source.binding_epoch_with_meter(work)?,
            page: None,
            entry_position: 0,
            entries_stamp: None,
            confirmed: HashMap::new(),
            emitted: 0,
            parked: 0,
        })
    }

    pub(super) fn list_directory(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        stream: &mut DirectoryStream,
        listing: &mut Option<DirectoryReply>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let counts_lookups = matches!(
            listing.as_ref().expect("unsent listing"),
            DirectoryReply::Plus(_)
        );
        let mut listed = false;
        let mut unlisted_reference = None;
        let result = (|| -> Result<(), NativeMountError> {
            if stream.emitted < 2 {
                let lookup = self.by_inode.get(&inode).ok_or(libc::ESTALE)?.lookup;
                let attr = self.attr(inode, &lookup)?;
                let resumed = stream.emitted;
                for (dot_inode, name, next) in [(inode, ".", 1), (stream.parent_inode, "..", 2)]
                    .into_iter()
                    .filter(|dot| dot.2 > resumed)
                {
                    work.charge(Items.delta(1))?;
                    let dot = FileAttr {
                        ino: INodeNo(dot_inode),
                        ..attr
                    };
                    work.admit(Copy.delta(name.len() as u64))?;
                    if listing.as_mut().expect("unsent listing").push(
                        next,
                        OsStr::new(name),
                        &dot,
                        &Duration::ZERO,
                    ) {
                        listing.take().expect("unsent listing").ok();
                        return Ok(());
                    }
                    work.record_admitted(Copy.delta(name.len() as u64))?;
                    stream.emitted = next;
                }
            }
            self.admit_listing(source, inode, stream.entries_stamp, work)?;
            loop {
                let next = stream.emitted.saturating_add(1);
                let Some(entry) = stream
                    .page
                    .as_ref()
                    .and_then(|page| page.entries().get(stream.entry_position))
                else {
                    listing.take().expect("unsent listing").ok();
                    return Ok(());
                };
                work.charge(Items.delta(1))?;
                let child = ChildName::new(
                    source,
                    fuse_child_path(&stream.path, &entry.name, work)?,
                    work,
                )?;
                let current = match stream.entries_stamp {
                    Some(stamp)
                        if source.unchanged_since_with_meter(
                            &stream.path,
                            None,
                            stamp,
                            work,
                        )? && source.unchanged_since_with_meter(
                            child.key(),
                            Some(entry.node.file_id),
                            stamp,
                            work,
                        )? =>
                    {
                        Some(stamp)
                    }
                    _ => None,
                };
                let page_lookup = MountLookup {
                    node: entry.node,
                    metadata: entry.metadata,
                };
                // A stale page must not roll back facts mounted writes refreshed.
                let lookup = if current.is_some() {
                    page_lookup
                } else {
                    self.inode_by_file
                        .get(&page_lookup.node.file_id)
                        .and_then(|(known, _)| self.by_inode.get(known))
                        .map_or(page_lookup, |known| known.lookup)
                };
                let child_inode = self.intern(
                    source,
                    fuse_clone_path(child.key(), work)?,
                    &lookup,
                    current,
                    counts_lookups,
                    &stream.confirmed,
                    work,
                )?;
                if counts_lookups {
                    unlisted_reference = Some(child_inode);
                }
                let attr = self.attr(child_inode, &lookup)?;
                let ttl = if counts_lookups {
                    self.admit_entry(source, child_inode, &child, current, work)?
                } else {
                    Duration::ZERO
                };
                work.admit(Copy.delta(entry.name.len() as u64))?;
                if listing.as_mut().expect("unsent listing").push(
                    next,
                    OsStr::from_bytes(&entry.name),
                    &attr,
                    &ttl,
                ) {
                    if let Some(inode) = unlisted_reference.take() {
                        self.release_lookup_reference(inode, 1);
                    }
                    listing.take().expect("unsent listing").ok();
                    return Ok(());
                }
                work.record_admitted(Copy.delta(entry.name.len() as u64))?;
                unlisted_reference = None;
                listed = true;
                stream.emitted = next;
                stream.entry_position += 1;
            }
        })();
        if let Some(inode) = unlisted_reference {
            self.release_lookup_reference(inode, 1);
        }
        if result.is_err() && listed {
            listing.take().expect("unsent listing prefix").ok();
        }
        result
    }
}

pub(super) enum DirectoryReply {
    Basic(ReplyDirectory),
    Plus(ReplyDirectoryPlus),
}

impl DirectoryReply {
    pub(super) fn push(
        &mut self,
        offset: u64,
        name: &OsStr,
        attr: &FileAttr,
        ttl: &Duration,
    ) -> bool {
        match self {
            Self::Basic(reply) => reply.add(attr.ino, offset, attr.kind, name),
            Self::Plus(reply) => reply.add(attr.ino, offset, name, ttl, attr, Generation(0)),
        }
    }
    pub(super) fn ok(self) {
        match self {
            Self::Basic(reply) => reply.ok(),
            Self::Plus(reply) => reply.ok(),
        }
    }
    pub(super) fn error(self, error: Errno) {
        match self {
            Self::Basic(reply) => reply.error(error),
            Self::Plus(reply) => reply.error(error),
        }
    }
}

impl FuseProjection {
    pub(super) fn list_directory(
        &self,
        inode: u64,
        offset: u64,
        listing: &mut Option<DirectoryReply>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let mut stream = self
            .core
            .state()?
            .directory_stream(source, inode, offset, work)?;
        self.position_directory_stream(&mut stream, offset, work)?;
        let expected = stream.binding_epoch;
        let _lease = source.acquire_binding_lease_with_meter(expected, work)?;
        if !source.view_is_stable_with_meter(work)?
            || source.binding_epoch_with_meter(work)? != expected
        {
            return Err(libc::ESTALE.into());
        }
        let mut state = self.core.state()?;
        let parking = state.streams.prepare_park(work)?;
        let result = state.list_directory(source, inode, &mut stream, listing, work);
        state.streams.park(inode, stream, parking);
        result
    }

    pub(super) fn position_directory_stream(
        &self,
        stream: &mut DirectoryStream,
        offset: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let source = self.core.source.as_ref();
        stream.emitted = stream.emitted.max(offset.min(2));
        loop {
            work.charge(Items.delta(1))?;
            if let Some(page) = &stream.page {
                if stream.entry_position < page.entries().len() {
                    if stream.emitted >= offset {
                        return Ok(());
                    }
                    stream.entry_position += 1;
                    stream.emitted += 1;
                    continue;
                }
                if page.next_cursor().is_none() {
                    return Ok(());
                }
            }
            let lease = source.acquire_binding_lease_with_meter(stream.binding_epoch, work)?;
            let stamp = source.view_stamp_with_meter(work)?;
            let cursor = stream.page.as_ref().and_then(|page| page.next_cursor());
            let page = source.read_directory_with_meter(
                &stream.path,
                cursor,
                DIRECTORY_PAGE_SIZE,
                work,
            )?;
            if !source.view_is_stable_with_meter(work)?
                || source.binding_epoch_with_meter(work)? != stream.binding_epoch
            {
                return Err(libc::ESTALE.into());
            }
            drop(lease);
            stream.confirmed =
                self.confirm_listed_names(&stream.path, page.entries(), stamp, work)?;
            stream.page = Some(page);
            stream.entry_position = 0;
            stream.entries_stamp = stamp;
        }
    }

    pub(super) fn confirm_listed_names(
        &self,
        directory: &MountPath,
        entries: &[MountDirectoryEntry],
        stamp: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>, NativeMountError>
    {
        let source = self.core.source.as_ref();
        let mut listed = fuse_vec(entries.len(), work)?;
        for entry in entries {
            work.charge(Items.delta(1))?;
            if entry.node.link_count <= 1 {
                continue;
            }
            let name =
                ChildName::new(source, fuse_child_path(directory, &entry.name, work)?, work)?;
            listed.push((fuse_clone_path(name.key(), work)?, entry.node));
        }
        let mut confirmed = HashMap::new();
        for (path, node) in listed.iter() {
            self.confirm_names(path, *node, &mut confirmed, work)?;
        }
        for (path, node) in listed.drain(..) {
            confirm_name(&mut confirmed, node.file_id, path, stamp, work)?;
        }
        Ok(confirmed)
    }
}
