use super::*;

pub(super) fn confirm_name(
    names: &mut HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
    file_id: crate::FileId, mut name: FuseOwned<MountPath>, stamp: Option<ViewStamp>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), FuseRequestError> {
    for ((known_id, known_name), held) in names.iter_mut() {
        fuse_item(work)?;
        if *known_id == file_id && **known_name == *name {
            *held = stamp;
            return Ok(());
        }
    }
    name.claim(std::mem::size_of::<((crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>)>() as u64, work)?;
    names.try_reserve(1).map_err(NativeMountError::Allocation)?;
    names.insert((file_id, name), stamp);
    Ok(())
}

pub(super) fn name_is_bound(
    names: &HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
    source: &dyn MountFilesystem, file_id: crate::FileId, name: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<bool, FuseRequestError> {
    for ((known_id, known_name), stamp) in names {
        fuse_item(work)?;
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
    pub(super) exhausted: bool,
    pub(super) emitted: u64,
    pub(super) parked: u64,
    pub(super) park_replacement: Option<(u64, u64)>,
    pub(super) storage: FuseOwned<()>,
}

impl DirectoryStream {
    pub(super) fn entries_remaining(&self) -> usize {
        self.page.as_ref().map_or(0, |page| page.entries().len().saturating_sub(self.entry_position))
    }

    pub(super) fn cursor(&self) -> Option<&[u8]> {
        self.page.as_ref().and_then(|page| page.next_cursor())
    }
}

#[derive(Default)]
pub(super) struct DirectoryStreams {
    pub(super) waiting: HashMap<(u64, u64), DirectoryStream>,
    pub(super) parked: u64,
}

impl DirectoryStreams {
    pub(super) fn take(&mut self, inode: u64, offset: u64) -> Option<DirectoryStream> {
        let mut stream = self.waiting.remove(&(inode, offset))?;
        // The initialized table row is gone; page/path owners remain live.
        drop(std::mem::replace(&mut stream.storage, FuseOwned::empty(())));
        stream.park_replacement = None;
        Some(stream)
    }

    pub(super) fn prepare_park(&mut self, stream: &mut DirectoryStream,
        work: &crate::NativeMaterializationWork) -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        let storage = fuse_initialized((), std::mem::size_of::<((u64, u64), DirectoryStream)>() as u64, work)?;
        self.waiting.try_reserve(1).map_err(NativeMountError::Allocation)?;
        let mut oldest = None;
        if self.waiting.len() >= MAXIMUM_DIRECTORY_STREAMS {
            for (key, candidate) in &self.waiting {
                fuse_item(work)?;
                if oldest.is_none_or(|(_, parked)| candidate.parked < parked) {
                    oldest = Some((*key, candidate.parked));
                }
            }
        }
        stream.park_replacement = oldest.map(|(key, _)| key);
        stream.storage = storage;
        Ok(())
    }

    pub(super) fn park(&mut self, inode: u64, mut stream: DirectoryStream)
    {
        if let Some(key) = stream.park_replacement.take() { drop(self.waiting.remove(&key)); }
        self.parked = self.parked.wrapping_add(1);
        stream.parked = self.parked;
        self.waiting.insert((inode, stream.emitted), stream);
    }

    pub(super) fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath,
        work: &crate::NativeMaterializationWork) -> Result<(), FuseRequestError>
    {
        let mut replacements = fuse_vec(0, work)?;
        for (key, stream) in &self.waiting {
            fuse_item(work)?;
            if let Some(path) = fuse_replace_prefix(&stream.path, source, destination, work)? {
                fuse_reserve(&mut replacements, 1, work)?;
                replacements.push((*key, path));
            }
        }
        for (key, path) in replacements {
            self.waiting.get_mut(&key).expect("planned stream remains locked").path = path;
        }
        Ok(())
    }
}

impl ProjectionState {
    pub(super) fn directory_open_flags(source: &dyn MountFilesystem, work: &crate::NativeMaterializationWork)
        -> Result<FopenFlags, FuseRequestError>
    {
        Ok(if source.view_stamp_with_meter(work)?.is_some() {
            FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE
        } else { FopenFlags::empty() })
    }

    pub(super) fn directory_stream(&mut self, source: &dyn MountFilesystem, inode: u64, offset: u64,
        work: &crate::NativeMaterializationWork) -> Result<DirectoryStream, FuseRequestError>
    {
        fuse_item(work)?;
        if let Some(stream) = self.streams.take(inode, offset) { return Ok(stream); }
        if !source.view_is_stable_with_meter(work)? { return Err(libc::ESTALE.into()); }
        let path = fuse_clone_path(self.path(inode)?, work)?;
        let parent_inode = fuse_parent(&path, work)?
            .and_then(|parent| self.inode_by_path.get(&*parent).copied()).unwrap_or(ROOT_INODE);
        Ok(DirectoryStream {
            path, parent_inode, binding_epoch: source.binding_epoch_with_meter(work)?,
            page: None, entry_position: 0, entries_stamp: None, confirmed: HashMap::new(),
            exhausted: false, emitted: 0, parked: 0, park_replacement: None, storage: FuseOwned::empty(()),
        })
    }

    pub(super) fn list_directory(
        &mut self, source: &dyn MountFilesystem, inode: u64, stream: &mut DirectoryStream,
        listing: &mut Option<DirectoryReply>, work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
        let counts_lookups = listing.as_ref().expect("unsent listing").counts_lookups();
        let mut listed = false;
        let mut unlisted_reference = None;
        let result = (|| -> Result<(), FuseRequestError> {
            if stream.emitted < 2 {
                let attr = self.node_attr(inode)?;
                let resumed = stream.emitted;
                let dots = [(inode, ".", 1), (stream.parent_inode, "..", 2)];
                for (dot_inode, name, next) in dots.into_iter().filter(|dot| dot.2 > resumed) {
                    fuse_item(work)?;
                    let dot = FileAttr { ino: INodeNo(dot_inode), ..attr };
                    fuse_copy_admit(name.len() as u64, work)?;
                    if listing.as_mut().expect("unsent listing").push(
                        next, OsStr::new(name), &dot, &Duration::ZERO)
                    {
                        listing.take().expect("unsent listing").ok();
                        return Ok(());
                    }
                    fuse_copy_record(name.len() as u64, work)?;
                    stream.emitted = next;
                }
            }
            self.admit_listing(source, inode, stream.entries_stamp, work)?;
            loop {
                let next = stream.emitted.saturating_add(1);
                let Some(entry) = stream.page.as_ref()
                    .and_then(|page| page.entries().get(stream.entry_position))
                else {
                    listing.take().expect("unsent listing").ok();
                    return Ok(());
                };
                fuse_item(work)?;
                let path = fuse_child_path(&stream.path, &entry.name, work)?;
                let child = ChildName::new(source, path, work)?;
                let current = match stream.entries_stamp {
                    Some(stamp) if source.unchanged_since_with_meter(&stream.path, None, stamp, work)?
                        && source.unchanged_since_with_meter(child.key(), Some(entry.node.file_id), stamp, work)? =>
                            Some(stamp),
                    _ => None,
                };
                let page_lookup = MountLookup { node: entry.node, metadata: entry.metadata };
                // A stale page must not roll back facts mounted writes refreshed.
                let lookup = if current.is_some() { page_lookup } else {
                    self.inode_by_file.get(&page_lookup.node.file_id)
                        .and_then(|known| self.by_inode.get(known)).map_or(page_lookup, |known| known.lookup)
                };
                let child_inode = self.intern(source, fuse_clone_path(child.key(), work)?, &lookup,
                    current, counts_lookups, &stream.confirmed, work)?;
                if counts_lookups { unlisted_reference = Some(child_inode); }
                let attr = self.attr(child_inode, &lookup)?;
                let ttl = if counts_lookups {
                    self.admit_entry(source, child_inode, &child, current, work)?
                } else { Duration::ZERO };
                fuse_copy_admit(entry.name.len() as u64, work)?;
                if listing.as_mut().expect("unsent listing").push(
                    next, OsStr::from_bytes(&entry.name), &attr, &ttl)
                {
                    if let Some(inode) = unlisted_reference.take() {
                        self.release_lookup_reference_administrative(inode, 1);
                    }
                    listing.take().expect("unsent listing").ok();
                    return Ok(());
                }
                fuse_copy_record(entry.name.len() as u64, work)?;
                unlisted_reference = None;
                listed = true;
                stream.emitted = next;
                stream.entry_position += 1;
            }
        })();
        if let Some(inode) = unlisted_reference {
            self.release_lookup_reference_administrative(inode, 1);
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
    pub(super) fn counts_lookups(&self) -> bool { matches!(self, Self::Plus(_)) }

    pub(super) fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, ttl: &Duration) -> bool {
        match self {
            Self::Basic(reply) => reply.add(attr.ino, offset, attr.kind, name),
            Self::Plus(reply) => reply.add(attr.ino, offset, name, ttl, attr, Generation(0)),
        }
    }

    pub(super) fn ok(self) {
        match self { Self::Basic(reply) => reply.ok(), Self::Plus(reply) => reply.ok() }
    }

    pub(super) fn error(self, error: Errno) {
        match self { Self::Basic(reply) => reply.error(error), Self::Plus(reply) => reply.error(error) }
    }
}

impl FuseProjection {
    pub(super) fn list_directory(
        &self,
        inode: u64,
        offset: u64,
        listing: &mut Option<DirectoryReply>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let mut stream = self.core.state()?.directory_stream(source, inode, offset, work)?;
        self.position_directory_stream(&mut stream, offset, work)?;
        let expected = stream.binding_epoch;
        let _lease = source.acquire_binding_lease_with_meter(expected, work)
            .map_err(FuseRequestError::from)?;
        if !source.view_is_stable_with_meter(work)? || source.binding_epoch_with_meter(work)? != expected {
            return Err(libc::ESTALE.into());
        }
        let mut state = self.core.state()?;
        state.streams.prepare_park(&mut stream, work)?;
        state.list_directory(source, inode, &mut stream, listing, work)?;
        state.streams.park(inode, stream);
        Ok(())
    }

    pub(super) fn position_directory_stream(
        &self,
        stream: &mut DirectoryStream,
        offset: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
        let source = self.core.source.as_ref();
        stream.emitted = stream.emitted.max(offset.min(2));
        loop {
            fuse_item(work)?;
            if stream.emitted < offset && stream.entries_remaining() != 0 {
                stream.entry_position += 1;
                stream.emitted += 1;
                continue;
            }
            if stream.entries_remaining() != 0 || stream.exhausted { return Ok(()); }
            let lease = source.acquire_binding_lease_with_meter(stream.binding_epoch, work)
                .map_err(FuseRequestError::from)?;
            let stamp = source.view_stamp_with_meter(work)?;
            let page = source.read_directory_with_meter(&stream.path, stream.cursor(), DIRECTORY_PAGE_SIZE, work)
                .map_err(FuseRequestError::from)?;
            if !source.view_is_stable_with_meter(work)? || source.binding_epoch_with_meter(work)? != stream.binding_epoch {
                return Err(libc::ESTALE.into());
            }
            stream.exhausted = page.next_cursor().is_none();
            drop(lease);
            stream.confirmed = self.confirm_listed_names(&stream.path, page.entries(), stamp, work)?;
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
    ) -> Result<HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>, FuseRequestError> {
        let source = self.core.source.as_ref();
        let mut listed = fuse_vec(entries.len(), work)?;
        for entry in entries {
            fuse_item(work)?;
            if entry.node.link_count <= 1 { continue; }
            let name = ChildName::new(source, fuse_child_path(directory, &entry.name, work)?, work)?;
            listed.push((fuse_clone_path(name.key(), work)?, entry.node));
        }
        let mut confirmed = self.confirm_names(
            HashMap::new(),
            listed.iter().map(|(path, node)| (&**path, *node)),
            work,
        )?;
        for (path, node) in listed.drain(..) {
            confirm_name(&mut confirmed, node.file_id, path, stamp, work)?;
        }
        Ok(confirmed)
    }

}
