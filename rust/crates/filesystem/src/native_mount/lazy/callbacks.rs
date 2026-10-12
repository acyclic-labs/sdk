use super::*;

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn supports_posix_named_attributes(&self) -> bool {
        self.authored.supports_posix_named_attributes()
    }

    fn flush_on_handle_close(&self) -> bool {
        false
    }

    fn flush_publishes(&self) -> bool {
        self.authored.flush_publishes()
    }

    fn view_is_stable(&self) -> bool {
        self.source_view.is_stable()
    }

    /// Only a view whose source reports every change can be cached: the
    fn view_stamp(&self) -> Option<ViewStamp> {
        self.ledger_stamp().filter(|_| self.source_observed())
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        self.authored.observe_view(observer);
    }

    fn fence_changes(&self) -> Result<(), MountSourceError> {
        self.source_watch.as_ref().map_or(Ok(()), |watch| {
            watch.fence().map_err(|error| lazy_error(error.into()))
        })
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.source_view.is_stable()
            && file_id.is_none_or(|file_id| self.reported(file_id))
            && self.authored.unchanged_since(path, file_id, stamp)
    }

    fn reports_changes_to(&self, file_id: FileId) -> bool {
        self.source_observed() && self.reported(file_id)
    }

    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.source_view.is_stable()
            && file_id.is_none_or(|file_id| self.reported(file_id))
            && self.authored.reported_unchanged_since(path, file_id, stamp)
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.source_view.is_stable()
            && self.reported(file_id)
            && self.authored.node_unchanged_since(file_id, stamp)
    }

    fn listing_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.source_view.is_stable() && self.authored.listing_unchanged_since(path, stamp)
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.source_view.generation())
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        Ok(Box::new(self.view_lease(None)?))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.view_lease(expected_epoch)
            .map(|lease| Box::new(lease) as Box<dyn MountViewLease>)
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        Ok(self.lookup_pinned(path)?.map(|(lookup, _)| lookup))
    }

    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let path_text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            let _lease = self.source_view.read_for_callback(owner, None).await?;
            // Newly authored objects are visible before the operation barrier has
            // published them into the lazy view. This is required for NFS CREATE
            // compounds, which immediately GETATTR the returned filehandle.
            // Inspection never extends the durable observation index. A
            // caller that must later reproduce this exact content keeps the
            // returned pin instead.
            match self
                .resolve(path, &path_text, owner, SourceProof::Lookup)
                .await?
            {
                Resolution::Authored(lookup) => {
                    Ok(Some((self.project_authored(lookup).await?, None)))
                }
                Resolution::Absent => Ok(None),
                Resolution::Unauthored(lookup, source) => self
                    .project_unauthored(&path_text, lookup, source)
                    .await
                    .map(Some),
            }
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.open_proven(path).map(|(file, _)| file)
    }

    fn open_file_with_lookup(
        &self,
        path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        let (file, proven) = self.open_proven(path)?;
        let lookup = match proven {
            Some(lookup) => lookup,
            None => file.lookup()?,
        };
        Ok((file, lookup))
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lease = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored
            .detach_file(path)
            .and_then(|file| self.bind_open_file(file, None, lease.generation))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if self.authored.lookup(path)?.is_some() {
            return self.authored.read_link(path);
        }
        let path = self.path(path)?;
        if self.is_removed(&path)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| async move { self.lazy.read_link(&path).await.map_err(lazy_error) })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let mut read = Bytes::new();
        self.read_content(
            path,
            None,
            offset,
            u64::from(length),
            length,
            &mut |_, bytes| {
                read = bytes;
                Ok(())
            },
        )?;
        Ok(read)
    }

    fn read_pinned(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        self.read_content(path, Some(pin), offset, length, piece, sink)
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if self.authored.lookup(path)?.is_some() {
            return self.authored.seek(path, offset, target);
        }
        let path = self.path(path)?;
        if self.is_removed(&path)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| async move {
            self.lazy
                .seek(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => LazySeekTarget::Data,
                        MountSeekTarget::Hole => LazySeekTarget::Hole,
                    },
                )
                .await
                .map_err(lazy_error)
        })
    }

    /// A first page that a change interrupted is listed again: the change
    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let attempts = if cursor.is_some() {
            1
        } else {
            MAXIMUM_INTERRUPTED_LISTINGS
        };
        let mut page = Err(MountSourceError::Stale);
        for _ in 0..attempts {
            page = self.directory_page(path, cursor, maximum_entries);
            if !matches!(page, Err(MountSourceError::Stale)) {
                break;
            }
        }
        let mut page = page?;
        while page.entries.is_empty() {
            let Some(next) = page.next_cursor.take() else {
                break;
            };
            page = self.directory_page(path, Some(&next), maximum_entries)?;
        }
        Ok(page)
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_file(path, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn create_absent_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        absent_since: ViewStamp,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self
            .authored
            .create_absent_file(path, metadata, absent_since)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lease = self.view_lease(None)?;
        let file = self.authored.open_created(path, created)?;
        self.bind_open_file(file, Some(created.node.file_id), lease.generation)
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_directory(path, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_symbolic_link(path, target, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_special(path, kind, device, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.set_attributes(metadata, logical_bytes);
        }
        self.promote_locked(path)?;
        self.authored.set_attributes(path, metadata, logical_bytes)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.read_attribute(name);
        }
        if self.is_source_projection(path)? {
            return Ok(None);
        }
        self.authored.read_attribute(path, name)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.list_attributes(cursor, maximum_entries);
        }
        if self.is_source_projection(path)? {
            return Ok(MountAttributePage {
                names: Vec::new(),
                next_cursor: None,
            });
        }
        self.authored.list_attributes(path, cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.write_attribute(name, value, mode);
        }
        self.promote_locked(path)?;
        self.authored.write_attribute(path, name, value, mode)
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.remove_attribute(name);
        }
        self.promote_locked(path)?;
        self.authored.remove_attribute(path, name)
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.write_range(offset, bytes);
        }
        self.promote_locked(path)?;
        self.authored.write_range(path, offset, bytes)
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.resize(logical_bytes);
        }
        self.promote_locked(path)?;
        self.authored.resize(path, logical_bytes)
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.allocate_range(offset, length, operation);
        }
        self.promote_locked(path)?;
        self.authored
            .allocate_range(path, offset, length, operation)
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if self.detached_for_path(source)?.is_some()
            || self.detached_for_path(destination)?.is_some()
        {
            return Err(MountSourceError::Unsupported(
                "range cloning a detached identity requires identity-level clone support"
                    .to_owned(),
            ));
        }
        self.promote_locked(source)?;
        self.promote_locked(destination)?;
        self.authored.clone_range(
            source,
            source_offset,
            destination,
            destination_offset,
            length,
        )
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let _ = (
            source_file_id,
            source_offset,
            destination_file_id,
            destination_offset,
            length,
        );
        Err(MountSourceError::Unsupported(
            "clone-by-identity requires path-backed promotion in a lazy mount".to_owned(),
        ))
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        let text = self.path(path)?;
        let removed_id = if let Some(authored) = self.authored.lookup(path)? {
            if self.removed_identity(&text, authored.node.file_id)? {
                return Err(MountSourceError::NotFound);
            }
            if expected.is_some_and(|expected| expected != authored.node.file_id) {
                return Err(MountSourceError::Stale);
            }
            let removed_open = if self.has_open_source_handle(authored.node.file_id)? {
                let record = self.authored.record_by_id(authored.node.file_id)?;
                let file = self
                    .authored
                    .detached_file_from_record(record, authored.metadata)?;
                Some((record, authored.metadata, file))
            } else {
                None
            };
            self.authored.remove(path, expected)?;
            if let Some((record, metadata, file)) = removed_open {
                self.record_removed_identity(record, metadata, file)?;
            }
            authored.node.file_id
        } else {
            if self.is_removed(&text)? {
                return Err(MountSourceError::NotFound);
            }
            let file_id = self.wait(|| async {
                let lookup = self.lazy.lookup(&text).await.map_err(lazy_error)?;
                let actual = self
                    .lazy
                    .stable_file_id_for_lookup(&text, &lookup)
                    .await
                    .map_err(lazy_error)?;
                if expected.is_some_and(|expected| expected != actual) {
                    return Err(MountSourceError::Stale);
                }
                Ok(actual)
            })?;
            if self.has_open_source_handle(file_id)? {
                self.promote_locked(path)?;
                let record = self.authored.record_by_id(file_id)?;
                let metadata = self
                    .authored
                    .lookup(path)?
                    .ok_or(MountSourceError::Stale)?
                    .metadata;
                let file = self.authored.detached_file_from_record(record, metadata)?;
                self.authored.remove(path, Some(file_id))?;
                self.record_removed_identity(record, metadata, file)?;
            }
            file_id
        };
        self.record_removed(path, removed_id)
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        let source_text = self.path(source)?;
        let destination_text = self.path(destination)?;
        if self.authored.lookup(source)?.is_none() && self.is_removed(&source_text)? {
            return Err(MountSourceError::NotFound);
        }
        if source_text == destination_text {
            if self.authored.lookup(source)?.is_some() {
                return Ok(());
            }
            return self.wait(|| async {
                self.lazy.lookup(&source_text).await.map_err(lazy_error)?;
                Ok(())
            });
        }
        if self.authored.lookup(source)?.is_none() {
            let source_lookup =
                self.wait(|| async { self.lazy.lookup(&source_text).await.map_err(lazy_error) })?;
            if matches!(
                source_lookup,
                LazyLookup::Source(SourceNode {
                    kind: SourceNodeKind::Directory,
                    ..
                })
            ) {
                // Its entries resolve through the source by their paths, which
                // the rename changes: the subtree becomes authored first and
                // moves as one directory.
                self.promote_subtree_locked(source)?;
            }
        }
        self.promote_locked(source)?;
        let source_id = self
            .authored
            .lookup(source)?
            .ok_or(MountSourceError::Stale)?
            .node
            .file_id;
        self.promote_parents_locked(destination)?;
        if self.authored.lookup(destination)?.is_none() && !self.is_removed(&destination_text)? {
            let source_destination = self.wait(|| async {
                self.lazy
                    .lookup(&destination_text)
                    .await
                    .map_err(lazy_error)
            });
            match source_destination {
                Ok(_) if replace => self.promote_locked(destination)?,
                Ok(_) => return Err(MountSourceError::AlreadyExists),
                Err(MountSourceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let replaced_open = if replace {
            if let Some(destination_lookup) = self.authored.lookup(destination)? {
                let destination_id = destination_lookup.node.file_id;
                if destination_id != source_id && self.has_open_source_handle(destination_id)? {
                    let record = self.authored.record_by_id(destination_id)?;
                    let file = self
                        .authored
                        .detached_file_from_record(record, destination_lookup.metadata)?;
                    Some((record, destination_lookup.metadata, file))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        self.authored.rename(source, destination, replace)?;
        if let Some((record, metadata, file)) = replaced_open {
            self.record_removed_identity(record, metadata, file)?;
        }
        self.record_removed(source, source_id)?;
        self.record_rebound(&destination_text)
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(source)?;
        self.promote_parents_locked(destination)?;
        self.authored.hard_link(source, destination)?;
        self.record_rebound(&self.path(destination)?)
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        // The authored checkout's policy decides, as at its own native
        // boundary: a manual mount publishes only through sync and unmount.
        let authored = self.authored.shared_checkout();
        if !self.wait(|| async { Ok(authored.publishes_at_native_boundary().await) })? {
            return Ok(());
        }
        let _mutation = self.mutation_lease(None)?;

        self.wait(|| async {
            self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
                .await
        })
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let expected_root_identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        self.capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        match self.promote_locked(path) {
            Ok(()) | Err(MountSourceError::NotFound) => {}
            Err(error) => return Err(error),
        }
        self.authored.capture_host_subtree(source_root, path)
    }
}
