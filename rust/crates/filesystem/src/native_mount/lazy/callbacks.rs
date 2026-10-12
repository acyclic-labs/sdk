use super::*;

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn native_namespace_lineage(&self) -> Option<crate::native_mount::NativeMountSourceLineage<'_>> {
        let mut lineage = self.authored.native_namespace_lineage()?;
        lineage.lazy_owner = Some(self.lazy.clone());
        lineage.lazy_root = Some(&self.root);
        Some(lineage)
    }

    fn native_callback_guard(&self) -> Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>> {
        self.authored.native_callback_guard()
    }

    fn native_original_work(&self) -> Result<Option<crate::NativeMaterializationWork>, MountSourceError> {
        self.authored.native_original_work()
    }

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

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.ledger_stamp().filter(|_| self.source_observed())
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        self.authored.observe_view(observer)
    }

    fn observe_view_with_meter(
        &self, observer: Weak<dyn ViewObserver>, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.authored.observe_view_with_meter(observer, work)
    }

    fn fence_changes(&self) -> Result<(), MountSourceError> {
        self.source_watch.as_ref().map_or(Ok(()), |watch| {
            crate::demand::fence_watch(&**watch, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| lazy_error(failure.error.into()))
        })
    }

    fn fence_changes_with_meter(
        &self, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.fence_changes_with_carrier(&mut crate::native_mount::adapter::NativeDemandCarrier::Native(work))
    }

    fn shutdown_with_meter(
        &self, work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel(&mut crate::native_mount::adapter::NativeDemandCarrier::Native(work))?;
        Ok(changed)
    }

    fn shutdown_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel_with_cleanup_wait(wait)?;
        Ok(changed)
    }

    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel_accepted_cleanup()?;
        Ok(changed)
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.source_view.is_stable()
            && file_id.is_none_or(|file_id| self.reported(file_id))
            && self.authored.unchanged_since(path, file_id, stamp)
    }

    fn view_is_stable_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        carrier.with_work(|work| mount_items(work, 1))?;
        Ok(self.source_view.is_stable())
    }

    fn view_stamp_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<Option<ViewStamp>, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? || !self.source_observed() { return Ok(None); }
        self.authored.view_stamp_with_meter(work)
    }

    fn binding_epoch_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<Option<u64>, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        carrier.with_work(|work| mount_items(work, 1))?;
        Ok(Some(self.source_view.generation()))
    }

    fn folded_path_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<Option<(MountPath, u64)>, MountSourceError> {
        self.authored.folded_path_with_meter(path, work)
    }

    fn unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? { return Ok(false); }
        if let Some(file_id) = file_id { if !self.reported_with_meter(file_id, work)? { return Ok(false); } }
        self.authored.unchanged_since_with_meter(path, file_id, stamp, work)
    }

    fn reported_unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? { return Ok(false); }
        if let Some(file_id) = file_id { if !self.reported_with_meter(file_id, work)? { return Ok(false); } }
        self.authored.reported_unchanged_since_with_meter(path, file_id, stamp, work)
    }

    fn node_unchanged_since_with_meter(&self, file_id: FileId, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        Ok(self.view_is_stable_with_meter(work)? && self.reported_with_meter(file_id, work)?
            && self.authored.node_unchanged_since_with_meter(file_id, stamp, work)?)
    }

    fn listing_unchanged_since_with_meter(&self, path: &MountPath, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        Ok(self.view_is_stable_with_meter(work)? && self.authored.listing_unchanged_since_with_meter(path, stamp, work)?)
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

    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        Ok(Box::new(self.view_lease(None)?))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        self.view_lease(expected_epoch)
            .map(|lease| Box::new(lease) as Box<dyn Send>)
    }

    fn acquire_binding_lease_with_meter(
        &self, expected_epoch: Option<u64>, work: &crate::NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        self.acquire_binding_lease_with_carrier(
            expected_epoch, &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work),
        )
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        Ok(self.lookup_pinned(path)?.map(|(lookup, _)| lookup))
    }

    fn lookup_with_meter(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        with_native_callback_carrier(&self.runtime,
            &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work), |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    self.lookup_pinned_in_view(path, carrier).await
                        .map(|lookup| lookup.map(|(lookup, _)| lookup))
                })
            },
        )
    }

    fn lookup_pinned(
        &self, path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        with_native_callback_carrier(&self.runtime,
            &mut crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut work), |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || self.lookup_pinned_in_view(path, carrier))
            })
    }

    fn lookup_pinned_with_meter(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        with_native_callback_carrier(&self.runtime, &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work), |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || self.lookup_pinned_in_view(path, carrier))
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.native_call(|work| self.open_file_with_meter(path, work)).map(|(file, _)| file)
    }

    fn open_file_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.open_proven(path, work).map(|(file, _)| file)
    }

    fn open_file_with_lookup(&self, path: &MountPath) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        self.native_call(|work| self.open_file_with_lookup_with_meter(path, work)).map(|((file, _), lookup)| (file, lookup))
    }

    fn open_file_with_lookup_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        let (file, proven) = self.open_proven(path, work)?;
        let lookup = match proven { Some(lookup) => lookup, None => file.0.lookup_with_meter(work)? };
        Ok((file, lookup))
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.native_call(|work| self.detach_file_with_meter(path, work)).map(|(file, _)| file)
    }

    fn detach_file_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.native_mutation(path, None, true, false, work, |detached| {
            let file = match detached {
                Some(file) => (file as Arc<dyn MountOpenFile>, 0),
                None => self.authored.detach_file_with_meter(path, work)?,
            };
            self.bind_open_file(file, None, self.source_view.generation(), work)
        })
    }

    fn read_link(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        with_native_callback_carrier(&self.runtime, &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work), |_, prepared| {
            self.runtime.wait_prepared(prepared, || self.read_link_async(path, work))
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<OwnedBytes, MountSourceError> {
        let mut read = OwnedBytes::new();
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

    fn read_range_with_meter(
        &self, path: &MountPath, offset: u64, length: u32, work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let mut read = None;
        self.read_content_with_carrier(
            path, None, offset, u64::from(length), length,
            &mut |_, output| { read = Some(output); Ok(()) },
            &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work),
        )?;
        match read {
            Some(read) => Ok(read),
            None => crate::native_mount::adapter::NativeDemandCarrier::Native(work).empty_source_chunk(),
        }
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

    fn read_pinned_with_meter(
        &self, path: &MountPath, pin: MountContentPin, offset: u64, length: u64,
        piece: u32, sink: &mut crate::native_mount::NativeMountContentSink<'_>, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.read_content_with_carrier(path, Some(pin), offset, length, piece, sink,
            &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work))
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

    fn read_directory(
        &self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        self.list_directory(path, cursor, maximum_entries,
            &mut crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut work))
    }

    fn read_directory_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        self.list_directory(
            path, cursor, maximum_entries,
            &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work),
        )
    }

    fn create_file(&self, path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_|
            self.authored.create_file(path, metadata, absent_since, work))
    }

    fn create_directory(&self, path: &MountPath, metadata: FileMetadata, work: &crate::NativeMaterializationWork)
        -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_| self.authored.create_directory(path, metadata, work))
    }

    fn create_symbolic_link(&self, path: &MountPath, target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_| self.authored.create_symbolic_link(path, target, metadata, work))
    }

    fn create_special(&self, path: &MountPath, kind: MountNodeKind, device: Option<(u32, u32)>,
        metadata: FileMetadata, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_| self.authored.create_special(path, kind, device, metadata, work))
    }

    fn hard_link(&self, source: &MountPath, destination: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<(), MountSourceError> {
        self.native_mutation(destination, Some(source), false, true, work, |_| self.authored.hard_link(source, destination, work))
    }

    fn clone_range_by_id(
        &self, _source_file_id: FileId, _source_offset: u64,
        _destination_file_id: FileId, _destination_offset: u64, _length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        Err(MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Unsupported,
            text: "clone-by-identity requires path-backed promotion in a lazy mount",
        })
    }

    fn read_attribute(&self, path: &MountPath, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.native_attributes(path, work, |file, projected| match file {
            Some(file) => file.read_attribute(name, work),
            None if projected => Ok(None),
            None => self.authored.read_attribute(path, name, work),
        })
    }

    fn list_attributes_with_meter(&self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.native_attributes(path, work, |file, projected| match file {
            Some(file) => file.list_attributes_with_meter(cursor, maximum_entries, work),
            None if projected => Ok((MountAttributePage { names: Vec::new(), next_cursor: None },
                work.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?)),
            None => self.authored.list_attributes_with_meter(path, cursor, maximum_entries, work),
        })
    }

    fn write_attribute(&self, path: &MountPath, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.native_mutation(path, None, true, false, work, |file| match file {
            Some(file) => file.write_attribute(name, value, mode, work),
            None => self.authored.write_attribute(path, name, value, mode, work),
        })
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<(), MountSourceError> {
        self.native_mutation(path, None, true, false, work, |file| match file {
            Some(file) => file.remove_attribute(name, work),
            None => self.authored.remove_attribute(path, name, work),
        })
    }

    fn open_created(&self, path: &MountPath, created: &MountLookup) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.native_call(|work| self.open_created_with_meter(path, created, work)).map(|(file, _)| file)
    }

    fn open_created_with_meter(&self, path: &MountPath, created: &MountLookup, work: &crate::NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        let owner = SourceViewGate::callback_owner();
        let lease = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || self.source_read_lease_with_carrier(owner, None, carrier))
        })?;
        let file = self.authored.open_created_with_meter(path, created, work)?;
        self.bind_open_file(file, Some(created.node.file_id), lease.generation, work)
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.set_attributes_with_carrier(
            path, metadata, logical_bytes,
            &mut crate::native_mount::adapter::NativeDemandCarrier::Native(work),
        )
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

    fn remove(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                let mut scratch = DirectoryCarrierScratch::new(carrier);
                scratch.prepare_finalization()?;
                let text = mount_path_text_with_carrier(&self.root, path, scratch.carrier)?;
                scratch.track(text.legacy_owned_bytes())?;
                let resolution = self.resolve(path, &text, owner, scratch.carrier).await?;
                let (file_id, authored) = match resolution {
                    Resolution::Absent => return Err(MountSourceError::NotFound),
                    Resolution::Authored(lookup) => (lookup.node.file_id, Some(lookup)),
                    Resolution::Unauthored(lookup, _, allocation) => {
                        let owned = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                        (self.mutation_file_id_with_carrier(&text, &owned.value, scratch.carrier).await?, None)
                    }
                };
                if expected.is_some_and(|expected| expected != file_id) { return Err(MountSourceError::Stale); }
                let opened = scratch.carrier.with_work(|work| self.has_open_source_handle_with_work(file_id, work))?;
                let (remove_authored, identity) = if opened {
                    let lookup = if let Some(authored) = authored { authored } else {
                        self.promote_with_carrier(path, owner, scratch.carrier).await?;
                        let lookup = self.authored.lookup_async(path, owner, scratch.carrier).await?
                            .ok_or(MountSourceError::Stale)?;
                        if lookup.node.file_id != file_id { return Err(MountSourceError::Stale); }
                        lookup
                    };
                    let identity = self.prepare_removed_identity_with_carrier(lookup, owner, scratch.carrier).await?;
                    (true, Some(identity))
                } else { (authored.is_some(), None) };
                let removal = self.prepare_removed_path_with_carrier(path, text, file_id, identity.as_ref(), scratch.carrier)?;
                if removal.key.native_allocation.is_none() { scratch.keep(removal.key.capacity() as u64)?; }
                scratch.reserve_final_work(removal.final_work)?;
                if let Some(identity) = &identity { scratch.reserve_final_work(identity.final_work)?; }
                if remove_authored {
                    let guarded = if authored.is_some() { expected } else { Some(file_id) };
                    self.authored.remove_async_with_carrier(path, guarded, scratch.carrier).await?;
                } else {
                    scratch.carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                }
                self.commit_removed_with_carrier(removal, identity, None, &mut scratch)
            })
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                let mut scratch = DirectoryCarrierScratch::new(carrier);
                scratch.prepare_finalization()?;
                let source_text = mount_path_text_with_carrier(&self.root, source, scratch.carrier)?;
                scratch.track(source_text.legacy_owned_bytes())?;
                let destination_text = mount_path_text_with_carrier(&self.root, destination, scratch.carrier)?;
                scratch.track(destination_text.legacy_owned_bytes())?;
                let resolved = self.resolve(source, &source_text, owner, scratch.carrier).await?;
                if matches!(resolved, Resolution::Absent) { return Err(MountSourceError::NotFound); }
                if source_text.value == destination_text.value { return Ok(()); }
                if matches!(resolved, Resolution::Unauthored(LazyLookup::Source(SourceNode {
                    kind: SourceNodeKind::Directory, ..
                }), _, _)) {
                    self.promote_subtree_with_carrier(source, owner, scratch.carrier).await?;
                }
                self.promote_with_carrier(source, owner, scratch.carrier).await?;
                let source_id = self.authored.lookup_async(source, owner, scratch.carrier).await?
                    .ok_or(MountSourceError::Stale)?.node.file_id;
                self.promote_parents_with_carrier(destination, owner, scratch.carrier).await?;
                if self.authored.lookup_async(destination, owner, scratch.carrier).await?.is_none()
                    && !scratch.carrier.with_work(|work| self.is_removed_with_work(&destination_text, work))? {
                    match self.resolve(destination, &destination_text, owner, scratch.carrier).await? {
                        Resolution::Absent => {}
                        _ if replace => self.promote_with_carrier(destination, owner, scratch.carrier).await?,
                        _ => return Err(MountSourceError::AlreadyExists),
                    }
                }
                let identity = if replace {
                    if let Some(lookup) = self.authored.lookup_async(destination, owner, scratch.carrier).await? {
                        if lookup.node.file_id != source_id
                            && scratch.carrier.with_work(|work| self.has_open_source_handle_with_work(lookup.node.file_id, work))? {
                            let identity = self.prepare_removed_identity_with_carrier(lookup, owner, scratch.carrier).await?;
                            Some(identity)
                        } else { None }
                    } else { None }
                } else { None };
                let removal = self.prepare_removed_path_with_carrier(
                    source, source_text, source_id, identity.as_ref(), scratch.carrier,
                )?;
                if removal.key.native_allocation.is_none() { scratch.keep(removal.key.capacity() as u64)?; }
                let rebound = self.prepare_rebound_with_carrier(
                    &destination_text, identity.as_ref(), &removal, scratch.carrier,
                )?;
                scratch.reserve_final_work(removal.final_work)?;
                scratch.reserve_final_work(rebound.final_work)?;
                if let Some(identity) = &identity { scratch.reserve_final_work(identity.final_work)?; }
                self.authored.rename_async_with_carrier(source, destination, replace, scratch.carrier).await?;
                self.commit_removed_with_carrier(removal, identity, Some((&destination_text, rebound)), &mut scratch)
            })
        })
    }

    fn flush(
        &self, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.flush_with_carrier(&mut crate::native_mount::adapter::NativeDemandCarrier::Native(work))
    }

    fn capture_host_paths_with_meter(
        &self, source_root: &Path, paths: &[MountPath],
        expected_root_identity: NativeRootIdentity, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_native_work(
            source_root, paths, expected_root_identity, crate::native_mount::adapter::HostCaptureScope::ExactPaths,
            work,
        )
    }

    fn capture_host_subtree_with_meter(
        &self, source_root: &Path, path: &MountPath,
        expected_root_identity: NativeRootIdentity, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_native_work(
            source_root, std::slice::from_ref(path), expected_root_identity,
            crate::native_mount::adapter::HostCaptureScope::Subtree, work,
        )
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
        let expected_root_identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        ).map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        self.capture_host_with_native_work(
            source_root, std::slice::from_ref(path), expected_root_identity,
            crate::native_mount::adapter::HostCaptureScope::Subtree, &original,
        )
    }
}
