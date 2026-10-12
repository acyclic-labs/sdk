impl<A, O> MountFilesystem for CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn native_namespace_lineage(&self) -> Option<super::NativeMountSourceLineage<'_>> {
        Some(super::NativeMountSourceLineage {
            owner: self.checkout.clone(),
            root: &self.root,
            native_original: self.native_io_original.as_ref(),
            lazy_owner: None,
            lazy_root: None,
            limits: self.limits,
            capture_gate: &self.capture_gate,
            capture_cancellation: &self.cancellation,
        })
    }

    fn native_original_work(
        &self,
    ) -> Result<Option<crate::NativeMaterializationWork>, MountSourceError> {
        self.cancellation
            .check()
            .map_err(|_| MountSourceError::Stale)?;
        Ok(self.native_original.clone())
    }

    fn native_callback_guard(&self) -> Option<Arc<dyn super::NativeMountCallbackGuard>> {
        self.native_callback_guard.clone()
    }

    fn fence_changes_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))
    }

    fn acquire_binding_lease_with_meter(
        &self,
        expected_epoch: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        self.binding_lease(expected_epoch, work)
    }

    fn capture_host_paths_with_meter(
        &self,
        root: &Path,
        paths: &[MountPath],
        identity: NativeRootIdentity,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || {
            self.capture_host(
                root,
                paths,
                identity,
                HostCaptureScope::ExactPaths,
                None,
                work,
            )
        })
    }

    fn capture_host_subtree_with_meter(
        &self,
        root: &Path,
        path: &MountPath,
        identity: NativeRootIdentity,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || {
            self.capture_host(
                root,
                std::slice::from_ref(path),
                identity,
                HostCaptureScope::Subtree,
                None,
                work,
            )
        })
    }

    fn read_pinned_with_meter(
        &self,
        _path: &MountPath,
        _pin: super::MountContentPin,
        _offset: u64,
        _length: u64,
        _piece: u32,
        _sink: &mut super::NativeMountContentSink<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))?;
        Err(MountSourceError::Stale)
    }

    fn read_directory_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        self.directory_page(path, cursor, maximum_entries, work)
    }

    fn open_file_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.open_handle(path, work).map(|(file, _)| file)
    }

    fn open_file_with_lookup_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        self.open_handle(path, work)
    }

    fn open_created_with_meter(
        &self,
        _path: &MountPath,
        created: &MountLookup,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.created_handle(created, work)
    }

    fn detach_file_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.detached_handle(path, work)
    }

    fn reported_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        self.query_history(path, work, |path, leaf| {
            self.checkout
                .ledger
                .reported_unchanged_since(path, file_id, stamp, leaf)
        })
    }

    fn listing_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        self.query_history(path, work, |path, leaf| {
            self.checkout
                .ledger
                .listing_unchanged_since(path, stamp, leaf)
        })
    }

    fn node_unchanged_since_with_meter(
        &self,
        file_id: FileId,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        carrier.check(work.cancellation(), Some(&self.cancellation))?;
        let result = self.checkout.view_gate.is_stable()
            && carrier.with_work(|leaf| {
                self.checkout
                    .ledger
                    .node_unchanged_since(file_id, stamp, leaf)
            })?;
        carrier.check(work.cancellation(), Some(&self.cancellation))?;
        Ok(result)
    }

    fn folded_path_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountPath, u64)>, MountSourceError> {
        self.fold_path(path, work)
    }

    fn shutdown_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        self.shutdown_with_cleanup_wait(work.cancellation())
    }

    fn shutdown_with_cleanup_wait(
        &self,
        wait: &CancellationToken,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.capture_retired();
        self.cancel_cleanup(Some(wait))?;
        Ok(changed)
    }

    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        let changed = !self.capture_retired();
        self.cancel_cleanup(None)?;
        Ok(changed)
    }

    fn supports_posix_named_attributes(&self) -> bool {
        self.profile == FilesystemProfile::Posix
    }

    fn flush_publishes(&self) -> bool {
        self.runtime
            .wait(|| async { Ok(self.checkout.publishes_at_native_boundary().await) })
            .unwrap_or(true)
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.checkout
            .view_gate
            .is_stable()
            .then(|| self.checkout.ledger.stamp())
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        boundary(&self.cancellation, |work| {
            self.observe_view_with_meter(observer, work)
        })
        .expect("observer registration budget");
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        boundary(&self.cancellation, |work| {
            self.unchanged_since_with_meter(path, file_id, stamp, work)
        })
        .unwrap_or(false)
    }

    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        boundary(&self.cancellation, |work| {
            self.reported_unchanged_since_with_meter(path, file_id, stamp, work)
        })
        .unwrap_or(false)
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        boundary(&self.cancellation, |work| {
            self.node_unchanged_since_with_meter(file_id, stamp, work)
        })
        .unwrap_or(false)
    }

    fn listing_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        boundary(&self.cancellation, |work| {
            self.listing_unchanged_since_with_meter(path, stamp, work)
        })
        .unwrap_or(false)
    }

    fn folded_path(&self, path: &MountPath) -> Option<MountPath> {
        boundary(&self.cancellation, |work| {
            self.folded_path_with_meter(path, work)
        })
        .ok()
        .flatten()
        .map(|(path, _)| path)
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.checkout.view_gate.generation())
    }

    fn view_is_stable(&self) -> bool {
        self.checkout.view_gate.is_stable()
    }

    fn view_is_stable_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))?;
        Ok(self.view_is_stable())
    }

    fn binding_epoch_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))?;
        Ok(self.binding_epoch())
    }

    fn view_stamp_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<ViewStamp>, MountSourceError> {
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))?;
        Ok(self.view_stamp())
    }

    fn unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        self.path_is_unchanged(path, file_id, stamp, &mut NativeDemandCarrier::Native(work))
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        self.acquire_binding_lease(None)
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.acquire_binding_lease_with_meter(expected_epoch, work)
        })
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.lookup_with_meter(path, work)
        })
    }

    fn lookup_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        invoke(&self.runtime, work, || async {
            self.lookup_async(
                path,
                ViewGate::callback_owner(),
                &mut NativeDemandCarrier::Native(work),
            )
            .await
        })
    }

    fn lookup_pinned_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<super::MountContentPin>)>, MountSourceError> {
        self.lookup_with_meter(path, work)
            .map(|lookup| lookup.map(|lookup| (lookup, None)))
    }

    fn observe_view_with_meter(
        &self,
        observer: Weak<dyn ViewObserver>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        carrier.check(work.cancellation(), Some(&self.cancellation))?;
        self.checkout.ledger.observe(observer, &mut carrier)
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.open_file_with_meter(path, work)
        })
        .map(|(file, _)| file)
    }

    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.open_created_with_meter(path, created, work)
        })
        .map(|(file, _)| file)
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.detach_file_with_meter(path, work)
        })
        .map(|(file, _)| file)
    }

    fn read_link(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let mut carrier = NativeDemandCarrier::Native(work);
        invoke(&self.runtime, work, || {
            self.read_bytes(path, None, owner, &mut carrier)
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<OwnedBytes, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.read_range_with_meter(path, offset, length, work)
        })
        .map(|chunk| chunk.bytes)
    }

    fn read_range_with_meter(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let mut carrier = NativeDemandCarrier::Native(work);
        invoke(&self.runtime, work, || {
            self.read_bytes(
                path,
                Some(ByteRange {
                    offset,
                    length: u64::from(length),
                }),
                owner,
                &mut carrier,
            )
        })
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.seek_extent(path, offset, target, work)
        })
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.read_directory_with_meter(path, cursor, maximum_entries, work)
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        _absent_since: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        invoke(&self.runtime, work, || {
            self.create(path, work, |path| AuthoredMutation::CreateFile {
                path,
                metadata,
                bytes: acyclic_stream::OwnedBytes::new(),
            })
        })
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        invoke(&self.runtime, work, || {
            self.create(path, work, |path| AuthoredMutation::CreateDirectory {
                path,
                metadata,
            })
        })
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        invoke(&self.runtime, work, || {
            self.create(path, work, |path| AuthoredMutation::CreateSymbolicLink {
                path,
                metadata,
                target: target.bytes,
            })
        })
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        let kind = match (kind, device) {
            (MountNodeKind::Fifo, None) => FileKind::Fifo,
            (MountNodeKind::Socket, None) => FileKind::Socket,
            (MountNodeKind::CharacterDevice, Some(_)) => FileKind::CharacterDevice,
            (MountNodeKind::BlockDevice, Some(_)) => FileKind::BlockDevice,
            _ => {
                return Err(MountSourceError::Unsupported(
                    "special-file kind and device identity disagree".into(),
                ));
            }
        };
        invoke(&self.runtime, work, || {
            self.create(path, work, |path| match device {
                Some((major, minor)) => AuthoredMutation::CreateDevice {
                    path,
                    kind,
                    major,
                    minor,
                    metadata,
                },
                None => AuthoredMutation::CreateEmptySpecial {
                    path,
                    kind,
                    metadata,
                },
            })
        })
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        CheckoutMountSource::set_attributes(self, path, metadata, logical_bytes, work)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        CheckoutMountSource::read_attribute(self, path, name, work)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.list_attributes_with_meter(path, cursor, maximum_entries, work)
        })
        .map(|(page, _)| page)
    }

    fn list_attributes_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        CheckoutMountSource::attribute_page(self, path, cursor, maximum_entries, work)
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        invoke(&self.runtime, work, || {
            self.change_attribute(path, name, Some((value.bytes.as_ref(), mode)), work)
        })
    }

    fn remove_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || {
            self.change_attribute(path, name, None, work)
        })
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        boundary(&self.cancellation, |work| {
            self.change_content(
                path,
                ContentChange::Write {
                    offset,
                    bytes: bytes.into(),
                },
                work,
            )
        })
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        boundary(&self.cancellation, |work| {
            self.change_content(path, ContentChange::Resize { logical_bytes }, work)
        })
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        boundary(&self.cancellation, |work| {
            self.change_content(
                path,
                range_allocation(ByteRange { offset, length }, operation),
                work,
            )
        })
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        boundary(&self.cancellation, |work| {
            self.change_content(
                destination,
                ContentChange::CloneFrom {
                    source,
                    source_offset,
                    offset: destination_offset,
                    length,
                },
                work,
            )
        })
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content(
            destination_file_id,
            ContentChange::CloneFrom {
                source: source_file_id,
                source_offset,
                offset: destination_offset,
                length,
            },
            work,
        )
    }

    fn remove(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        CheckoutMountSource::remove(self, path, expected, work)
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        CheckoutMountSource::rename(self, source, destination, replace, work)
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        CheckoutMountSource::hard_link(self, source, destination, work)
    }

    fn flush(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            carrier.check(work.cancellation(), Some(&self.cancellation))?;
            if self.checkout.publishes_at_native_boundary().await {
                self.checkout
                    .publish(
                        crate::PublicationPermit::Unrestricted,
                        work,
                        work.cancellation(),
                        Some(&self.cancellation),
                        self.native_callback_guard.clone(),
                    )
                    .await?;
            }
            carrier.check(work.cancellation(), Some(&self.cancellation))
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
        let expected_root_identity = capture_root_identity(source_root).map_err(engine_error)?;
        self.capture_host_with_identity(
            source_root,
            paths,
            expected_root_identity,
            HostCaptureScope::ExactPaths,
        )
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_identity(
            source_root,
            std::slice::from_ref(path),
            capture_root_identity(source_root).map_err(engine_error)?,
            HostCaptureScope::Subtree,
        )
    }
}
