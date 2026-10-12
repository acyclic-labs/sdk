use super::*;

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn native_namespace_lineage(
        &self,
    ) -> Option<crate::native_mount::NativeMountSourceLineage<'_>> {
        let mut lineage = self.authored.native_namespace_lineage()?;
        lineage.lazy_owner = Some(self.lazy.clone());
        lineage.lazy_root = Some(&self.root);
        Some(lineage)
    }

    fn native_callback_guard(
        &self,
    ) -> Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>> {
        self.authored.native_callback_guard()
    }

    fn native_original_work(
        &self,
    ) -> Result<Option<crate::NativeMaterializationWork>, MountSourceError> {
        self.authored.native_original_work()
    }

    fn observe_view_with_meter(
        &self,
        observer: Weak<dyn ViewObserver>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.authored.observe_view_with_meter(observer, work)
    }

    fn view_is_stable_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        carrier.with_work(|work| mount_items(work, 1))?;
        Ok(self.source_view.is_stable())
    }

    fn view_stamp_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<ViewStamp>, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? || !self.source_observed() {
            return Ok(None);
        }
        self.authored.view_stamp_with_meter(work)
    }

    fn binding_epoch_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        carrier.with_work(|work| mount_items(work, 1))?;
        Ok(Some(self.source_view.generation()))
    }

    fn unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? {
            return Ok(false);
        }
        if let Some(file_id) = file_id {
            if !self.reported(file_id) {
                return Ok(false);
            }
        }
        self.authored
            .unchanged_since_with_meter(path, file_id, stamp, work)
    }

    fn reported_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? {
            return Ok(false);
        }
        if let Some(file_id) = file_id {
            if !self.reported(file_id) {
                return Ok(false);
            }
        }
        self.authored
            .reported_unchanged_since_with_meter(path, file_id, stamp, work)
    }

    fn node_unchanged_since_with_meter(
        &self,
        file_id: FileId,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        Ok(self.view_is_stable_with_meter(work)?
            && self.reported(file_id)
            && self
                .authored
                .node_unchanged_since_with_meter(file_id, stamp, work)?)
    }

    fn listing_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        Ok(self.view_is_stable_with_meter(work)?
            && self
                .authored
                .listing_unchanged_since_with_meter(path, stamp, work)?)
    }
    fn fence_changes_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.source_watch
            .as_ref()
            .and_then(|owner| owner.watch.as_deref())
            .map_or(Ok(()), |watch| {
                crate::demand::fence_watch(watch, work, work.cancellation())
                    .map_err(mount_demand_error)
            })
    }
    fn shutdown_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        let changed = !self.physically_retired()?;
        self.cancel_cleanup(Some(work.cancellation()))?;
        Ok(changed)
    }
    fn shutdown_with_cleanup_wait(
        &self,
        wait: &CancellationToken,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel_cleanup(Some(wait))?;
        Ok(changed)
    }
    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel_cleanup(None)?;
        Ok(changed)
    }

    super::mutations::mutation_callbacks!();

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
        if !self.source_view.is_stable() || !self.source_observed() {
            return None;
        }
        self.authored.view_stamp()
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        self.authored.observe_view(observer);
    }

    fn fence_changes(&self) -> Result<(), MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.fence_changes_with_meter(work)
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

    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        Ok(Box::new(self.view_lease(None, None)?))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        self.view_lease(expected_epoch, None)
            .map(|lease| Box::new(lease) as Box<dyn Send>)
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        Ok(self.lookup_pinned(path)?.map(|(lookup, _)| lookup))
    }

    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.lookup_pinned_with_meter(path, work)
        })
    }

    fn lookup_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        self.lookup_pinned_with_meter(path, work)
            .map(|lookup| lookup.map(|(lookup, _)| lookup))
    }

    fn lookup_pinned_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let (text, _allocation) = self.path(path, Some(work))?;
        let owner = ViewGate::callback_owner();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _lease = self
                    .source_view
                    .read_for_callback_with_carrier(
                        owner,
                        None,
                        carrier,
                        work.cancellation(),
                        Some(self.authored.cancellation()),
                    )
                    .await?;
                match self.resolve(path, &text, owner, Some(work)).await? {
                    Resolution::Authored(lookup) => Ok(Some((
                        self.project_authored(lookup, Some(work)).await?,
                        None,
                    ))),
                    Resolution::Absent => Ok(None),
                    Resolution::Unauthored(lookup, _allocation) => self
                        .project_unauthored(&text, lookup, Some(work))
                        .await
                        .map(Some),
                }
            })
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.open_file_with_meter(path, work).map(|(file, _)| file)
        })
    }
    fn open_file_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.open_proven(path, work).map(|(file, _)| file)
    }

    fn open_file_with_lookup(
        &self,
        path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.open_file_with_lookup_with_meter(path, work)
                .map(|((file, _), lookup)| (file, lookup))
        })
    }
    fn open_file_with_lookup_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        let (file, proven) = self.open_proven(path, work)?;
        let lookup = match proven {
            Some(lookup) => lookup,
            None => file.0.lookup_with_meter(work)?,
        };
        Ok((file, lookup))
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.detach_file_with_meter(path, work)
                .map(|(file, _)| file)
        })
    }
    fn detach_file_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let lease = self.mutation_lease(None, Some(work))?;
        self.promote_locked(path, Some(work))?;
        let file = self.authored.detach_file_with_meter(path, work)?;
        self.bind_open_file(file, None, lease.generation, work)
    }

    fn read_link(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let (text, _allocation) = self.path(path, Some(work))?;
        let owner = ViewGate::callback_owner();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _lease = self
                    .source_view
                    .read_for_callback_with_carrier(
                        owner,
                        None,
                        carrier,
                        work.cancellation(),
                        Some(self.authored.cancellation()),
                    )
                    .await?;
                match self.resolve(path, &text, owner, Some(work)).await? {
                    Resolution::Absent => Err(MountSourceError::NotFound),
                    Resolution::Authored(_) => self.authored.read_link(path, work),
                    Resolution::Unauthored(lookup, _allocation) => {
                        let mut accounting =
                            crate::kernel::KernelAccounting::native(work, work.cancellation());
                        self.lazy
                            .read_resolved(&lookup, &text, None, u64::MAX, crate::model::GenerationSelector::Head, &mut accounting)
                            .await
                            .map_err(lazy_error)
                    }
                }
            })
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<OwnedBytes, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.read_range_with_meter(path, offset, length, work)
                .map(|packet| packet.bytes)
        })
    }

    fn read_range_with_meter(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let mut read = None;
        self.read_content(
            path,
            None,
            offset,
            u64::from(length),
            length,
            &mut |_, packet| {
                read = Some(packet);
                Ok(())
            },
            work,
        )?;
        match read {
            Some(packet) => Ok(packet),
            None => Ok(crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::new(), 0)),
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
        adapter::boundary(self.authored.cancellation(), |work| {
            self.read_content(
                path,
                Some(pin),
                offset,
                length,
                piece,
                &mut |offset, packet| sink(offset, packet.bytes),
                work,
            )
        })
    }

    fn read_pinned_with_meter(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut crate::native_mount::NativeMountContentSink<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.read_content(path, Some(pin), offset, length, piece, sink, work)
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        self.open_file(path)?.seek(offset, target)
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.read_directory_with_meter(path, cursor, maximum_entries, work)
        })
    }

    fn read_directory_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let attempts = if cursor.is_some() {
            1
        } else {
            MAXIMUM_INTERRUPTED_LISTINGS
        };
        let mut page = Err(MountSourceError::Stale);
        for _ in 0..attempts {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
            page = self.directory_page(path, cursor, maximum_entries, work);
            if !matches!(page, Err(MountSourceError::Stale)) {
                break;
            }
        }
        let mut page = page?;
        while page.entries.is_empty() {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
            let Some(next) = page.next_cursor.take() else {
                break;
            };
            page = self.directory_page(path, Some(&next), maximum_entries, work)?;
        }
        Ok(page)
    }

    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.open_created_with_meter(path, created, work)
                .map(|(file, _)| file)
        })
    }
    fn open_created_with_meter(
        &self,
        path: &MountPath,
        created: &MountLookup,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let lease = self.view_lease(None, Some(work))?;
        self.bind_open_file(
            self.authored.open_created_with_meter(path, created, work)?,
            Some(created.node.file_id),
            lease.generation,
            work,
        )
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        let _lease = self.view_lease(None, Some(work))?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.read_attribute(name, work);
        }
        if self.is_source_projection(path, Some(work))? {
            return Ok(None);
        }
        self.authored.read_attribute(path, name, work)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.list_attributes_with_meter(path, cursor, maximum_entries, work)
                .map(|(page, _)| page)
        })
    }

    fn list_attributes_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        let _lease = self.view_lease(None, Some(work))?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.list_attributes_with_meter(cursor, maximum_entries, work);
        }
        if self.is_source_projection(path, Some(work))? {
            return Ok((
                MountAttributePage {
                    names: Vec::new(),
                    next_cursor: None,
                },
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork)?,
            ));
        }
        self.authored
            .list_attributes_with_meter(path, cursor, maximum_entries, work)
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
            .map_err(|error| MountSourceError::Engine(error.to_string().into()))?;
        self.capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(error.to_string().into()))?;
        adapter::boundary(self.authored.cancellation(), |work| {
            self.capture_host_subtree_with_meter(source_root, path, identity, work)
        })
    }

    fn capture_host_paths_with_meter(
        &self,
        root: &Path,
        paths: &[MountPath],
        identity: NativeRootIdentity,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host(
            root,
            paths,
            identity,
            adapter::HostCaptureScope::ExactPaths,
            work,
        )
    }

    fn capture_host_subtree_with_meter(
        &self,
        root: &Path,
        path: &MountPath,
        identity: NativeRootIdentity,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host(
            root,
            std::slice::from_ref(path),
            identity,
            adapter::HostCaptureScope::Subtree,
            work,
        )
    }

    fn folded_path_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountPath, u64)>, MountSourceError> {
        self.authored.folded_path_with_meter(path, work)
    }

    fn acquire_binding_lease_with_meter(
        &self,
        expected: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        let lease = self.view_lease(expected, Some(work))?;
        let allocation = work
            .claim(
                std::mem::size_of::<(ViewReadLease, crate::NativeAllocation)>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        Ok(Box::new((lease, allocation)))
    }
}
