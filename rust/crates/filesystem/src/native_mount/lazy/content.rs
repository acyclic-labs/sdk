use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Records that another handle on the workspace changed each of
    pub(crate) fn changed_outside(&self, paths: &[String]) -> Result<(), MountSourceError> {
        let mut effect = ViewEffect::default();
        let mut accounting = crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            self.authored.cancellation(),
        );
        for path in paths {
            effect.subtrees.push(
                self.lazy
                    .namespace_path(path, &mut accounting)
                    .map_err(lazy_error)?
                    .value,
            );
        }
        if !effect.is_empty() {
            self.cursors.clear();
            self.authored.record_projection_change(
                &ViewChange::Effect(&effect),
                &mut adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                    WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )),
                || Ok(()),
            )?;
        }
        Ok(())
    }

    pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.capture_host(
                source_root,
                paths,
                expected_root_identity,
                adapter::HostCaptureScope::ExactPaths,
                work,
            )
        })
    }

    pub(super) fn capture_host(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: adapter::HostCaptureScope,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        if paths.is_empty() {
            return Ok(());
        }
        let _mutation = self.mutation_lease(None, Some(work))?;
        for path in paths {
            self.promote_parents_locked(path, work)?;
            match self.promote_locked(path, Some(work)) {
                Ok(()) | Err(MountSourceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |_, prepared| {
            self.runtime.wait_prepared(prepared, || {
                self.authored.capture_host(
                    source_root,
                    paths,
                    expected_root_identity,
                    scope,
                    None,
                    work,
                )
            })
        })
    }

    pub(super) fn open_proven(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), Option<MountLookup>), MountSourceError> {
        let (text, mut allocation) = self.path(path, work)?;
        let owner = ViewGate::callback_owner();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (lease, resolution, source_file) =
            with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let lease = self
                        .source_view
                        .read_for_callback_with_carrier(
                            owner,
                            None,
                            carrier,
                            work.cancellation(),
                            Some(self.authored.cancellation()),
                        )
                        .await?;
                    let (resolution, file) = self.resolve_opened(path, &text, owner, work).await?;
                    Ok((lease, resolution, file))
                })
            })?;
        let generation = lease.generation;
        let (lookup, _lookup_allocation) = match resolution {
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Authored(lookup) => {
                let file = match self.detached_by_id(lookup.node.file_id)? {
                    Some(file) => (file as Arc<dyn MountOpenFile>, 0),
                    None => self.authored.open_file_with_meter(path, work)?,
                };
                return self
                    .bind_open_file(file, Some(lookup.node.file_id), generation, work)
                    .map(|file| (file, None));
            }
            Resolution::Unauthored(lookup, allocation) => (lookup, allocation),
        };
        let id = self
            .runtime
            .wait(|| async { file_id(&self.lazy, &text, &lookup, work).await })?;
        if let Some(file) = self.detached_by_id(id)? {
            return self
                .bind_open_file((file, 0), Some(id), generation, work)
                .map(|file| (file, None));
        }
        if let LazyLookup::Source { node, source } = lookup {
            if node.kind != SourceNodeKind::RegularFile {
                return Err(MountSourceError::Invalid(
                    "open requires a regular file".into(),
                ));
            }
            let source_file = source_file.ok_or(MountSourceError::Stale)?;
            let mut owned = work
                .claim(
                    (std::mem::size_of::<LazyOpenFile<A, O, D, S>>()
                        + 2 * std::mem::size_of::<usize>()) as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )
                .map_err(MountSourceError::NativeWork)?;
            owned
                .absorb(&mut allocation)
                .map_err(MountSourceError::NativeWork)?;
            let file: Arc<dyn MountOpenFile> = Arc::new(LazyOpenFile {
                lazy: Arc::clone(&self.lazy),
                authored: Arc::clone(&self.authored),
                runtime: Arc::clone(&self.runtime),
                path: text,
                expected_source: id,
                source_file,
                source_node: node,
                source_generation: generation,
                source_view: Arc::clone(&self.source_view),
                detached: Arc::clone(&self.detached),
                open_sources: Arc::clone(&self.open_sources),
                _allocation: owned,
            });
            self.open_sources
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .entry(id)
                .or_default()
                .push(Arc::downgrade(&file));
            return Ok((
                (file, 0),
                Some(mount_lookup(LazyLookup::Source { node, source }, id)),
            ));
        }
        drop(lease);
        let _mutation = self.mutation_lease(Some(generation), Some(work))?;
        self.promote_locked(path, Some(work))?;
        self.bind_open_file(
            self.authored.open_file_with_meter(path, work)?,
            Some(id),
            generation,
            work,
        )
        .map(|file| (file, None))
    }

    pub(super) fn bind_open_file(
        &self,
        file: (Arc<dyn MountOpenFile>, u64),
        file_id: Option<FileId>,
        generation: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let allocation = work
            .claim(
                (std::mem::size_of::<ViewBoundOpenFile<A, O>>() + 2 * std::mem::size_of::<usize>())
                    as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let bound: Arc<dyn MountOpenFile> = Arc::new(ViewBoundOpenFile {
            inner: file.0,
            runtime: Arc::clone(&self.runtime),
            source_view: Arc::clone(&self.source_view),
            detached: Arc::clone(&self.detached),
            open_sources: Arc::clone(&self.open_sources),
            file_id,
            generation,
            cancellation: self.authored.cancellation().clone(),
            _allocation: allocation,
        });
        if let Some(file_id) = file_id {
            self.open_sources
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .entry(file_id)
                .or_default()
                .push(Arc::downgrade(&bound));
        }
        Ok((bound, file.1))
    }

    pub(super) fn mutation_lease(
        &self,
        expected: Option<u64>,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<ViewMutationPhase, MountSourceError> {
        let Some(work) = original else {
            return self.runtime.wait(|| async {
                self.source_view
                    .write_stable(expected)
                    .await
                    .map(|lease| lease.into_mutation_phase())
            });
        };
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                self.source_view
                    .write_stable_with_carrier(
                        expected,
                        carrier,
                        work.cancellation(),
                        Some(self.authored.cancellation()),
                    )
                    .await
                    .map(|lease| lease.into_mutation_phase())
            })
        })
    }

    pub(super) fn view_lease(
        &self,
        expected: Option<u64>,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<ViewReadLease, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let Some(work) = original else {
            return self
                .runtime
                .wait(|| async { self.source_view.read_for_callback(owner, expected).await });
        };
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                self.source_view
                    .read_for_callback_with_carrier(
                        owner,
                        expected,
                        carrier,
                        work.cancellation(),
                        Some(self.authored.cancellation()),
                    )
                    .await
            })
        })
    }

    pub(super) fn is_source_projection(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        let (text, _allocation) = self.path(path, work)?;
        self.runtime.wait(|| async {
            match self
                .resolve(path, &text, ViewGate::callback_owner(), work)
                .await?
            {
                Resolution::Absent => Err(MountSourceError::NotFound),
                Resolution::Authored(_) => Ok(false),
                Resolution::Unauthored(LazyLookup::Source { .. }, _) => Ok(true),
                Resolution::Unauthored(..) => Err(MountSourceError::Unsupported(
                    "named attributes of an unpromoted shadow are not projected".into(),
                )),
            }
        })
    }

    pub(super) fn promote_locked(
        &self,
        path: &MountPath,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError> {
        match original {
            Some(work) => self.runtime.wait(|| self.promote(path, work)),
            None => adapter::boundary(self.authored.cancellation(), |work| {
                self.promote_locked(path, Some(work))
            }),
        }
    }

    pub(super) fn read_content(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut crate::native_mount::NativeMountContentSink<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let end = offset
            .checked_add(length)
            .ok_or(MountSourceError::Invalid("content range overflows".into()))?;
        let (text, _allocation) = self.path(path, work)?;
        let owner = ViewGate::callback_owner();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (_lease, resolution, source_file) =
            with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let lease = self
                        .source_view
                        .read_for_callback_with_carrier(
                            owner,
                            None,
                            carrier,
                            work.cancellation(),
                            Some(self.authored.cancellation()),
                        )
                        .await?;
                    let (resolution, source_file) =
                        self.resolve_opened(path, &text, owner, work).await?;
                    Ok((lease, resolution, source_file))
                })
            })?;
        let (lookup, allocation) = match resolution {
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Authored(lookup) => {
                let detached = self.detached_by_id(lookup.node.file_id)?;
                return stream_pieces(offset, end, piece, sink, |offset, length| match &detached {
                    Some(file) => file.read_range_with_meter(offset, length, work),
                    None => self
                        .authored
                        .read_range_with_meter(path, offset, length, work),
                });
            }
            Resolution::Unauthored(lookup, allocation) => (lookup, allocation),
        };
        let _allocation = allocation;
        let id = self
            .runtime
            .wait(|| async { file_id(&self.lazy, &text, &lookup, work).await })?;
        if let Some(detached) = self.detached_by_id(id)? {
            return stream_pieces(offset, end, piece, sink, |offset, length| {
                detached.read_range_with_meter(offset, length, work)
            });
        }
        if let LazyLookup::Source { node, source } = &lookup
            && pin.is_some_and(|pin| pin != source_content_pin(*source, node))
        {
            return Err(MountSourceError::Stale);
        }
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        stream_pieces(offset, end, piece, sink, |offset, length| {
            if let Some(file) = &source_file {
                return crate::demand::read_file(
                    file.as_ref(),
                    offset,
                    u64::from(length),
                    work,
                    work.cancellation(),
                )
                .map_err(mount_demand_error);
            }
            self.runtime.wait(|| async {
                self.lazy
                    .read_unauthored(
                        &lookup,
                        &text,
                        Some(crate::ByteRange {
                            offset,
                            length: u64::from(length),
                        }),
                        u64::from(length),
                        &mut accounting,
                    )
                    .await
                    .map_err(lazy_error)
            })
        })
    }
}

pub(super) fn stream_pieces(
    offset: u64,
    end: u64,
    piece: u32,
    sink: &mut crate::native_mount::NativeMountContentSink<'_>,
    mut read: impl FnMut(u64, u32) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>,
) -> Result<(), MountSourceError> {
    let mut next = offset;
    while next < end {
        let wanted = u32::try_from(end - next)
            .unwrap_or(u32::MAX)
            .min(piece.max(1));
        let packet = read(next, wanted)?;
        let length = packet.bytes.len() as u64;
        if length > u64::from(wanted) {
            return Err(MountSourceError::Invalid(
                "source returned more than was asked".into(),
            ));
        }
        if length == 0 {
            break;
        }
        sink(next, packet)?;
        next = next
            .checked_add(length)
            .ok_or(MountSourceError::Invalid("content piece overflows".into()))?;
    }
    Ok(())
}

pub(super) fn source_content_pin(source: SourceReference, node: &SourceNode) -> MountContentPin {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-content-pin-v1\0");
    hasher.update(&source.identity);
    hasher.update(&node.file_identity);
    hasher.update(&node.version.0);
    MountContentPin(*hasher.finalize().as_bytes())
}
