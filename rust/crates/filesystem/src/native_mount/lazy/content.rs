use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        ).map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        self.capture_host_with_native_work(
            source_root, paths, expected_root_identity, crate::native_mount::adapter::HostCaptureScope::ExactPaths,
            &original,
        )
    }

pub(in crate::native_mount::lazy) fn open_proven(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), Option<MountLookup>), MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        let path_text = mount_path_text_with_carrier(&self.root, path, &mut carrier)?;
        let owner = SourceViewGate::callback_owner();
        let (lease, resolution, source_file) = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
                let (resolution, source_file) = self.resolve_opened_with_carrier(
                    path, &path_text, owner, carrier,
                ).await?;
                Ok((lease, resolution, source_file))
            })
        })?;
        let generation = lease.generation;
        let lookup = match resolution {
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Authored(lookup) => {
                let detached = carrier.with_work(|work| self.detached_by_id(lookup.node.file_id, Some(work)))?;
                let file = match detached {
                    Some(file) => (file as Arc<dyn MountOpenFile>, 0),
                    None => self.authored.open_file_with_meter(path, work)?,
                };
                return self.bind_open_file(file, Some(lookup.node.file_id), generation, work)
                    .map(|file| (file, None));
            }
            Resolution::Unauthored(lookup, source, allocation) => {
                let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                match &lookup.value {
                    LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => {
                        let source_file = source.and(source_file).ok_or(MountSourceError::Stale)?;
                        let expected_source = self.lazy.source_file_id(&node.file_identity,
                            &mut crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())).map_err(lazy_error)?;
                        let (mount_path, mut mount_allocation, _) = mount_components_with_carrier(
                            path.components(), None, &mut carrier,
                        )?.into_parts();
                        let MountOwnedBuffer { value: path, allocation: mut input_allocation, .. } = path_text;
                        if let (Some(input), Some(mount)) = (&mut input_allocation, &mut mount_allocation) {
                            input.absorb(mount).map_err(MountSourceError::NativeWork)?;
                        }
                        let bytes = std::mem::size_of::<LazyOpenFile<A, O, D, S>>() as u64;
                        carrier.with_work(|work| work.allocate(bytes)).map_err(mount_demand_error)?;
                        let file: Arc<dyn MountOpenFile> = Arc::new(LazyOpenFile {
                            lazy: Arc::clone(&self.lazy), authored: Arc::clone(&self.authored), runtime: Arc::clone(&self.runtime),
                            path, mount_path, expected_source, source_file, source_node: *node, source_generation: generation,
                            source_view: Arc::clone(&self.source_view), detached: Arc::clone(&self.detached),
                            open_sources: Arc::clone(&self.open_sources),
                            _input_allocation: input_allocation,
                        });
                        self.open_sources.lock().map_err(|_| MountSourceError::Stale)?
                            .entry(expected_source).or_default().push(Arc::downgrade(&file));
                        return Ok(((file, bytes), Some(mount_lookup(&LazyLookup::Source(*node), expected_source))));
                    }
                    LazyLookup::Source(_) => return Err(MountSourceError::Invalid("open requires a regular file".to_owned())),
                    LazyLookup::Authored { stat, .. } => stat.file_id,
                    LazyLookup::Shadow { record, .. } => record.file_id,
                }
            }
        };
        let detached = carrier.with_work(|work| self.detached_by_id(lookup, Some(work)))?;
        if let Some(file) = detached {
            return self.bind_open_file((file, 0), Some(lookup), generation, work).map(|file| (file, None));
        }
        drop(lease);
        let mutation = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_mutation_lease_with_carrier(Some(generation), carrier).await?;
                self.promote_with_carrier(path, owner, carrier).await?;
                Ok(lease)
            })
        })?;
        let file = self.authored.open_file_with_meter(path, work)?;
        let result = self.bind_open_file(file, Some(lookup), generation, work).map(|file| (file, None));
        drop(mutation);
        result
    }

pub(in crate::native_mount::lazy) fn bind_open_file(
        &self, file: (Arc<dyn MountOpenFile>, u64), file_id: Option<FileId>, generation: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let bytes = std::mem::size_of::<ViewBoundOpenFile<A, O>>() as u64;
        work.native_read_meter().lock().allocate(bytes).map_err(mount_demand_error)?;
        let bound: Arc<dyn MountOpenFile> = Arc::new(ViewBoundOpenFile {
            inner: file.0, runtime: Arc::clone(&self.runtime), source_view: Arc::clone(&self.source_view),
            source_lifetime: self.authored.cancellation().clone(), detached: Arc::clone(&self.detached),
            open_sources: Arc::clone(&self.open_sources), file_id, generation,
        });
        if let Some(file_id) = file_id {
            self.open_sources.lock().map_err(|_| MountSourceError::Stale)?
                .entry(file_id).or_default().push(Arc::downgrade(&bound));
        }
        Ok((bound, bytes.checked_add(file.1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?))
    }

pub(in crate::native_mount::lazy) fn mutation_lease(
        &self,
        expected: Option<u64>,
    ) -> Result<SourceMutationLease, MountSourceError> {
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(expected).await }
        })
    }

pub(in crate::native_mount::lazy) fn view_lease(&self, expected: Option<u64>) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.read_for_callback(owner, expected).await }
        })
    }

pub(in crate::native_mount::lazy) fn read_content(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        );
        self.read_content_with_carrier(path, pin, offset, length, piece,
            &mut |offset, output| sink(offset, output.bytes),
            &mut crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut work))
    }

pub(in crate::native_mount::lazy) fn read_content_with_carrier(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut crate::native_mount::NativeMountContentSink<'_>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let _lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
                let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
                let result = self.read_resolved_content_with_carrier(
                    path, &text, owner, pin, offset, length, piece, sink, carrier,
                ).await;
                let retained = text.legacy_owned_bytes();
                drop(text);
                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                result
            })
        })
    }

pub(in crate::native_mount::lazy) async fn content_detached_with_carrier(
        &self,
        resolution: &Resolution,
        text: &str,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        if self.detached.lock().map_err(|_| MountSourceError::Stale)?.is_empty() {
            return Ok(None);
        }
        let file_id = match resolution {
            Resolution::Absent => return Ok(None),
            Resolution::Authored(lookup) => lookup.node.file_id,
            Resolution::Unauthored(lookup, _, _) =>
                file_id(&self.lazy, text, lookup, carrier).await?,
        };
        carrier.with_work(|work| self.detached_by_id(file_id, Some(work)))
    }

pub(in crate::native_mount::lazy) async fn open_resolved_source_with_carrier(
        &self,
        text: &str,
        resolution: &Resolution,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<Box<dyn DemandFile>>, MountSourceError> {
        let Resolution::Unauthored(LazyLookup::Source(node), Some(source), _) = resolution else {
            return Ok(None);
        };
        if node.kind != SourceNodeKind::RegularFile { return Ok(None); }
        let caller = carrier.cancellation();
        if let Some(original) = carrier.original_native_work() {
            return match self.lazy.open_source_file_with_native_work(
                text, *source, *node, &original, &caller,
            ).await {
                Ok((file, request)) => {
                    drop(request);
                    Ok(Some(file))
                }
                Err((error, request)) => {
                    let cause = native_lazy_error(error);
                    drop(request);
                    Err(cause)
                }
            };
        }
        let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
        let opened = self.lazy.open_source_file_measured(text, *source, *node, budget, &caller).await;
        let file = carrier.with_work(|work| mount_accept(work, opened, lazy_error))?;
        let retained = file.logical_owned_bytes();
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(file);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        Ok(Some(file))
    }

pub(in crate::native_mount::lazy) async fn resolve_opened_with_carrier(
        &self,
        path: &MountPath,
        text: &str,
        owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(Resolution, Option<Box<dyn DemandFile>>), MountSourceError> {
        let resolution = self.resolve(path, text, owner, carrier).await?;
        let retained = match &resolution {
            Resolution::Unauthored(lookup, _, allocation) =>
                if allocation.is_none() { lookup.logical_owned_bytes() } else { 0 },
            Resolution::Authored(_) | Resolution::Absent => 0,
        };
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(resolution);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        match self.open_resolved_source_with_carrier(text, &resolution, carrier).await {
            Ok(file) => Ok((resolution, file)),
            Err(error) => {
                drop(resolution);
                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                Err(error)
            }
        }
    }

pub(in crate::native_mount::lazy) async fn read_resolved_content_with_carrier(
        &self,
        path: &MountPath,
        text: &str,
        owner: crate::native_mount::view_gate::CallbackOwner,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut crate::native_mount::NativeMountContentSink<'_>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let resolution = self.resolve(path, text, owner, carrier).await?;
        let lookup_bytes = match &resolution {
            Resolution::Unauthored(lookup, _, allocation) =>
                if allocation.is_none() { lookup.logical_owned_bytes() } else { 0 },
            Resolution::Authored(_) | Resolution::Absent => 0,
        };
        if let Err(error) = carrier.with_work(|work| work.record_adoption(lookup_bytes, &None)) {
            drop(resolution);
            carrier.with_work(|work| work.release(lookup_bytes)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        let result = async {
            if matches!(resolution, Resolution::Absent) { return Err(MountSourceError::NotFound); }
            let detached = self.content_detached_with_carrier(&resolution, text, carrier).await?;
            if detached.is_none()
                && let Resolution::Unauthored(LazyLookup::Source(node), Some(source), _) = &resolution
                && let Some(pin) = pin
                && pin != carrier.with_work(|work| source_content_pin(*source, node, work))?
            {
                return Err(MountSourceError::Stale);
            }
            let source_file = if detached.is_none() {
                self.open_resolved_source_with_carrier(text, &resolution, carrier).await?
            } else { None };
            let result = async {
                let Some(end) = offset.checked_add(length) else {
                    return Err(mount_content_invalid_with_carrier(carrier, "content range overflows")?);
                };
                let mut next = offset;
                while next < end {
                    carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                    self.authored.cancellation().check().map_err(|_| MountSourceError::Cancelled)?;
                    let wanted = u32::try_from(end - next).unwrap_or(u32::MAX).min(piece.max(1));
                    let output = if let Some(file) = &detached {
                        file.read_range_owned_async_with_carrier(next, wanted, carrier).await?
                    } else if let Some(file) = &source_file {
                        let caller = carrier.cancellation();
                        if let Some(original) = carrier.original_native_work() {
                            crate::demand::read_file(file.as_ref(), next, u64::from(wanted), &original, &caller).map_err(mount_demand_error)?
                        } else {
                            let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                            let read = crate::demand::read_file(file.as_ref(), next, u64::from(wanted), budget, &caller);
                            let output = carrier.with_work(|work| mount_accept(work, read, mount_demand_error))?;
                            let retained = output.logical_owned_bytes;
                            if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
                                drop(output);
                                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                                return Err(mount_demand_error(error));
                            }
                            output
                        }
                    } else {
                        match &resolution {
                            Resolution::Authored(_) => self.authored.read_bytes(path, Some(crate::ByteRange { offset: next, length: u64::from(wanted) }), owner, carrier).await?,
                            Resolution::Unauthored(lookup, source, _) => {
                                let caller = carrier.cancellation();
                                if let Some(original) = carrier.original_native_work() {
                                    let read = self.lazy.read_unauthored(
                                        lookup, *source, text, Some(crate::ByteRange { offset: next, length: u64::from(wanted) }),
                                        u64::MAX, &LazyTreapWork::native_with_work(&original, &caller).map_err(native_lazy_error)?,
                                    ).await;
                                    accept_native_content(read)?
                                } else {
                                    let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                                    let accounting = LazyTreapWork::new(budget, &caller);
                                    let read = self.lazy.read_unauthored(lookup, *source, text,
                                        Some(crate::ByteRange { offset: next, length: u64::from(wanted) }),
                                        u64::MAX, &accounting).await.map_err(|(error, _)| error);
                                    let read = accounting.measured(read, |error, work| error.with_prior_work(work).into_measured_failure());
                                    let output = carrier.with_work(|work| mount_accept(work, read, lazy_error))?;
                                    let retained = output.logical_owned_bytes;
                                    if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
                                        drop(output);
                                        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                                        return Err(mount_demand_error(error));
                                    }
                                    output
                                }
                            }
                            Resolution::Absent => return Err(MountSourceError::NotFound),
                        }
                    };
                    let read_length = output.bytes.len() as u64;
                    if read_length == 0 || read_length > u64::from(wanted) {
                        let retained = if output.is_intrinsic() { 0 } else { output.logical_owned_bytes };
                        drop(output);
                        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                        if read_length == 0 { break; }
                        return Err(mount_content_invalid_with_carrier(carrier, "source returned more than was asked")?);
                    }
                    sink(next, output)?;
                    next += read_length;
                    if read_length < u64::from(wanted) { break; }
                }
                Ok(())
            }.await;
            if let Some(file) = source_file {
                let retained = if carrier.original_native_work().is_some() { 0 } else { file.logical_owned_bytes() };
                drop(file);
                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            }
            result
        }.await;
        drop(resolution);
        carrier.with_work(|work| work.release(lookup_bytes)).map_err(mount_demand_error)?;
        result
    }

pub(in crate::native_mount::lazy) async fn read_link_async(
        &self,
        path: &MountPath,
        original: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(original);
        let carrier = &mut carrier;
        let owner = SourceViewGate::callback_owner();
        let _lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
        let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
        match self.resolve(path, &text, owner, carrier).await? {
            Resolution::Absent => Err(MountSourceError::NotFound),
            Resolution::Authored(_) => self.authored.read_bytes(path, None, owner, carrier).await,
            Resolution::Unauthored(lookup, source, allocation) => {
                let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                let maximum = original.remaining().map_err(MountSourceError::NativeWork)?.output_bytes;
                accept_native_content(self.lazy.read_unauthored(
                    &lookup.value, source, &text, None, maximum,
                    &LazyTreapWork::native_with_work(original, original.cancellation()).map_err(native_lazy_error)?,
                ).await)
            }
        }
    }
}

pub(in crate::native_mount::lazy) fn source_content_pin(
    source: SourceReference,
    node: &SourceNode,
    work: &mut crate::demand::DemandWork,
) -> Result<MountContentPin, MountSourceError> {
    work.charge(crate::WorkCounters {
        bytes_hashed: (b"acyclic-fs-lazy-content-pin-v1\0".len()
            + source.identity.len() + node.file_identity.len() + node.version.0.len()) as u64,
        ..crate::WorkCounters::default()
    }).map_err(mount_demand_error)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-content-pin-v1\0");
    hasher.update(&source.identity);
    hasher.update(&node.file_identity);
    hasher.update(&node.version.0);
    Ok(MountContentPin(*hasher.finalize().as_bytes()))
}
