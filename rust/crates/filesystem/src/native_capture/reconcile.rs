use super::*;

pub(super) async fn capture_observed_paths_from_root<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    observed: (Vec<Option<HostStat>>, Vec<NamespacePath>),
    batch_size: usize,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    baseline: Option<&NativeViewBaseline>,
    meter: &NativeMaterializationWork,
) -> Result<CaptureReceipt, CaptureError> {
    let states = lookup_capture_states(checkout, observed, batch_size, meter).await?;
    let bytes = vector_bytes(&states);
    let count = states.len();
    let mut batch = CaptureBatch {
        source_root,
        watch_epoch: None,
        baseline,
        maximum_extent_spans,
        meter,
        host_links: Vec::new(),
        mutations: Vec::new(),
        receipt: CaptureReceipt::default(),
    };
    for (index, (observation, path, current)) in states.into_iter().enumerate() {
        batch
            .prepare(
                checkout,
                (observation, path, current),
                CaptureIntent::Complete,
            )
            .await?;
        if (index + 1) % batch_size == 0 && index + 1 < count {
            apply_capture_transaction(checkout, std::mem::take(&mut batch.mutations), meter)
                .await?;
        }
    }
    meter.clone().release(bytes).map_err(capture_demand_error)?;
    apply_capture_transaction(checkout, batch.mutations, meter).await?;
    release_host_links(batch.host_links, meter)?;
    meter.clone().check().map_err(capture_demand_error)?;
    Ok(batch.receipt)
}

pub(super) type CapturePathState = (Option<HostStat>, NamespacePath, Option<FileRecord>);

pub(super) async fn lookup_capture_states<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    (observations, paths): (Vec<Option<HostStat>>, Vec<NamespacePath>),
    page_size: usize,
    meter: &NativeMaterializationWork,
) -> Result<Vec<CapturePathState>, CaptureError> {
    let input_bytes = observations.capacity() as u64
        * std::mem::size_of::<Option<HostStat>>() as u64
        + paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    let mut states = capture_vec(paths.len(), meter)?;
    let mut paths = paths.into_iter();
    let mut observations = observations.into_iter();
    let cancellation = meter.cancellation().clone();
    while paths.len() != 0 {
        let count = paths.len().min(page_size);
        let page = paths.as_slice().get(..count).ok_or(WorkError::Overflow)?;
        let lookup = checkout
            .lookup_batch_no_follow(page, meter, &cancellation)
            .await?;
        for ((path, observation), entry) in paths
            .by_ref()
            .take(count)
            .zip(observations.by_ref())
            .zip(lookup.0.entries)
        {
            meter.clone().check().map_err(capture_demand_error)?;
            states.push((observation, path, entry.record));
        }
        drop(lookup.1);
    }
    drop(paths);
    drop(observations);
    meter
        .clone()
        .release(input_bytes)
        .map_err(capture_demand_error)?;
    sort_capture_states(&mut states, meter)?;
    Ok(states)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum CaptureIntent {
    MetadataOnly,
    Complete,
    /// As `Complete`, for a name a rename bound: a directory's subtree is new.
    Arrived,
    Replace,
}

pub(super) struct CaptureBatch<'a> {
    pub(super) source_root: &'a HostRoot,
    pub(super) watch_epoch: Option<u64>,
    pub(super) baseline: Option<&'a NativeViewBaseline>,
    pub(super) maximum_extent_spans: u32,
    pub(super) meter: &'a NativeMaterializationWork,
    pub(super) host_links: Vec<([u8; 16], NamespacePath)>,
    pub(super) mutations: Vec<AuthoredMutation>,
    pub(super) receipt: CaptureReceipt,
}

impl CaptureBatch<'_> {
    pub(super) async fn prepare<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self,
        checkout: &mut Checkout<A, O>,
        (observation, path, current): CapturePathState,
        intent: CaptureIntent,
    ) -> Result<(), CaptureError> {
        self.meter.cancellation().check()?;
        let host_path = namespace_to_host_path_measured(&path, self.meter)?;
        let observation = match observation {
            Some(observation) => Some(observation),
            None => observe_host_path(self.source_root, &host_path, self.meter)?,
        };
        if let Some(metadata) = observation {
            return self
                .present(checkout, path, current, intent, host_path, metadata)
                .await;
        }
        if let Some(record) = current {
            // A vanished directory requires descendant removals before removing the directory itself.
            if record.kind == FileKind::Directory {
                push_subtree_removals(
                    checkout,
                    &path,
                    &mut self.mutations,
                    &mut self.receipt,
                    self.meter,
                )
                .await?;
            }
            capture_reserve(&mut self.mutations, 1, self.meter)?;
            self.mutations.push(AuthoredMutation::Remove {
                path,
                expected_file_id: Some(record.file_id),
            });
            self.receipt.changed_paths = checked_increment(self.receipt.changed_paths)?;
        } else {
            capture_drop_path(path, self.meter)?;
        }

        capture_drop_host_path(host_path, self.meter)?;
        self.receipt.examined_paths = checked_increment(self.receipt.examined_paths)?;
        Ok(())
    }

    fn link_position(&self, metadata: &HostStat) -> Result<Option<usize>, CaptureError> {
        let identity = metadata.identity().to_bytes();
        for (index, (node, _)) in self.host_links.iter().enumerate() {
            capture_charge(
                self.meter,
                crate::materializer::NativeResourceCredit::Items.delta(1),
            )?;
            if *node == identity {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    async fn present<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self,
        checkout: &mut Checkout<A, O>,
        path: NamespacePath,
        current: Option<FileRecord>,
        intent: CaptureIntent,
        host_path: CaptureHostPath,
        metadata: HostStat,
    ) -> Result<(), CaptureError> {
        let host_kind = host_kind(&metadata)?;
        let linked_regular = host_kind == FileKind::Regular
            && checkout.volume_config().hard_links
            && metadata.nlink() > 1;
        if linked_regular {
            if let Some(index) = self.link_position(&metadata)? {
                return self.hard_link(index, path, current, host_path, &metadata);
            }
            if let Some(epoch) = self.watch_epoch {
                // An existing alias may lie outside this hint batch. Rebaseline instead of recording a second FileId.
                return Err(CaptureError::RescanRequired {
                    epoch,
                    reason: WatchInvalidationReason::NativeRescanRequired,
                });
            }
        }
        // Created files replace identity; same-kind directories retain identity while reconciling children.
        let replace = intent == CaptureIntent::Replace
            && !(host_kind == FileKind::Directory
                && current.is_some_and(|record| record.kind == FileKind::Directory));
        if let Some(record) = current
            && (record.kind != host_kind || replace)
        {
            if record.kind == FileKind::Directory {
                push_subtree_removals(
                    checkout,
                    &path,
                    &mut self.mutations,
                    &mut self.receipt,
                    self.meter,
                )
                .await?;
            }
            capture_reserve(&mut self.mutations, 1, self.meter)?;
            self.mutations.push(AuthoredMutation::Remove {
                path: clone_capture_path(&path, self.meter)?,
                expected_file_id: Some(record.file_id),
            });
        }
        let exists_with_kind = !replace && current.is_some_and(|record| record.kind == host_kind);
        let (canonical_metadata, prior_metadata) = self
            .canonical_metadata(checkout, current, host_kind, exists_with_kind, &metadata)
            .await?;
        // A directory's timestamps follow what happens to its entries, each captured through its own path, and to reads of it: a change to them alone is no change of the directory's.
        if host_kind == FileKind::Directory
            && prior_metadata.is_some_and(|prior| {
                prior.without_timestamps() == canonical_metadata.without_timestamps()
            })
        {
            self.receipt.examined_paths = checked_increment(self.receipt.examined_paths)?;
            capture_drop_path(path, self.meter)?;
            capture_drop_host_path(host_path, self.meter)?;
            return Ok(());
        }
        let linked_path = if linked_regular {
            Some(clone_capture_path(&path, self.meter)?)
        } else {
            None
        };
        let changed = if intent == CaptureIntent::MetadataOnly && exists_with_kind {
            ensure_current_host_node(self.source_root, &host_path, &metadata, self.meter)?;
            capture_reserve(&mut self.mutations, 1, self.meter)?;
            self.mutations.push(AuthoredMutation::SetMetadata {
                path,
                metadata: canonical_metadata,
            });
            capture_drop_host_path(host_path, self.meter)?;
            true
        } else if host_kind == FileKind::Regular {
            self.regular(
                checkout,
                path,
                host_path,
                &metadata,
                canonical_metadata,
                (
                    current.filter(|_| exists_with_kind),
                    !linked_regular && prior_metadata == Some(canonical_metadata),
                ),
            )
            .await?
        } else {
            self.special(
                path,
                host_path,
                &metadata,
                host_kind,
                canonical_metadata,
                exists_with_kind,
            )?;
            true
        };
        if changed {
            self.receipt.changed_paths = checked_increment(self.receipt.changed_paths)?;
        }
        if let Some(linked_path) = linked_path {
            capture_reserve(&mut self.host_links, 1, self.meter)?;
            self.host_links
                .push((metadata.identity().to_bytes(), linked_path));
        }
        self.receipt.examined_paths = checked_increment(self.receipt.examined_paths)?;
        Ok(())
    }

    fn hard_link(
        &mut self,
        index: usize,
        path: NamespacePath,
        current: Option<FileRecord>,
        host_path: CaptureHostPath,
        metadata: &HostStat,
    ) -> Result<(), CaptureError> {
        let source = &self.host_links.get(index).ok_or(WorkError::Overflow)?.1;
        ensure_current_host_node(self.source_root, &host_path, metadata, self.meter)?;
        self.receipt.changed_paths = checked_increment(self.receipt.changed_paths)?;
        self.receipt.examined_paths = checked_increment(self.receipt.examined_paths)?;
        capture_drop_host_path(host_path, self.meter)?;
        capture_reserve(
            &mut self.mutations,
            1 + usize::from(current.is_some()),
            self.meter,
        )?;
        if let Some(record) = current {
            self.mutations.push(AuthoredMutation::Remove {
                path: clone_capture_path(&path, self.meter)?,
                expected_file_id: Some(record.file_id),
            });
        }
        self.mutations.push(AuthoredMutation::HardLink {
            source: clone_capture_path(source, self.meter)?,
            destination: path,
        });
        Ok(())
    }

    async fn canonical_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        checkout: &mut Checkout<A, O>,
        current: Option<FileRecord>,
        host_kind: FileKind,
        exists_with_kind: bool,
        metadata: &HostStat,
    ) -> Result<(FileMetadata, Option<FileMetadata>), CaptureError> {
        let mut canonical_metadata = if host_kind == FileKind::SymbolicLink && cfg!(unix) {
            FileMetadata::default()
        } else {
            capture_metadata(metadata)
        };
        let mut prior_metadata = None;
        if let Some(record) = current
            .filter(|_| exists_with_kind && (host_kind != FileKind::SymbolicLink || cfg!(windows)))
        {
            let lookup_cancellation = self
                .meter
                .with_original_leaf(|work| work.cancellation().clone());
            let prior = checkout
                .read_metadata(record.file_id, self.meter, &lookup_cancellation)
                .await?;
            prior_metadata = Some(prior);
            canonical_metadata = preserve_unobserved_metadata(canonical_metadata, prior);
            if let Some(baseline) = self.baseline {
                baseline.restore_canonical_stamps(
                    self.source_root.identity(),
                    prior,
                    &mut canonical_metadata,
                );
            }
        }
        Ok((canonical_metadata, prior_metadata))
    }

    async fn regular<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self,
        checkout: &mut Checkout<A, O>,
        path: NamespacePath,
        host_path: CaptureHostPath,
        metadata: &HostStat,
        canonical_metadata: FileMetadata,
        existing: (Option<FileRecord>, bool),
    ) -> Result<bool, CaptureError> {
        // Staging opens the file once and proves it is still the observed node before reading a byte.
        let staged = stage_regular_body(
            checkout,
            self.source_root,
            &host_path,
            metadata,
            self.maximum_extent_spans,
            self.meter,
            &mut self.receipt.staged_file_bytes,
        )
        .await?;
        capture_drop_host_path(host_path, self.meter)?;
        let logical_bytes = metadata.len();
        let ranges = staged;
        let ranges_bytes =
            ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        let unchanged = if logical_bytes == 0
            && ranges.is_empty()
            && existing.1
            && existing.0.is_some_and(|record| record.link_count == 1)
            && existing.0.is_some_and(|record| match record.payload {
                crate::kernel::FilePayload::InlineRegular(data) => data.as_bytes().is_empty(),
                _ => false,
            }) {
            true
        } else if let (Some(record), true, [(range, content)]) =
            (existing.0, existing.1, ranges.as_slice())
            && matches!(record.payload, crate::kernel::FilePayload::Regular { logical_bytes: prior, .. } if prior == logical_bytes)
            && range.offset == 0
            && range.length == logical_bytes
            && content.logical_bytes() == logical_bytes
        {
            let cancellation = self.meter.cancellation();
            let mut accounting = crate::kernel::KernelAccounting::native(self.meter, cancellation);
            let planned = checkout
                .plan_record_extents(
                    record,
                    crate::ByteRange {
                        offset: 0,
                        length: logical_bytes,
                    },
                    self.maximum_extent_spans,
                    &mut accounting,
                    cancellation,
                )
                .await;
            let planned = match planned {
                Ok(value) => Some(value),
                Err(crate::FsError::FileRead(crate::kernel::FileRangeReadError::Extent(
                    crate::kernel::ExtentReadError::TooManySpans,
                ))) => None,
                Err(error) => return Err(error.into()),
            };
            let expected = crate::kernel::ExtentSlice {
                offset: 0,
                length: logical_bytes,
                source_end: logical_bytes,
                kind: crate::kernel::ExtentKind::Content {
                    object: content.root(),
                    object_offset: 0,
                },
            };
            let unchanged = planned.as_ref().is_some_and(|(plan, _)| {
                plan.as_ref()
                    .is_some_and(|plan| plan.spans.as_slice() == std::slice::from_ref(&expected))
            });
            drop(planned);
            unchanged
        } else {
            false
        };
        #[cfg(test)]
        let unchanged = self
            .baseline
            .and_then(|baseline| baseline.reuse_override)
            .unwrap_or(unchanged);
        if unchanged {
            drop(ranges);
            self.meter
                .clone()
                .release(ranges_bytes)
                .map_err(capture_demand_error)?;
            capture_drop_path(path, self.meter)?;
            return Ok(false);
        }
        let count = ranges
            .len()
            .checked_add(2 + usize::from(logical_bytes != 0))
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        capture_reserve(&mut self.mutations, count, self.meter)?;
        if existing.0.is_some() {
            self.mutations.push(AuthoredMutation::Resize {
                path: clone_capture_path(&path, self.meter)?,
                logical_bytes: 0,
            });
        } else {
            self.mutations.push(AuthoredMutation::CreateFile {
                path: clone_capture_path(&path, self.meter)?,
                bytes: acyclic_stream::OwnedBytes::new(),
                metadata: canonical_metadata,
            });
        }
        if logical_bytes != 0 {
            self.mutations.push(AuthoredMutation::Resize {
                path: clone_capture_path(&path, self.meter)?,
                logical_bytes,
            });
        }
        for (range, content) in ranges {
            self.mutations.push(AuthoredMutation::WriteFromContent {
                path: clone_capture_path(&path, self.meter)?,
                offset: range.offset,
                content,
            });
        }
        self.meter
            .clone()
            .release(ranges_bytes)
            .map_err(capture_demand_error)?;
        self.mutations.push(AuthoredMutation::SetMetadata {
            path,
            metadata: canonical_metadata,
        });
        Ok(true)
    }

    fn special(
        &mut self,
        path: NamespacePath,
        host_path: CaptureHostPath,
        metadata: &HostStat,
        host_kind: FileKind,
        canonical_metadata: FileMetadata,
        exists_with_kind: bool,
    ) -> Result<(), CaptureError> {
        capture_reserve(
            &mut self.mutations,
            if exists_with_kind { 2 } else { 1 },
            self.meter,
        )?;
        match host_kind {
            FileKind::Regular => unreachable!("regular files are staged after preparation"),
            FileKind::Directory => self.mutations.push(if exists_with_kind {
                AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                }
            } else {
                AuthoredMutation::CreateDirectory {
                    path,
                    metadata: canonical_metadata,
                }
            }),
            FileKind::SymbolicLink => {
                let target = read_link_bytes(self.source_root, &host_path, self.meter)?;
                if exists_with_kind {
                    self.mutations.push(AuthoredMutation::Remove {
                        path: clone_capture_path(&path, self.meter)?,
                        expected_file_id: None,
                    });
                }
                let target = capture_link_target(target, self.meter)?;
                self.mutations.push(AuthoredMutation::CreateSymbolicLink {
                    path,
                    target: target.into(),
                    metadata: canonical_metadata,
                });
            }
            FileKind::Fifo | FileKind::Socket => self.mutations.push(if exists_with_kind {
                AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                }
            } else {
                AuthoredMutation::CreateEmptySpecial {
                    path,
                    kind: host_kind,
                    metadata: canonical_metadata,
                }
            }),
            FileKind::CharacterDevice | FileKind::BlockDevice => {
                let (major, minor) = host_device_identity(metadata)?;
                if exists_with_kind {
                    self.mutations.push(AuthoredMutation::Remove {
                        path: clone_capture_path(&path, self.meter)?,
                        expected_file_id: None,
                    });
                }
                self.mutations.push(AuthoredMutation::CreateDevice {
                    path,
                    kind: host_kind,
                    major,
                    minor,
                    metadata: canonical_metadata,
                });
            }
            FileKind::ReparsePoint | FileKind::MountBoundary => {
                return Err(CaptureError::UnsupportedKind);
            }
        }
        ensure_current_host_node(self.source_root, &host_path, metadata, self.meter)?;
        capture_drop_host_path(host_path, self.meter)?;
        Ok(())
    }
}

pub(super) fn host_path_is_absent(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    ) || cfg!(windows) && error.raw_os_error() == Some(267)
}
