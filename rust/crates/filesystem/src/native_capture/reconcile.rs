use super::*;

pub(super) async fn capture_observed_paths_from_root<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    observed: (Vec<Option<HostStat>>, Vec<NamespacePath>),
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    baseline: Option<&NativeViewBaseline>,
    meter: &NativeMaterializationWork,
) -> Result<CaptureReceipt, CaptureError> {
    let states = lookup_capture_states(checkout, observed, usize::MAX, meter).await?;
    let mut host_links = Vec::new();
    let captured = capture_ranked_paths_from_root(
        checkout,
        states,
        &mut host_links,
        maximum_extent_spans,
        source_root,
        baseline,
        meter,
    )
    .await;
    let cleanup = release_host_links(host_links, meter);
    match captured {
        Ok(captured) => {
            cleanup?;
            Ok(captured)
        }
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn capture_ranked_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    states: Vec<CapturePathState>,
    host_links: &mut Vec<([u8; 16], NamespacePath)>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    baseline: Option<&NativeViewBaseline>,
    meter: &NativeMaterializationWork,
) -> Result<CaptureReceipt, CaptureError> {
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    meter.cancellation().check()?;
    meter.clone().check().map_err(capture_demand_error)?;
    let mut receipt = CaptureReceipt::default();
    let mut mutations = Vec::new();
    for (observation, path, current) in states {
        prepare_final_path(
            checkout,
            path,
            current,
            CaptureIntent::Complete,
            source_root,
            observation,
            host_links,
            None,
            baseline,
            &mut mutations,
            &mut receipt,
            maximum_extent_spans,
            meter,
        )
        .await?;
    }
    {
        meter
            .clone()
            .release(states_bytes)
            .map_err(capture_demand_error)?;
    }
    apply_capture_transaction(checkout, mutations, meter).await?;
    {
        meter
            .receipt()
            .verify(meter.with_original_leaf(|work| work.budget))
            .map_err(CaptureError::from)?;
    }
    Ok(receipt)
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
        let page = &paths.as_slice()[..count];
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

pub(super) fn sort_capture_states(
    states: &mut [CapturePathState],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    capture_sort_by(
        states,
        |(left, left_path, left_current), (right, right_path, right_current)| {
            let rank =
                |observation: &Option<HostStat>, current: &Option<FileRecord>| match observation {
                    None => 0,
                    Some(observation) if observation.is_dir() => 1,
                    Some(_) if current.is_some() => 2,
                    Some(_) => 3,
                };
            let left_rank = rank(left, left_current);
            let right_rank = rank(right, right_current);
            left_rank.cmp(&right_rank).then_with(|| {
                if left_rank == 0 {
                    right_path
                        .depth()
                        .cmp(&left_path.depth())
                        .then_with(|| left_path.cmp(right_path))
                } else {
                    left_path
                        .depth()
                        .cmp(&right_path.depth())
                        .then_with(|| left_path.cmp(right_path))
                }
            })
        },
        meter,
    )
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum CaptureIntent {
    MetadataOnly,
    Complete,
    /// As `Complete`, for a name a rename bound: a directory's subtree is new.
    Arrived,
    Replace,
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
pub(super) async fn prepare_final_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: NamespacePath,
    current: Option<FileRecord>,
    intent: CaptureIntent,
    source_root: &HostRoot,
    observation: Option<HostStat>,
    host_links: &mut Vec<([u8; 16], NamespacePath)>,
    watch_epoch: Option<u64>,
    baseline: Option<&NativeViewBaseline>,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    maximum_extent_spans: u32,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    meter.cancellation().check()?;
    let host_path = namespace_to_host_path_measured(&path, meter)?;
    let observation = match observation {
        Some(observation) => Some(observation),
        None => {
            let observed = observe_host_path(source_root, &host_path, meter);
            observed?
        }
    };
    match observation {
        Some(observation) => {
            let metadata = observation;
            let host_kind = host_kind(&metadata)?;
            let snapshot = HostSnapshot::from_metadata(&metadata);
            let linked_regular = host_kind == FileKind::Regular
                && checkout.volume_config().hard_links
                && metadata.nlink() > 1;
            if linked_regular {
                let identity = snapshot.identity.to_bytes();
                let mut source = None;
                for (node, path) in host_links.iter() {
                    capture_charge(
                        meter,
                        WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        },
                    )?;
                    if *node == identity {
                        source = Some(path);
                        break;
                    }
                }
                if let Some(source) = source {
                    ensure_current_host_node(source_root, &host_path, &snapshot, meter)?;
                    receipt.changed_paths = checked_increment(receipt.changed_paths)?;
                    receipt.examined_paths = checked_increment(receipt.examined_paths)?;
                    capture_drop_host_path(host_path, meter)?;
                    capture_reserve(mutations, 1 + usize::from(current.is_some()), meter)?;
                    if let Some(record) = current {
                        mutations.push(AuthoredMutation::Remove {
                            path: clone_capture_path(&path, meter)?,
                            expected_file_id: Some(record.file_id),
                        });
                    }
                    mutations.push(AuthoredMutation::HardLink {
                        source: clone_capture_path(source, meter)?,
                        destination: path,
                    });
                    return Ok(());
                }
                if let Some(epoch) = watch_epoch {
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
                    push_subtree_removals(checkout, &path, mutations, receipt, meter).await?;
                }
                capture_reserve(mutations, 1, meter)?;
                mutations.push(AuthoredMutation::Remove {
                    path: clone_capture_path(&path, meter)?,
                    expected_file_id: Some(record.file_id),
                });
            }
            let exists_with_kind =
                !replace && current.is_some_and(|record| record.kind == host_kind);
            let mut canonical_metadata = if host_kind == FileKind::SymbolicLink && cfg!(unix) {
                FileMetadata::default()
            } else {
                snapshot.metadata
            };
            let mut prior_metadata = None;
            if let Some(record) = current.filter(|record| {
                exists_with_kind
                    && record.kind == host_kind
                    && (host_kind != FileKind::SymbolicLink || cfg!(windows))
            }) {
                let lookup_cancellation =
                    meter.with_original_leaf(|work| work.cancellation().clone());
                let prior = checkout
                    .read_metadata(record.file_id, meter, &lookup_cancellation)
                    .await?;
                prior_metadata = Some(prior);
                canonical_metadata = preserve_unobserved_metadata(canonical_metadata, prior);
                if let Some(baseline) = baseline {
                    baseline.restore_canonical_stamps(
                        source_root.identity(),
                        prior,
                        &mut canonical_metadata,
                    );
                }
            }
            // A directory's timestamps follow what happens to its entries, each captured through its own path, and to reads of it: a change to them alone is no change of the directory's.
            if host_kind == FileKind::Directory
                && prior_metadata.is_some_and(|prior| {
                    prior.without_timestamps() == canonical_metadata.without_timestamps()
                })
            {
                receipt.examined_paths = checked_increment(receipt.examined_paths)?;
                {
                    capture_drop_path(path, meter)?;
                    capture_drop_host_path(host_path, meter)
                }?;
                return Ok(());
            }
            let linked_path = if linked_regular {
                Some(clone_capture_path(&path, meter)?)
            } else {
                None
            };
            if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                ensure_current_host_node(source_root, &host_path, &snapshot, meter)?;
                capture_reserve(mutations, 1, meter)?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
                capture_drop_host_path(host_path, meter)?;
            } else if host_kind == FileKind::Regular {
                // Staging opens the file once and proves it is still the observed node before reading a byte.
                let staged = stage_regular_body(
                    checkout,
                    source_root,
                    &host_path,
                    &snapshot,
                    maximum_extent_spans,
                    meter,
                    &mut receipt.staged_file_bytes,
                )
                .await?;
                capture_drop_host_path(host_path, meter)?;
                let logical_bytes = snapshot.length;
                let ranges = staged;
                let ranges_bytes = ranges.capacity() as u64
                    * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
                let count = ranges
                    .len()
                    .checked_add(2 + usize::from(logical_bytes != 0))
                    .ok_or(CaptureError::from(WorkError::Overflow))?;
                capture_reserve(mutations, count, meter)?;
                if exists_with_kind {
                    mutations.push(AuthoredMutation::Resize {
                        path: clone_capture_path(&path, meter)?,
                        logical_bytes: 0,
                    });
                } else {
                    mutations.push(AuthoredMutation::CreateFile {
                        path: clone_capture_path(&path, meter)?,
                        bytes: acyclic_stream::OwnedBytes::new(),
                        metadata: canonical_metadata,
                    });
                }
                if logical_bytes != 0 {
                    mutations.push(AuthoredMutation::Resize {
                        path: clone_capture_path(&path, meter)?,
                        logical_bytes,
                    });
                }
                for (range, content) in ranges {
                    mutations.push(AuthoredMutation::WriteFromContent {
                        path: clone_capture_path(&path, meter)?,
                        offset: range.offset,
                        content,
                    });
                }
                meter
                    .clone()
                    .release(ranges_bytes)
                    .map_err(capture_demand_error)?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
            } else {
                capture_reserve(mutations, if exists_with_kind { 2 } else { 1 }, meter)?;
                match host_kind {
                    FileKind::Regular => unreachable!("regular files are staged after preparation"),
                    FileKind::Directory => mutations.push(if exists_with_kind {
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
                        let target = read_link_bytes(source_root, &host_path, meter)?;
                        if exists_with_kind {
                            mutations.push(AuthoredMutation::Remove {
                                path: clone_capture_path(&path, meter)?,
                                expected_file_id: None,
                            });
                        }
                        let target = capture_link_target(target, meter)?;
                        mutations.push(AuthoredMutation::CreateSymbolicLink {
                            path,
                            target: target.into(),
                            metadata: canonical_metadata,
                        });
                    }
                    FileKind::Fifo | FileKind::Socket => mutations.push(if exists_with_kind {
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
                        let (major, minor) = host_device_identity(&metadata)?;
                        if exists_with_kind {
                            mutations.push(AuthoredMutation::Remove {
                                path: clone_capture_path(&path, meter)?,
                                expected_file_id: None,
                            });
                        }
                        mutations.push(AuthoredMutation::CreateDevice {
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
                ensure_current_host_node(source_root, &host_path, &snapshot, meter)?;
                capture_drop_host_path(host_path, meter)?;
            };
            receipt.changed_paths = checked_increment(receipt.changed_paths)?;
            if let Some(linked_path) = linked_path {
                capture_reserve(host_links, 1, meter)?;
                host_links.push((snapshot.identity.to_bytes(), linked_path));
            }
            receipt.examined_paths = checked_increment(receipt.examined_paths)?;
            return Ok(());
        }
        None => {
            if let Some(record) = current {
                // A vanished directory requires descendant removals before removing the directory itself.
                if record.kind == FileKind::Directory {
                    push_subtree_removals(checkout, &path, mutations, receipt, meter).await?;
                }
                capture_reserve(mutations, 1, meter)?;
                mutations.push(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(record.file_id),
                });
                receipt.changed_paths = checked_increment(receipt.changed_paths)?;
            } else {
                capture_drop_path(path, meter)?;
            }
        }
    }
    capture_drop_host_path(host_path, meter)?;
    receipt.examined_paths = checked_increment(receipt.examined_paths)?;
    Ok(())
}

pub(super) fn host_path_is_absent(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    ) || cfg!(windows) && error.raw_os_error() == Some(267)
}
