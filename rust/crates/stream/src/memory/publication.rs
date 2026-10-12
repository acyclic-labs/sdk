use super::*;

pub(super) fn replay(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<IdempotencyOutcome>, StreamError> {
    let Some(key) = key else { return Ok(None) };
    let Some(replay) = state.replays.get(key.as_bytes()) else {
        return Ok(None);
    };
    if replay.digest != digest {
        return Err(StreamError::IdempotencyMismatch);
    }
    Ok(Some(replay.result.clone()))
}

pub(super) fn replay_fork(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<ForkReceipt>, StreamError> {
    match replay(state, key, digest)? {
        Some(IdempotencyOutcome::Fork(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}

pub(super) fn missing_paths<'a>(
    state: &State,
    paths: impl IntoIterator<Item = &'a StreamPath>,
    meter: WireMeter<'_>,
) -> MemoryResult<NativeStreamOutput<BTreeSet<&'a str>>> {
    let mut output = NativeStreamOutput {
        value: BTreeSet::new(),
        allocation: allocate(meter, 0)?,
    };
    for path in paths {
        let mut current = Some(path.as_str());
        while let Some(path) = current {
            admit_item(meter)?;
            if !state.paths.contains_key(path) && !output.value.contains(path) {
                // Declared logical entry quota, not an invented std BTree node.
                absorb(
                    &mut output.allocation,
                    adopt(meter, std::mem::size_of::<&str>())?,
                )?;
                output.value.insert(path);
            }
            record_item(meter)?;
            current = path.rsplit_once('/').map(|(parent, _)| parent);
        }
    }
    Ok(output)
}

pub(super) struct PreparedMutation {
    pub(super) path: StreamPath,
    pub(super) history: Arc<History>,
    pub(super) bytes: usize,
}

pub(super) struct PreparedPublication {
    pub(super) cached: Arc<Replay>,
    pub(super) key: Option<OwnedBytes>,
    pub(super) paths: NativeStreamOutput<Vec<(StreamPath, PathState)>>,
    pub(super) mutations: NativeStreamOutput<Vec<PreparedMutation>>,
    pub(super) resident: Allocation,
}

pub(super) fn prepare_paths(
    paths: &BTreeSet<&str>,
    envelope: &CommittedEnvelope,
    meter: WireMeter<'_>,
) -> MemoryResult<NativeStreamOutput<Vec<(StreamPath, PathState)>>> {
    let mut output = vec(meter, paths.len())?;
    for path in paths {
        admit_item(meter)?;
        let mut ancestor = envelope
            .mutations
            .iter()
            .map(|mutation| match mutation {
                CommittedMutation::Append(append) => &append.path,
                CommittedMutation::Fork(fork) => &fork.destination,
            })
            .find(|candidate| {
                candidate.as_str() == *path
                    || candidate
                        .as_str()
                        .strip_prefix(*path)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
            .ok_or(StreamError::Unavailable)?
            .clone();
        while ancestor.as_str() != *path {
            admit_item(meter)?;
            ancestor = parent_path(meter, &ancestor)?.ok_or(StreamError::Unavailable)?;
            record_item(meter)?;
        }
        let allocation = adopt(meter, std::mem::size_of::<(StreamPath, PathState)>())?;
        let (changed, _) = watch::channel(0);
        output.value.push((
            ancestor,
            PathState {
                history: None,
                tail: 0,
                changed,
                _allocation: allocation,
            },
        ));
        record_item(meter)?;
    }
    Ok(output)
}

pub(super) fn batch_history(
    parent: Option<Arc<History>>,
    records: &[Record],
    meter: WireMeter<'_>,
) -> MemoryResult<Arc<History>> {
    let mut records = clone_records(records, meter, false)?;
    absorb(
        &mut records.allocation,
        allocate(
            meter,
            std::mem::size_of::<NativeStreamOutput<Vec<Record>>>(),
        )?,
    )?;
    let records = Arc::new(records);
    let allocation = allocate(meter, std::mem::size_of::<History>())?;
    Ok(Arc::new(History::Batch {
        parent,
        records,
        _allocation: allocation,
    }))
}

pub(super) fn prepare_mutations(
    state: &State,
    envelope: &CommittedEnvelope,
    meter: WireMeter<'_>,
) -> MemoryResult<NativeStreamOutput<Vec<PreparedMutation>>> {
    let mut output = vec(meter, envelope.mutations.len())?;
    for mutation in &envelope.mutations {
        admit_item(meter)?;
        let (path, history, records) = match mutation {
            CommittedMutation::Append(append) => (
                append.path.clone(),
                batch_history(
                    state
                        .paths
                        .get(&append.path)
                        .and_then(|path| path.history.clone()),
                    &append.records,
                    meter,
                )?,
                &append.records,
            ),
            CommittedMutation::Fork(fork) => {
                let source = state.paths.get(&fork.source).ok_or(StreamError::NotFound)?;
                let allocation = allocate(meter, std::mem::size_of::<History>())?;
                let prefix = Arc::new(History::Prefix {
                    source: source.history.clone(),
                    tail: fork.forked_at,
                    _allocation: allocation,
                });
                let history = if fork.records.is_empty() {
                    prefix
                } else {
                    batch_history(Some(prefix), &fork.records, meter)?
                };
                (fork.destination.clone(), history, &fork.records)
            }
        };
        output.value.push(PreparedMutation {
            path,
            history,
            bytes: records.iter().map(|record| record.value.len()).sum(),
        });
        record_item(meter)?;
    }
    Ok(output)
}

pub(super) fn prepare_retained_capacity(
    state: &mut State,
    meter: WireMeter<'_>,
) -> MemoryResult<()> {
    let minimum = state
        .retained
        .len()
        .checked_add(state.prepared_capacity.pending.load(Ordering::Acquire))
        .and_then(|length| length.checked_add(1))
        .ok_or(StreamError::Capacity)?;
    if minimum <= state.retained.capacity() {
        return Ok(());
    }
    let capacity = state
        .retained
        .capacity()
        .checked_mul(2)
        .ok_or(StreamError::Capacity)?
        .max(minimum)
        .max(4);
    let allocation = allocate(
        meter,
        capacity
            .checked_mul(std::mem::size_of::<Retained>())
            .ok_or(StreamError::Capacity)?,
    )?;
    state.prepared_capacity.advance_revision();
    state
        .retained
        .reserve_exact(capacity - state.retained.len());
    // Transfer the new capacity before fallible reconciliation.
    state.retained_allocation = allocation;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(allocation) = state.retained_allocation.as_mut() {
        allocation.reconcile_capacity(
            state
                .retained
                .capacity()
                .checked_mul(std::mem::size_of::<Retained>())
                .ok_or(StreamError::Capacity)?,
        )?;
    }
    Ok(())
}

pub(super) fn publish(state: &mut State, mut prepared: PreparedPublication, digest: [u8; 32]) {
    state.prepared_capacity.advance_revision();
    let creates_paths = !prepared.paths.value.is_empty();
    for (path, stream) in prepared.paths.value {
        state.path_bytes = state.path_bytes.saturating_add(path.as_str().len());
        state.paths.insert(path, stream);
    }
    for mutation in prepared.mutations.value {
        let stream = state
            .paths
            .get_mut(&mutation.path)
            .expect("all destinations were prepared under this same write lock");
        let (tail, count) = match mutation.history.as_ref() {
            History::Batch { records, .. } => (
                records
                    .value
                    .last()
                    .expect("canonical batches contain records")
                    .sequence
                    + 1,
                records.value.len(),
            ),
            History::Prefix { tail, .. } => (*tail, 0),
        };
        stream.history = Some(mutation.history);
        stream.tail = tail;
        stream.changed.send_replace(tail);
        state.record_count += count;
        state.payload_bytes += mutation.bytes;
    }
    Arc::get_mut(&mut prepared.cached)
        .expect("an unpublished cache has one owner")
        .digest = digest;
    let commit = prepared
        .cached
        .envelope()
        .map(|envelope| envelope.commit_id);
    if let Some(commit_id) = commit {
        state
            .commits
            .insert(commit_id, Arc::clone(&prepared.cached));
        if creates_paths {
            state.hierarchy_version = commit_id;
        }
    }
    let replay = prepared.key.map(|key| {
        state.replays.insert(key.clone(), prepared.cached);
        key
    });
    let resident = prepared.resident;
    if commit.is_some() || replay.is_some() {
        state.retained.push(Retained {
            replay,
            commit,
            _allocation: resident,
        });
    }
}
