pub(crate) fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
        Err(StreamError::LimitExceeded)
    } else {
        Ok(())
    }
}

fn reserved(state: &State) -> (usize, usize) {
    #[cfg(not(target_arch = "wasm32"))]
    return *state
        .reserved
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    #[cfg(target_arch = "wasm32")]
    {
        let _ = state;
        (0, 0)
    }
}

fn reserve_records(
    state: &State,
    records: usize,
    bytes: usize,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let (pending, payload) = reserved(state);
    if state
        .record_count
        .saturating_add(pending)
        .saturating_add(records)
        > limits.records
        || state
            .payload_bytes
            .saturating_add(payload)
            .saturating_add(bytes)
            > limits.payload_bytes
    {
        return Err(StreamError::Capacity);
    }
    Ok(())
}

fn missing_paths<'a>(
    state: &'a State,
    destinations: impl Iterator<Item = &'a StreamPath> + Clone + 'a,
) -> impl Iterator<Item = (&'a StreamPath, usize)> + 'a {
    let mut previous = "";
    let all = destinations.clone();
    destinations.flat_map(move |destination| {
        let path = destination.as_str();
        let before = previous;
        let all = all.clone();
        previous = path;
        path.match_indices('/')
            .map(|(end, _)| end)
            .chain(std::iter::once(path.len()))
            .filter(move |end| {
                let prefix = &path[..*end];
                !state.paths.contains_key(prefix)
                    && before != prefix
                    && !before
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                    && (*end == path.len() || !all.clone().any(|other| other.as_str() == prefix))
            })
            .map(move |end| (destination, end))
    })
}

fn reserve_paths(
    state: &State,
    path: &StreamPath,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let (count, bytes) = missing_paths(state, std::iter::once(path))
        .fold((0usize, 0usize), |(count, bytes), (_, end)| {
            (count + 1, bytes.saturating_add(end))
        });
    if state.paths.len().saturating_add(count) > limits.paths
        || state.path_bytes.saturating_add(bytes) > limits.path_bytes
    {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}

fn reserve_coordinated(
    state: &State,
    request: &CommitRequest,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    reserve_commit(state, limits)?;
    let (new_paths, path_bytes) = missing_paths(state, request.mutations.iter().map(mutation_path))
        .fold((0usize, 0usize), |(count, bytes), (_, end)| {
            (count + 1, bytes.saturating_add(end))
        });
    let mut records = 0_usize;
    let mut bytes = 0_usize;
    for mutation in &request.mutations {
        let batch = match mutation {
            CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                records
            }
        };
        records = records.saturating_add(batch.len());
        bytes = bytes.saturating_add(batch.iter().map(Bytes::len).sum::<usize>());
    }
    if state.paths.len().saturating_add(new_paths) > limits.paths
        || state.path_bytes.saturating_add(path_bytes) > limits.path_bytes
    {
        return Err(StreamError::Capacity);
    }
    reserve_records(state, records, bytes, limits)
}
