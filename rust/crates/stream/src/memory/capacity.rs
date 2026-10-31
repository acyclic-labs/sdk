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
    if limits.records.is_some_and(|limit| {
        state
            .record_count
            .saturating_add(pending)
            .saturating_add(records)
            > limit
    }) || limits.payload_bytes.is_some_and(|limit| {
        state
            .payload_bytes
            .saturating_add(payload)
            .saturating_add(bytes)
            > limit
    }) {
        return Err(StreamError::Capacity);
    }
    Ok(())
}

#[inline]
fn contains_path(state: &State, path: &str) -> bool {
    #[cfg(test)]
    PATH_LOOKUPS.with(|count| count.set(count.get() + 1));
    state.paths.contains_key(path)
}
#[cfg(test)]
std::thread_local! { static PATH_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn missing_paths<'a>(
    state: &'a State,
    destinations: impl Iterator<Item = &'a StreamPath> + Clone + 'a,
) -> impl Iterator<Item = (&'a StreamPath, usize)> + 'a {
    let mut previous = "";
    let all = destinations.clone();
    // Existing destinations imply indexed ancestors; no hierarchy work remains.
    destinations
        .filter(|path| !contains_path(state, path.as_str()))
        .flat_map(move |destination| {
            let path = destination.as_str();
            let before = previous;
            let all = all.clone();
            previous = path;
            path.match_indices('/')
                .map(|(end, _)| end)
                .chain(std::iter::once(path.len()))
                .filter(move |end| {
                    let Some(prefix) = path.get(..*end) else {
                        return false;
                    };
                    !contains_path(state, prefix)
                        && before != prefix
                        && !before
                            .strip_prefix(prefix)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                        && (*end == path.len()
                            || !all.clone().any(|other| other.as_str() == prefix))
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
    if limits
        .paths
        .is_some_and(|limit| state.paths.len().saturating_add(count) > limit)
        || limits
            .path_bytes
            .is_some_and(|limit| state.path_bytes.saturating_add(bytes) > limit)
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
        bytes = bytes.saturating_add(batch.iter().map(AppendRecord::payload_bytes).sum::<usize>());
    }
    let mut inherited_paths = 0usize;
    let mut inherited_bytes = 0usize;
    for mutation in &request.mutations {
        if let CommitMutation::Fork {
            source,
            destination,
            exclude,
            ..
        } = mutation
        {
            let node = checked_subtree(state, source, destination, exclude)?;
            inherited_paths = inherited_paths.saturating_add(node.count() - 1);
            inherited_bytes = inherited_bytes.saturating_add(
                (node.count() - 1)
                    .saturating_mul(destination.as_str().len())
                    .saturating_add(node.suffix_bytes()),
            );
        }
    }
    if limits.paths.is_some_and(|limit| {
        state
            .paths
            .len()
            .saturating_add(new_paths)
            .saturating_add(inherited_paths)
            > limit
    }) || limits.path_bytes.is_some_and(|limit| {
        state
            .path_bytes
            .saturating_add(path_bytes)
            .saturating_add(inherited_bytes)
            > limit
    }) {
        return Err(StreamError::Capacity);
    }
    reserve_records(state, records, bytes, limits)
}

fn checked_subtree(
    state: &State,
    source: &StreamPath,
    destination: &StreamPath,
    exclude: &[crate::RelativePath],
) -> Result<Arc<NamespaceNode>, StreamError> {
    let source = state.paths.node(source).ok_or(StreamError::NotFound)?;
    let node = Namespace::excluded(source, exclude, &mut NamespaceBuilder::new(&|_| Ok(None)))?;
    if destination.as_str().len().saturating_add(node.max_suffix()) > crate::MAX_PATH_BYTES
        || destination.as_str().split('/').count() + node.depth() - 1 > MAX_ITEMS
    {
        return Err(StreamError::InvalidPath);
    }
    Ok(node)
}
fn reserve_fork(
    state: &State,
    request: &ForkRequest,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let node = checked_subtree(
        state,
        &request.source,
        &request.destination,
        &request.exclude,
    )?;
    let (count, bytes) = missing_paths(state, std::iter::once(&request.destination)).fold(
        (
            node.count() - 1,
            (node.count() - 1)
                .saturating_mul(request.destination.as_str().len())
                .saturating_add(node.suffix_bytes()),
        ),
        |(count, bytes), (_, end)| (count + 1, bytes.saturating_add(end)),
    );
    if limits
        .paths
        .is_some_and(|limit| state.paths.len().saturating_add(count) > limit)
        || limits
            .path_bytes
            .is_some_and(|limit| state.path_bytes.saturating_add(bytes) > limit)
    {
        return Err(StreamError::Capacity);
    }
    Ok(())
}
