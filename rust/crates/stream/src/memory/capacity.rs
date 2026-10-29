pub(crate) fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 {
        Err(StreamError::InvalidArgument)
    } else {
        Ok(())
    }
}

pub(crate) fn page_limit(limit: u32) -> Result<u32, StreamError> {
    validate_limit(limit)?;
    Ok(limit.min(crate::CHILDREN_PAGE_ITEMS))
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
    explicit: impl Fn(&str) -> bool + Copy + 'a,
) -> impl Iterator<Item = (&'a StreamPath, usize)> + 'a {
    let mut previous = "";
    // Existing destinations imply indexed ancestors; no hierarchy work remains.
    // The explicit lookup uses the same canonical slice as destinations, so an
    // ancestor lookup never rescans the whole request or allocates another index.
    destinations
        .filter(|path| !contains_path(state, path.as_str()))
        .flat_map(move |destination| {
            let path = destination.as_str();
            let before = previous;
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
                        && (*end == path.len() || !explicit(prefix))
                })
                .map(move |end| (destination, end))
        })
}

fn reserve_paths(
    state: &State,
    path: &StreamPath,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let (count, bytes) = missing_paths(state, std::iter::once(path), |prefix| {
        path.as_str() == prefix
    })
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
    let (new_paths, path_bytes) = missing_paths(
        state,
        request.mutations.iter().map(mutation_path),
        |prefix| {
            request
                .mutations
                .binary_search_by(|mutation| mutation_path(mutation).as_str().cmp(prefix))
                .is_ok()
        },
    )
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
    if destination.as_str().split('/').count() + node.depth() - 1 > MAX_ITEMS {
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
    let (count, bytes) = missing_paths(state, std::iter::once(&request.destination), |prefix| {
        request.destination.as_str() == prefix
    })
    .fold(
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
