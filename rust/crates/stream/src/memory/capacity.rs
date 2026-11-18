pub(crate) fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
        Err(StreamError::LimitExceeded)
    } else {
        Ok(())
    }
}

#[inline(never)]
pub(super) fn reserved(state: &State) -> (usize, usize) {
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

#[inline(never)]
fn reserve_records(
    state: &State,
    records: usize,
    bytes: usize,
    limits: MemoryLimits,
    reserved: (usize, usize),
) -> Result<(), StreamError> {
    let (pending, payload) = reserved;
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

// The adapter chain is a large local; keep it in this frame so the commit path's frame stays
// small enough for a loom coroutine (and an unoptimized thread). The walk allocates nothing.
#[inline(never)]
fn for_each_missing_path<'a>(
    state: &'a State,
    destinations: impl Iterator<Item = &'a StreamPath> + 'a,
    explicit: impl Fn(&str) -> bool + Copy + 'a,
    mut visit: impl FnMut(&'a StreamPath, usize) -> Result<(), StreamError>,
) -> Result<(), StreamError> {
    let mut previous = "";
    // Existing destinations imply indexed ancestors; no hierarchy work remains.
    // The explicit lookup uses the same canonical slice as destinations, so an
    // ancestor lookup never rescans the whole request or allocates another index.
    for destination in destinations.filter(|path| !contains_path(state, path.as_str())) {
        let path = destination.as_str();
        let before = std::mem::replace(&mut previous, path);
        for prefix in StreamPath::root_prefixes(path) {
            if !contains_path(state, prefix)
                && crate::path::relative_suffix(before, prefix).is_none()
                && (prefix.len() == path.len() || !explicit(prefix))
            {
                visit(destination, prefix.len())?;
            }
        }
    }
    Ok(())
}

#[inline(never)]
fn reserve_paths(
    state: &State,
    path: &StreamPath,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for_each_missing_path(
        state,
        std::iter::once(path),
        |prefix| path.as_str() == prefix,
        |_, end| {
            count += 1;
            bytes = bytes.saturating_add(end);
            Ok(())
        },
    )?;
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

/// Reserves the `applied` mutations of a settled commit.
#[inline(never)]
fn reserve_coordinated(
    state: &State,
    applied: &[&CommitMutation],
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let reserved = reserved(state);
    reserve_commit(state, limits, reserved)?;
    let (new_paths, path_bytes) = missing_totals(state, applied)?;
    let (records, bytes) = record_totals(applied);
    let (inherited_paths, inherited_bytes) = inherited_totals(state, applied)?;
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
    reserve_records(state, records, bytes, limits, reserved)
}

// Each total is its own frame: the commit path runs on a loom coroutine, and the
// iterator and fork walks are too large to keep live in the caller's frame.
#[inline(never)]
fn missing_totals(
    state: &State,
    applied: &[&CommitMutation],
) -> Result<(usize, usize), StreamError> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for_each_missing_path(
        state,
        applied.iter().map(|m| crate::CommitMutation::path(m)),
        |prefix| {
            applied
                .binary_search_by(|mutation| mutation.path().as_str().cmp(prefix))
                .is_ok()
        },
        |_, end| {
            count += 1;
            bytes = bytes.saturating_add(end);
            Ok(())
        },
    )?;
    Ok((count, bytes))
}

#[inline(never)]
fn record_totals(applied: &[&CommitMutation]) -> (usize, usize) {
    let mut records = 0_usize;
    let mut bytes = 0_usize;
    for mutation in applied {
        let batch = match mutation {
            CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                records
            }
        };
        records = records.saturating_add(batch.len());
        bytes = bytes.saturating_add(batch.iter().map(AppendRecord::payload_bytes).sum::<usize>());
    }
    (records, bytes)
}

#[inline(never)]
fn inherited_totals(
    state: &State,
    applied: &[&CommitMutation],
) -> Result<(usize, usize), StreamError> {
    let mut inherited_paths = 0usize;
    let mut inherited_bytes = 0usize;
    for mutation in applied {
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
    Ok((inherited_paths, inherited_bytes))
}

fn checked_subtree(
    state: &State,
    source: &StreamPath,
    destination: &StreamPath,
    exclude: &[crate::RelativePath],
) -> Result<Arc<NamespaceNode>, StreamError> {
    let source = state.paths.node(source).ok_or(StreamError::NotFound)?;
    let node = Namespace::excluded(source, exclude, &mut NamespaceBuilder::new(&|_| Ok(None)))?;
    if destination.as_str().len().saturating_add(node.max_suffix()) > crate::MAX_PATH_BYTES {
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
    let mut count = node.count() - 1;
    let mut bytes = (node.count() - 1)
        .saturating_mul(request.destination.as_str().len())
        .saturating_add(node.suffix_bytes());
    for_each_missing_path(
        state,
        std::iter::once(&request.destination),
        |prefix| request.destination.as_str() == prefix,
        |_, end| {
            count += 1;
            bytes = bytes.saturating_add(end);
            Ok(())
        },
    )?;
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
