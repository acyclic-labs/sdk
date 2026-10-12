use super::*;

pub(crate) fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
        Err(StreamError::LimitExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_records(records: &[OwnedBytes], meter: WireMeter<'_>) -> MemoryResult<()> {
    if records.is_empty() || records.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded.into());
    }
    for record in records {
        admit_item(meter)?;
        let oversized = record.len() > MAX_RECORD_BYTES;
        record_item(meter)?;
        if oversized {
            return Err(StreamError::LimitExceeded.into());
        }
    }
    Ok(())
}

pub(crate) fn validate_append_size(request: &AppendRequest) -> Result<(), StreamError> {
    (crate::wire_codec::CanonicalWire::Append(request)
        .encoded_len(None, None)
        .expect("ordinary sizing has no fallible meter")
        <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(StreamError::LimitExceeded)
}

pub(crate) fn validate_fork_size(request: &ForkRequest) -> Result<(), StreamError> {
    (crate::wire_codec::CanonicalWire::Fork(request)
        .encoded_len(None, None)
        .expect("ordinary sizing has no fallible meter")
        <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(StreamError::LimitExceeded)
}

pub(crate) fn normalize_commit(
    request: &mut CommitRequest,
    meter: WireMeter<'_>,
) -> MemoryResult<()> {
    if request.conditions.is_empty()
        || request.conditions.len() > MAX_ITEMS
        || request.mutations.is_empty()
        || request.mutations.len() > MAX_ITEMS
    {
        return Err(StreamError::LimitExceeded.into());
    }
    for mutation in &request.mutations {
        admit_item(meter)?;
        match mutation {
            CommitMutation::Append { records, .. } => validate_records(records, meter)?,
            CommitMutation::Fork { records, .. } if !records.is_empty() => {
                validate_records(records, meter)?;
            }
            CommitMutation::Fork { .. } => {}
        }
        record_item(meter)?;
    }
    if crate::wire_codec::CanonicalWire::Commit(request).encoded_len(None, meter)?
        > MAX_COMMAND_BYTES
    {
        return Err(StreamError::LimitExceeded.into());
    }
    request.conditions.sort_unstable_by(|left, right| {
        condition_path(left)
            .cmp(condition_path(right))
            .then_with(|| {
                matches!(left, CommitCondition::Absent { .. })
                    .cmp(&matches!(right, CommitCondition::Absent { .. }))
            })
    });
    request
        .mutations
        .sort_unstable_by(|left, right| mutation_path(left).cmp(mutation_path(right)));
    for pair in request.conditions.windows(2) {
        admit_item(meter)?;
        let duplicate =
            matches!(pair, [left, right] if condition_path(left) == condition_path(right));
        record_item(meter)?;
        if duplicate {
            return Err(StreamError::InvalidArgument.into());
        }
    }
    for pair in request.mutations.windows(2) {
        admit_item(meter)?;
        let duplicate =
            matches!(pair, [left, right] if mutation_path(left) == mutation_path(right));
        record_item(meter)?;
        if duplicate {
            return Err(StreamError::InvalidArgument.into());
        }
    }
    Ok(())
}

pub(crate) fn validate_commit_shape(
    request: &CommitRequest,
    meter: WireMeter<'_>,
) -> MemoryResult<()> {
    // Every caller normalizes first, so a binary search avoids a temporary tree.
    let condition = |path: &StreamPath| {
        request
            .conditions
            .binary_search_by(|condition| condition_path(condition).cmp(path))
            .ok()
            .and_then(|index| request.conditions.get(index))
    };
    for mutation in &request.mutations {
        admit_item(meter)?;
        match mutation {
            CommitMutation::Append { path, .. } => match condition(path) {
                Some(CommitCondition::Tail { .. } | CommitCondition::Absent { .. }) => {}
                _ => return Err(StreamError::InvalidArgument.into()),
            },
            CommitMutation::Fork { destination, .. } => {
                if !matches!(condition(destination), Some(CommitCondition::Absent { .. })) {
                    return Err(StreamError::InvalidArgument.into());
                }
            }
        }
        record_item(meter)?;
    }
    Ok(())
}
