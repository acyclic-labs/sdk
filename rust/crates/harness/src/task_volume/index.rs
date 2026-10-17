use super::*;

pub(super) fn validate_file(index: &Index, file: &FileValue) -> Result<()> {
    if matches!(
        file.key,
        FileKey::Child {
            file: ChildFile::Status,
            ..
        }
    ) {
        ChildStatus::parse(&file.bytes)?;
    }
    file.validate()?;
    if let FileKey::Child {
        reference,
        file: child_file,
    } = &file.key
    {
        if matches!(child_file, ChildFile::Delivered) {
            let (task, delivered): (crate::TaskId, u64) =
                serde_json::from_slice(&file.bytes).map_err(|_| Error::InvalidSegment)?;
            let count = index.child_inputs.get(reference).map_or(0, Vec::len) as u64;
            if delivered == 0 || delivered > count {
                return Err(Error::InvalidSegment);
            }
            if let Some(old) = index.files.get(&file.key) {
                let (previous_task, previous): (crate::TaskId, u64) =
                    serde_json::from_slice(&old.file.bytes).map_err(|_| Error::InvalidSegment)?;
                if previous_task != task || previous > delivered {
                    return Err(Error::InvalidSegment);
                }
            }
        }
        let control_key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Control,
        };
        let control = index.files.get(&control_key).map(|stored| &stored.file);
        let state = match control {
            Some(control) => ChildState::Running.apply(Control::parse(&control.bytes)?),
            None => ChildState::Running,
        };
        let terminal = matches!(state, ChildState::Archived | ChildState::Cancelled);
        if matches!(child_file, ChildFile::Status) {
            let status = ChildStatus::parse(&file.bytes)?;
            if status.state != state {
                return Err(Error::ImmutableConflict(control_key));
            }
            if let Some(previous) = index.files.get(&file.key).map(|stored| &stored.file) {
                let previous = ChildStatus::parse(&previous.bytes)?;
                if status.inputs < previous.inputs
                    || (terminal
                        && (status.task != previous.task || status.inputs != previous.inputs))
                {
                    return Err(Error::ImmutableConflict(file.key.clone()));
                }
                // The first quiescent terminal acknowledgement freezes this
                // status too. No later result or input binding can replace it.
                let archival_ack =
                    previous.state == ChildState::Cancelled && status.state == ChildState::Archived;
                if matches!(previous.state, ChildState::Archived | ChildState::Cancelled)
                    && !archival_ack
                    && index.files.get(&file.key).map(|stored| &stored.file) != Some(file)
                {
                    return Err(Error::ImmutableConflict(file.key.clone()));
                }
            }
        } else if terminal && control != Some(file) {
            let draining_result = matches!(child_file, ChildFile::Result(_) | ChildFile::Delivered)
                && index
                    .files
                    .get(&FileKey::Child {
                        reference: reference.clone(),
                        file: ChildFile::Status,
                    })
                    .is_some_and(|stored| {
                        ChildStatus::parse(&stored.file.bytes).is_ok_and(|status| {
                            !matches!(status.state, ChildState::Cancelled | ChildState::Archived)
                        })
                    });
            let archival_intent = state == ChildState::Cancelled
                && matches!(child_file, ChildFile::Control)
                && Control::parse(&file.bytes)? == Control::Archive;
            if !draining_result && !archival_intent {
                return Err(Error::ImmutableConflict(control_key));
            }
        }
        if matches!(child_file, ChildFile::Control) {
            Control::parse(&file.bytes)?;
        }
        if let ChildFile::Result(turn) = child_file {
            let result = ChildTurnResult::parse(&file.bytes)?;
            let status_key = FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Status,
            };
            let status = index.files.get(&status_key).ok_or(Error::InvalidSegment)?;
            let status = ChildStatus::parse(&status.file.bytes)?;
            if result.turn != *turn || status.task != result.task || *turn > status.inputs {
                return Err(Error::InvalidSegment);
            }
            if !index.files.contains_key(&file.key)
                && *turn > 1
                && !index.files.contains_key(&FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Result(turn - 1),
                })
            {
                return Err(Error::InvalidSegment);
            }
        }
    }
    if !file.key.mutable()
        && index
            .files
            .get(&file.key)
            .is_some_and(|old| &old.file != file)
    {
        return Err(Error::ImmutableConflict(file.key.clone()));
    }
    if !index.files.contains_key(&file.key)
        && let FileKey::Task {
            task,
            file:
                TaskFile::Chunk {
                    execution,
                    turn,
                    sequence,
                },
        } = &file.key
        && index
            .chunk_tails
            .get(&(task.clone(), *execution, *turn))
            .is_some_and(|previous| sequence <= previous)
    {
        return Err(Error::SequenceConflict(file.key.clone()));
    }
    if !index.files.contains_key(&file.key)
        && let Some((first, last, sequence)) = file.key.sequence_range()
        && index
            .files
            .range(first..=last)
            .next_back()
            .and_then(|(key, _)| key.sequence_range().map(|(_, _, n)| n))
            .is_some_and(|previous| sequence <= previous)
    {
        return Err(Error::SequenceConflict(file.key.clone()));
    }
    Ok(())
}

pub(super) fn apply(index: &mut Index, file: FileValue, position: u64) {
    if matches!(
        file.key,
        FileKey::Task {
            file: TaskFile::Chunk { .. },
            ..
        }
    ) && index.files.contains_key(&file.key)
    {
        return;
    }
    scoped_chunks::record(index, &file, position);
    if let FileKey::Task {
        task,
        file:
            TaskFile::Chunk {
                execution,
                turn,
                sequence,
            },
    } = &file.key
    {
        index
            .chunk_tails
            .entry((task.clone(), *execution, *turn))
            .and_modify(|previous| *previous = (*previous).max(*sequence))
            .or_insert(*sequence);
    }
    if let FileKey::Child {
        reference,
        file: ChildFile::Request | ChildFile::Inbox(_),
    } = &file.key
        && !index.files.contains_key(&file.key)
    {
        index
            .child_inputs
            .entry(reference.clone())
            .or_default()
            .push(file.key.clone());
    }
    recent::record(index, &file, position);
    let owner = command_query::owner(&file.key);
    if command_query::is_result(&file.key) && !index.files.contains_key(&file.key) {
        index
            .executions
            .insert((owner.clone(), position), file.key.clone());
        if let Some(pending) = index.pending_commands.get_mut(&owner) {
            for make in [TaskFile::Request, TaskFile::ExecutionClaim] {
                if let Some(key) = command_query::related(&file.key, make) {
                    pending.remove(&key);
                }
            }
            if pending.is_empty() {
                index.pending_commands.remove(&owner);
            }
        }
    } else if let Some(result) = command_query::related(&file.key, TaskFile::ExecutionResult) {
        if !index.files.contains_key(&result) {
            index
                .pending_commands
                .entry(owner)
                .or_default()
                .insert(file.key.clone());
        }
    }
    index
        .files
        .insert(file.key.clone(), StoredFile { file, position });
}
