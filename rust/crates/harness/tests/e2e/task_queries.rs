use super::{TestResult, task_query_case::TaskQueryCase};
use acyclic_harness::task_volume::{
    Change, ChangeStream, Cursor, Error, FileKey, FileValue, Query, TaskVolume,
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::{FutureExt as _, StreamExt as _};
use std::{collections::BTreeMap, sync::Arc};

fn files(values: &[FileValue]) -> TestResult<BTreeMap<String, String>> {
    values
        .iter()
        .map(|file| {
            Ok((
                file.key.path(),
                std::str::from_utf8(&file.bytes)?.to_owned(),
            ))
        })
        .collect()
}

fn check_cursor(previous: &Cursor, next: &Cursor) -> TestResult {
    if next.volume != previous.volume
        || next.epoch != previous.epoch
        || next.schema != previous.schema
        || next.query != previous.query
        || next.position != previous.position + 1
        || next.commit == previous.commit
        || next.accepted < next.position
        || next.accepted < previous.accepted
        || next.durable > next.accepted
        || next.durable < previous.durable
    {
        return Err(format!("discontinuous query cursor: {previous:?} -> {next:?}").into());
    }
    Ok(())
}

// All required records have already been accepted by the in-memory provider.
// A pending follow is missing delivery, rather than work to wait for. Check one
// record independently of the enclosing Tokio task's cooperative yield budget.
pub(super) fn next_published(changes: &mut ChangeStream) -> TestResult<Change> {
    tokio::task::unconstrained(changes.next())
        .now_or_never()
        .ok_or("published follow is pending")?
        .ok_or("published follow ended")?
        .map_err(Into::into)
}

pub(super) async fn run(case: TaskQueryCase) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("task-query-scenario")?;
    let volume = TaskVolume::open(Arc::clone(&provider), path.clone()).await?;
    let query = Query::folder(&case.folder)?;
    let mut observation = volume.observe(query.clone(), None, 100).await?;
    if observation.reset {
        return Err("initial observe unexpectedly reset".into());
    }
    let initial = observation
        .snapshot
        .take()
        .ok_or("missing initial snapshot")?;
    if !initial.files.is_empty() {
        return Err("fresh volume contains query files".into());
    }
    let mut generation = initial.generation;
    let mut cursor = initial.cursor.clone();
    let mut cuts = vec![initial];
    let mut changes = Vec::<Change>::new();
    let mut errors = Vec::new();
    for input in &case.writes {
        match input.write() {
            Ok(write) => match volume.append(generation, &write).await {
                Ok(next) if next != generation => {
                    generation = next;
                    let change = next_published(&mut observation.changes)?;
                    check_cursor(&cursor, &change.cursor)?;
                    cursor = change.cursor.clone();
                    changes.push(change);
                    cuts.push(volume.snapshot(&query, 100).await?);
                }
                Ok(_) => {}
                Err(error) => errors.push(failure(error)?),
            },
            Err(error) => errors.push(failure(error)?),
        }
    }
    // Observe the actual accepted cut, regardless of the fixture's expected length.
    if cursor.position != generation.position || observation.changes.next().now_or_never().is_some()
    {
        return Err("live follow did not end at the accepted cut".into());
    }
    if errors != case.expect.errors {
        return Err(format!("errors: expected {:?}, got {errors:?}", case.expect.errors).into());
    }
    let actual = changes
        .iter()
        .map(|change| {
            change
                .file
                .as_ref()
                .map(|file| {
                    Ok(super::task_query_case::File {
                        path: file.key.path(),
                        text: std::str::from_utf8(&file.bytes)?.to_owned(),
                        channel: file.channel,
                    })
                })
                .transpose()
        })
        .collect::<TestResult<Vec<_>>>()?;
    if actual != case.expect.changes {
        return Err(format!(
            "changes: expected {:?}, got {actual:?}",
            case.expect.changes
        )
        .into());
    }
    let snapshot = volume.snapshot(&query, 100).await?;
    if files(&snapshot.files)? != case.expect.files {
        return Err("final query files differ".into());
    }
    if let Some(order) = case.expect.order {
        if snapshot
            .files
            .iter()
            .map(|file| file.key.path())
            .collect::<Vec<_>>()
            != order
        {
            return Err("query identity order differs".into());
        }
    }
    let heads = volume.heads(&query, None, 100).await?;
    let actual_heads = heads
        .files
        .iter()
        .map(|file| {
            let bytes: Vec<_> = file
                .segments
                .iter()
                .flat_map(|segment| segment.iter().copied())
                .collect();
            if bytes.len() as u64 != file.length {
                return Err("head length differs".into());
            }
            Ok((file.key.path(), String::from_utf8(bytes)?))
        })
        .collect::<TestResult<BTreeMap<_, _>>>()?;
    if &actual_heads != case.expect.heads.as_ref().unwrap_or(&case.expect.files)
        || heads.cursor != snapshot.cursor
    {
        return Err("materialized heads differ".into());
    }
    // Every cut must survive later writes, reopen, and snapshot + follow transitions.
    let reopened = TaskVolume::open(provider, path).await?;
    let recovered = reopened.snapshot(&query, 100).await?;
    if recovered.files != snapshot.files || recovered.cursor != snapshot.cursor {
        return Err("reopened query differs".into());
    }
    for cut in cuts {
        let pinned = reopened
            .snapshot_at(&query, cut.cursor.clone(), 100)
            .await?;
        if pinned.files != cut.files || pinned.generation != cut.generation {
            return Err("pinned query cut changed".into());
        }
        let expected_heads = cut.into_heads()?;
        let pinned_heads = reopened
            .heads(&query, Some(pinned.cursor.clone()), 100)
            .await?;
        if pinned_heads.files.len() != expected_heads.files.len()
            || pinned_heads
                .files
                .iter()
                .zip(&expected_heads.files)
                .any(|(actual, expected)| {
                    actual.key != expected.key
                        || actual.segments != expected.segments
                        || actual.length != expected.length
                        || actual.through_sequence != expected.through_sequence
                })
        {
            return Err("pinned materialized heads changed".into());
        }
        let mut folded: BTreeMap<FileKey, FileValue> = pinned
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let mut resumed = reopened
            .observe(query.clone(), Some(pinned.cursor.clone()), 100)
            .await?;
        if resumed.reset || resumed.snapshot.is_some() {
            return Err("valid cut unexpectedly reset".into());
        }
        let mut cursor = pinned.cursor;
        while cursor.position < generation.position {
            let change = next_published(&mut resumed.changes)?;
            check_cursor(&cursor, &change.cursor)?;
            let expected = &changes[(cursor.position - 1) as usize];
            if change.file != expected.file
                || change.cursor.position != expected.cursor.position
                || change.cursor.commit != expected.cursor.commit
            {
                return Err("resumed change differs from live delivery".into());
            }
            cursor = change.cursor;
            if let Some(file) = change.file {
                folded.insert(file.key.clone(), file);
            }
        }
        if folded.into_values().collect::<Vec<_>>() != snapshot.files
            || resumed.changes.next().now_or_never().is_some()
        {
            return Err("snapshot + follow differs from uninterrupted observation".into());
        }
    }
    Ok(())
}

fn failure(error: Error) -> TestResult<super::task_query_case::Failure> {
    use super::task_query_case::Failure;
    Ok(match error {
        Error::ImmutableConflict(_) => Failure::Immutable,
        Error::SequenceConflict(_) => Failure::Sequence,
        Error::InvalidPath => Failure::InvalidPath,
        Error::InvalidSegment => Failure::InvalidSegment,
        error => return Err(error.into()),
    })
}
